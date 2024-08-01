import com.atlan.Atlan;
import com.atlan.exception.AtlanException;
import com.atlan.exception.ErrorCode;
import com.atlan.exception.NotFoundException;
import com.atlan.model.assets.*;
import com.atlan.model.core.AssetMutationResponse;
import com.atlan.model.enums.AtlanLineageDirection;
import com.atlan.model.lineage.FluentLineage;
import com.atlan.model.search.IndexSearchRequest;
import com.atlan.model.search.IndexSearchResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class AtlanLineageCreator {

    static {
        Atlan.setBaseUrl(System.getenv("ATLAN_BASE_URL"));
        Atlan.setApiToken(System.getenv("ATLAN_API_KEY"));
    }

    public static final String s3ConnectionName = "aws-s3-connection-njay-v1";
    public static final String postgresConnectionName = "postgres-naj";
    public static final String snowflakeConnectionConnectionName = "snowflake-naj";
    public static final String OWNER = "nagajay_";

    private static final Logger logger = LoggerFactory.getLogger(AtlanLineageCreator.class);

    /**
     * Main method to execute the lineage creation process.
     *
     * @param args Command line arguments (not used)
     */
    public static void main(String[] args) {
        try {
            // Find the Postgres table
            Connection postgresConnection = findConnectionByName(postgresConnectionName);
            Table postgresTable = (Table) findAssetInConnectionByName(postgresConnection.getQualifiedName(), "EMPLOYEES");

            // Find the S3 object
            Connection s3Connection = findConnectionByName(s3ConnectionName);
            S3Object s3Object = (S3Object) findAssetInConnectionByName(s3Connection.getQualifiedName(), "EMPLOYEES.csv");

            // Find the Snowflake table
            Connection snowflakeConnection = findConnectionByName(snowflakeConnectionConnectionName);
            Table snowflakeTable = (Table) findAssetInConnectionByName(snowflakeConnection.getQualifiedName(), "EMPLOYEES");

            logAsset(postgresTable);
            logAsset(s3Object);
            logAsset(snowflakeTable);

            if (postgresTable != null && s3Object != null && snowflakeTable != null) {
                // Create lineage process: Postgres → S3
                Map<String, Object> postgresTo3SParams = new HashMap<>();
                postgresTo3SParams.put("sourceConnection", postgresConnection);
                postgresTo3SParams.put("sourceAsset", postgresTable);
                postgresTo3SParams.put("targetAsset", s3Object);
                postgresTo3SParams.put("processName", "Postgres to S3");
                createLineageIfNotExists(postgresTo3SParams);
                //createLineageProcess(postgresTo3SParams);

                // Create lineage process: S3 → Snowflake
                Map<String, Object> s3ToSnowflakeParams = new HashMap<>();
                s3ToSnowflakeParams.put("sourceConnection", s3Connection);
                s3ToSnowflakeParams.put("sourceAsset", s3Object);
                s3ToSnowflakeParams.put("targetAsset", snowflakeTable);
                s3ToSnowflakeParams.put("processName", "S3 to Snowflake");
                createLineageIfNotExists(s3ToSnowflakeParams);
                //createLineageProcess(s3ToSnowflakeParams);

                // Verify lineage
                verifyLineage(postgresTable.getGuid(), s3Object.getGuid(), AtlanLineageDirection.DOWNSTREAM);
                verifyLineage(s3Object.getGuid(), snowflakeTable.getGuid(), AtlanLineageDirection.DOWNSTREAM);
            } else {
                logger.info("One or more assets not found.");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Finds a connection by its name.
     *
     * @param connectionName The name of the connection to find.
     * @return The Connection object if found, null otherwise.
     * @throws AtlanException If there's an error communicating with Atlan.
     */
    private static Connection findConnectionByName(String connectionName) throws AtlanException {
        IndexSearchRequest request = Atlan.getDefaultClient()
                .assets
                .select()
                .where(Asset.TYPE_NAME.eq(Connection.TYPE_NAME))
                .where(Asset.NAME.eq(connectionName))
                .pageSize(1)
                .toRequest();

        IndexSearchResponse response = request.search();

        if (response.getAssets().isEmpty()) {
            return null;
        }

        return (Connection) response.getAssets().get(0);
    }

    /**
     * Finds an asset within a specific connection by its name.
     *
     * @param connectionQualifiedName The qualified name of the connection to search within.
     * @param assetName The name of the asset to find.
     * @return The Asset object if found, null otherwise.
     * @throws AtlanException If there's an error communicating with Atlan.
     */
    private static Asset findAssetInConnectionByName(String connectionQualifiedName, String assetName) throws AtlanException {
        IndexSearchRequest request = Atlan.getDefaultClient()
                .assets
                .select()
                .where(Asset.CONNECTION_QUALIFIED_NAME.eq(connectionQualifiedName))
                .where(Asset.NAME.eq(assetName))
                .pageSize(1)
                .toRequest();

        IndexSearchResponse response = request.search();

        if (response.getAssets().isEmpty()) {
            return null;
        }

        return response.getAssets().get(0);
    }

    /**
     * Create lineage only if not exists
     * @param params
     * @throws AtlanException
     */
    private static void createLineageIfNotExists(Map<String, Object> params) throws AtlanException {
        Asset sourceAsset = (Asset) params.get("sourceAsset");
        Asset targetAsset = (Asset) params.get("targetAsset");
        String processName = (String) params.get("processName");

        if (!lineageExists(sourceAsset.getGuid(), targetAsset.getGuid(), AtlanLineageDirection.DOWNSTREAM)) {
            logger.debug("lineage not exists.. creating lineage.. "+ processName );
            createLineageProcess(params);
        } else {
            logger.info("Lineage already exists from {} to {}. Skipping creation.", sourceAsset.getQualifiedName(), targetAsset.getQualifiedName());
        }
    }

    /**
     * Check if the lineage exists
     * @param sourceGuid
     * @param targetGuid
     * @param direction
     * @return
     * @throws AtlanException
     */
    private static boolean lineageExists(String sourceGuid, String targetGuid, AtlanLineageDirection direction) throws AtlanException {
        AtomicBoolean exists = new AtomicBoolean(false);

        // First, fetch the source asset to get its qualified name
        Asset sourceAsset = Asset.get(Atlan.getDefaultClient(),sourceGuid, false);
        String sourceQualifiedName = sourceAsset != null ? sourceAsset.getQualifiedName() : "Unknown";

        FluentLineage.builder(Atlan.getDefaultClient(), sourceGuid)
                .direction(direction)
                .stream()
                .filter(a -> !(a instanceof LineageProcess))
                .limit(100)
                .forEach(result -> {
                    if (result.getGuid().equals(targetGuid)) {
                        exists.set(true);
                        logger.info("Existing lineage found from {} to {}", sourceGuid, targetGuid);
                        logger.info("Source Asset - Qualified Name: {}, GUID: {}", sourceQualifiedName, sourceGuid);
                        logger.info("Target Asset - Qualified Name: {}, GUID: {}", result.getQualifiedName(), result.getGuid());
                    }
                });

        return exists.get();
    }

    /**
     * method is used to create the lineage process
     * @param params
     * @throws AtlanException
     */
    private static void createLineageProcess(Map<String, Object> params) throws AtlanException {
        Connection sourceConnection = (Connection) params.get("sourceConnection");
        Asset sourceAsset = (Asset) params.get("sourceAsset");
        Asset targetAsset = (Asset) params.get("targetAsset");
        String processName = (String) params.get("processName");

        String connectionQualifiedName = sourceAsset.getConnectionQualifiedName();
        if(null == connectionQualifiedName) {
            connectionQualifiedName = sourceConnection.getQualifiedName();
            logger.debug("sourceAsset qualified name " + sourceAsset.getQualifiedName());
            logger.debug("connection qualified name is not present in Asset .. trying with the source connection "+ connectionQualifiedName);
        }

        LineageProcess process = LineageProcess.creator(
                        processName,
                        connectionQualifiedName,
                        "nj_v1_dag_" + processName.replaceAll("\\s+", "_").toLowerCase(),
                        List.of(
                                sourceAsset instanceof Table ? Table.refByGuid(sourceAsset.getGuid()) : S3Object.refByGuid(sourceAsset.getGuid())
                        ),
                        List.of(
                                targetAsset instanceof Table ? Table.refByGuid(targetAsset.getGuid()) : S3Object.refByGuid(targetAsset.getGuid())
                        ),
                        null)
                .ownerUser(OWNER)
                .build();

        AssetMutationResponse response = process.save();

        if (response.getCreatedAssets().isEmpty()) {
            throw new NotFoundException(ErrorCode.NOT_FOUND_PASSTHROUGH, "Failed to create lineage process: " + processName);
        }

        logger.info("Lineage process created successfully: " + processName);
        logger.info("Created assets: " + response.getCreatedAssets().size());
        logger.info("Updated assets: " + response.getUpdatedAssets().size());
    }


    /**
     * Verifies the lineage between two assets.
     *
     * @param sourceGuid The GUID of the source asset.
     * @param targetGuid The GUID of the target asset.
     * @param direction The direction of the lineage.
     * @throws AtlanException If there's an error verifying the lineage.
     */
    private static void verifyLineage(String sourceGuid, String targetGuid, AtlanLineageDirection direction) throws AtlanException {
        // First, fetch the source asset to get its qualified name
        Asset sourceAsset = Asset.get(Atlan.getDefaultClient(),sourceGuid, false);
        String sourceQualifiedName = sourceAsset != null ? sourceAsset.getQualifiedName() : "Unknown";

        FluentLineage.builder(Atlan.getDefaultClient(), sourceGuid)
                .direction(direction)
                .stream()
                .filter(a -> !(a instanceof LineageProcess))
                .limit(100)
                .forEach(result -> {
                    if (result.getGuid().equals(targetGuid)) {
                        logger.info("Lineage verified successfully");
                        logger.info("Source Asset - Qualified Name: {}, GUID: {}", sourceQualifiedName, sourceGuid);
                        logger.info("Target Asset - Qualified Name: {}, GUID: {}", result.getQualifiedName(), result.getGuid());
                    }
                });
    }


    /**
     * Logs information about an asset.
     *
     * @param asset The asset to log information about.
     */
    private static void logAsset(Asset asset) {
        if (asset != null) {
            logger.info("Found asset: {}", asset.getQualifiedName());
        } else {
            logger.info("Asset not found.");
        }
    }
}
