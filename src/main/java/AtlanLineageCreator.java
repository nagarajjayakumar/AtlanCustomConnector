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

import java.util.List;

public class AtlanLineageCreator {

    static {
        Atlan.setBaseUrl(System.getenv("ATLAN_BASE_URL"));
        Atlan.setApiToken(System.getenv("ATLAN_API_KEY"));
    }


    public static final  String s3ConnectionName = "aws-s3-connection-njay-v1";
    public static final  String postgresConnectionName = "postgres-naj";
    public static final  String snowflakeConnectionConnectionName = "snowflake-naj";

    private static final Logger logger = LoggerFactory.getLogger(AtlanLineageCreator.class);

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

//            if (postgresTable != null && s3Object != null && snowflakeTable != null) {
//                // Create lineage process: Postgres → S3
//                createLineageProcess(postgresTable, s3Object, "Postgres to S3");
//
//                // Create lineage process: S3 → Snowflake
//                createLineageProcess(s3Object, snowflakeTable, "S3 to Snowflake");
//
//                // Verify lineage
//                verifyLineage(postgresTable.getGuid(), s3Object.getGuid(), AtlanLineageDirection.DOWNSTREAM);
//                verifyLineage(s3Object.getGuid(), snowflakeTable.getGuid(), AtlanLineageDirection.DOWNSTREAM);
//            } else {
//                System.out.println("One or more assets not found.");
//            }

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

    private static void createLineageProcess(Asset sourceAsset, Asset targetAsset, String processName) throws AtlanException {
        String connectionQualifiedName = sourceAsset.getConnectionQualifiedName();

        LineageProcess process = LineageProcess.creator(
                        processName,
                        connectionQualifiedName,
                        "dag_" + processName.replaceAll("\\s+", "_").toLowerCase(),
                        List.of(
                                sourceAsset instanceof Table ? Table.refByGuid(sourceAsset.getGuid()) : S3Object.refByGuid(sourceAsset.getGuid())
                        ),
                        List.of(
                                targetAsset instanceof Table ? Table.refByGuid(targetAsset.getGuid()) : S3Object.refByGuid(targetAsset.getGuid())
                        ),
                        null)
                .sql("SELECT * FROM " + sourceAsset.getName() + ";")
                .sourceURL("https://your.orchestrator/unique/id/" + processName.replaceAll("\\s+", "_").toLowerCase())
                .build();

        AssetMutationResponse response = process.save();

        if (response.getCreatedAssets().isEmpty()) {
            throw new NotFoundException(ErrorCode.NOT_FOUND_PASSTHROUGH, "Failed to create lineage process: " + processName);
        }

        System.out.println("Lineage process created successfully: " + processName);
        System.out.println("Created assets: " + response.getCreatedAssets().size());
        System.out.println("Updated assets: " + response.getUpdatedAssets().size());
    }

    private static void verifyLineage(String sourceGuid, String targetGuid, AtlanLineageDirection direction) throws AtlanException {
        FluentLineage.builder(Atlan.getDefaultClient(), sourceGuid)
                .direction(direction)
                .stream()
                .filter(a -> !(a instanceof LineageProcess))
                .limit(100)
                .forEach(result -> {
                    if (result.getGuid().equals(targetGuid)) {
                        System.out.println("Lineage verified successfully from " + sourceGuid + " to " + targetGuid);
                    }
                });
    }



    private static void logAsset(Asset asset) {
        if (asset != null) {
            logger.info("Found asset: {}", asset.getQualifiedName());
        } else {
            logger.info("Asset not found.");
        }
    }

}
