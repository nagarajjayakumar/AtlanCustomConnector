import com.atlan.Atlan;
import com.atlan.AtlanClient;
import com.atlan.exception.AtlanException;
import com.atlan.exception.ErrorCode;
import com.atlan.exception.NotFoundException;
import com.atlan.model.assets.*;
import com.atlan.model.core.AssetMutationResponse;
import com.atlan.model.enums.AtlanConnectorType;
import com.atlan.model.enums.AtlanLineageDirection;
import com.atlan.model.lineage.FluentLineage;
import com.atlan.model.search.IndexSearchRequest;
import com.atlan.model.search.IndexSearchResponse;

import java.util.List;
import java.util.stream.Collectors;

public class AtlanLineageCreator {

    static {
        Atlan.setBaseUrl(System.getenv("ATLAN_BASE_URL"));
        Atlan.setApiToken(System.getenv("ATLAN_API_KEY"));
    }

    public static void main(String[] args) {
        try {
            // Find the Postgres asset
            Table postgresTable = (Table) findAsset("your_postgres_asset_name", Table.TYPE_NAME);

            // Find the S3Object asset
            S3Object s3Object = (S3Object) findAsset("your_s3_object_name", S3Object.TYPE_NAME);

            // Find the Snowflake asset
            Table snowflakeTable = (Table) findAsset("your_snowflake_asset_name", Table.TYPE_NAME);

            // Create lineage process: Postgres → S3
            createLineageProcess(postgresTable, s3Object, "Postgres to S3");

            // Create lineage process: S3 → Snowflake
            createLineageProcess(s3Object, snowflakeTable, "S3 to Snowflake");

            // Verify lineage
            verifyLineage(postgresTable.getGuid(), s3Object.getGuid(), AtlanLineageDirection.DOWNSTREAM);
            verifyLineage(s3Object.getGuid(), snowflakeTable.getGuid(), AtlanLineageDirection.DOWNSTREAM);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Asset findAsset(String assetName, String assetType) throws AtlanException {
        IndexSearchRequest request = Atlan.getDefaultClient()
                .assets
                .select()
                .where(Asset.NAME.eq(assetName))
                .where(Asset.TYPE_NAME.eq(assetType))
                .where(Asset.CONNECTION_NAME.eq("connectionQualifiedName"))
                .pageSize(1)
                .toRequest();

        IndexSearchResponse response = request.search();

        if (response.getAssets().isEmpty()) {
            throw new NotFoundException(ErrorCode.NOT_FOUND_PASSTHROUGH,"Asset not found: " + assetName);
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
                //.sql("SELECT * FROM " + sourceAsset.getName() + ";")
                //.sourceURL("https://your.orchestrator/unique/id/" + processName.replaceAll("\\s+", "_").toLowerCase())
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
}
