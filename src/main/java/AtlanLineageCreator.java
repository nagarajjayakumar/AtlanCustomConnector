import com.atlan.Atlan;
import com.atlan.exception.AtlanException;
import com.atlan.exception.ErrorCode;
import com.atlan.exception.NotFoundException;
import com.atlan.model.assets.*;
import com.atlan.model.core.AssetMutationResponse;
import com.atlan.model.enums.AtlanLineageDirection;
import com.atlan.model.lineage.*;
import com.atlan.model.search.*;

import java.util.List;
import java.util.stream.Collectors;

public class AtlanLineageCreator {

    static {
        Atlan.setBaseUrl(System.getenv("ATLAN_BASE_URL"));
        Atlan.setApiToken(System.getenv("ATLAN_API_KEY"));
    }

    public static void main(String[] args) {
        try {
            // Find Postgres asset
            Table postgresTable = (Table) findAsset("your_postgres_asset_name", Table.TYPE_NAME);

            // Find Snowflake asset
            Table snowflakeTable = (Table) findAsset("your_snowflake_asset_name", Table.TYPE_NAME);

            // Create lineage
            createLineageProcess(postgresTable, snowflakeTable);

            // Verify lineage
            // verifyLineage(postgresTable.getGuid(), snowflakeTable.getGuid());

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
                .pageSize(1)
                .toRequest();

        IndexSearchResponse response = request.search();

        if (response.getAssets().isEmpty()) {
            throw new NotFoundException(ErrorCode.NOT_FOUND_PASSTHROUGH,"Asset not found: " + assetName);
        }

        return response.getAssets().get(0);
    }

    private static void createLineageProcess(Table sourceTable, Table targetTable) throws AtlanException {
        String processName = "Lineage between " + sourceTable.getName() + " and " + targetTable.getName();
        String connectionQualifiedName = sourceTable.getConnectionQualifiedName();

        LineageProcess toCreate = LineageProcess.creator(
                        processName,
                        connectionQualifiedName,
                        null,
                        List.of(Table.refByGuid(sourceTable.getGuid())),
                        List.of(Table.refByGuid(targetTable.getGuid())),
                        null)
                .build();

        AssetMutationResponse response = toCreate.save();

        if (response.getCreatedAssets().isEmpty()) {
            throw new NotFoundException(ErrorCode.NOT_FOUND_PASSTHROUGH,"Failed to create lineage process");
        }

        System.out.println("Lineage process created successfully");
    }

}
