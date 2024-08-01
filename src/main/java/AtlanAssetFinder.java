import com.atlan.Atlan;
import com.atlan.exception.AtlanException;
import com.atlan.model.assets.Asset;
import com.atlan.model.assets.Connection;
import com.atlan.model.search.IndexSearchRequest;
import com.atlan.model.search.IndexSearchResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class AtlanAssetFinder {

    private static final Logger logger = LoggerFactory.getLogger(AtlanAssetFinder.class);

    static {
        Atlan.setBaseUrl(System.getenv("ATLAN_BASE_URL"));
        Atlan.setApiToken(System.getenv("ATLAN_API_KEY"));
    }

    public static void main(String[] args) {
        try {
            // Example connection name and asset name
            String connectionName = "aws-s3-connection-njay-v1";
            String assetName = "SUPPLIERS.csv";

            // Find connection by name
            Connection connection = findConnectionByName(connectionName);

            if (connection != null) {
                logger.debug("Found connection: {}", connection.getQualifiedName());

                // Find asset within the connection by name
                Asset asset = findAssetInConnectionByName(connection.getQualifiedName(), assetName);

                if (asset != null) {
                    logger.debug("Found asset: {}", asset.getQualifiedName());
                } else {
                    logger.debug("Asset not found.");
                }

                // Find all assets within the connection
                List<Asset> assets = findAssetsInConnection(connection.getQualifiedName());
                logger.debug("Found {} assets in the connection.", assets.size());
                assets.forEach(a -> logger.debug("Asset: {}", a.getName()));
            } else {
                logger.debug("Connection not found.");
            }

        } catch (Exception e) {
            logger.error("An error occurred:", e);
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
     * Finds all assets within a specific connection.
     *
     * @param connectionQualifiedName The qualified name of the connection to search within.
     * @return A list of Asset objects found in the connection.
     * @throws AtlanException If there's an error communicating with Atlan.
     */
    private static List<Asset> findAssetsInConnection(String connectionQualifiedName) throws AtlanException {
        IndexSearchRequest request = Atlan.getDefaultClient()
                .assets
                .select()
                .where(Asset.CONNECTION_QUALIFIED_NAME.eq(connectionQualifiedName))
                .pageSize(100) // Adjust the page size as needed
                .toRequest();

        IndexSearchResponse response = request.search();

        return response.getAssets();
    }
}
