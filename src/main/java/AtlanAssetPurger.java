import com.atlan.Atlan;
import com.atlan.model.assets.Asset;
import com.atlan.model.core.AssetMutationResponse;
import com.atlan.model.enums.AtlanConnectorType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;


public class AtlanAssetPurger {

    private static final Logger logger = LoggerFactory.getLogger(AtlanAssetPurger.class);

    public static final AtlanConnectorType CONNECTOR_TYPE = AtlanConnectorType.S3;
    public static final String CONNECTION_NAME = "aws-s3-connection-njay";

    static {
        Atlan.setBaseUrl(System.getenv("ATLAN_BASE_URL"));
        Atlan.setApiToken(System.getenv("ATLAN_API_KEY"));
    }

    /**
     * Method to purge Atlan assets
     * @param args
     */
    public static void main(String[] args) {
        logger.info("Starting Atlan Asset Deletion ...");

        try{
            AssetMutationResponse response =
                    Asset.delete("64555684-78f7-43b1-af46-d9a270873d98"); //

            List<Asset> deleteAsset = response.getDeletedAssets();
            logger.warn("asset empty "+ deleteAsset.isEmpty());
            if(null != response.getDeletedAssets() && ! response.getDeletedAssets().isEmpty()) {
                Asset deleted = response.getDeletedAssets().get(0);
                logger.info("Deleted --> " + deleted);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
