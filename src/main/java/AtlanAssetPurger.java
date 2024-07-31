import com.atlan.Atlan;
import com.atlan.exception.AtlanException;
import com.atlan.model.assets.Asset;
import com.atlan.model.assets.Connection;
import com.atlan.model.assets.S3Bucket;
import com.atlan.model.core.AssetMutationResponse;
import com.atlan.model.enums.AtlanConnectorType;
import org.w3c.dom.Document;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

public class AtlanAssetPurger {

    public static final AtlanConnectorType CONNECTOR_TYPE = AtlanConnectorType.S3;
    public static final String CONNECTION_NAME = "aws-s3-connection-njay";

    static {
        Atlan.setBaseUrl(System.getenv("ATLAN_BASE_URL"));
        Atlan.setApiToken(System.getenv("ATLAN_API_KEY"));
    }

    public static void main(String[] args) {
        System.out.println("Starting Atlan Asset Deletion ...");

        try{
            AssetMutationResponse response =
                    Asset.delete("08e2bc65-61d4-407d-9678-000d58734b0a"); //


            Asset deleted = response.getDeletedAssets().get(0);
            System.out.println("Deleted --> " + deleted);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Connection createS3Connection(String connectionName,
                                                 AtlanConnectorType connectorType) throws AtlanException, InterruptedException {
        Connection connection = Connection.creator(connectionName, connectorType)
                .build();
        AssetMutationResponse response = connection.save();

        Asset created_asset = response.getCreatedAssets().get(0);
        Connection result_connection = (Connection) created_asset;
        return result_connection;
    }
}
