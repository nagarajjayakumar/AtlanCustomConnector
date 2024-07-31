import co.elastic.clients.elasticsearch._types.SortOrder;
import com.atlan.Atlan;
import com.atlan.AtlanClient;
import com.atlan.exception.AtlanException;
import com.atlan.exception.ErrorCode;
import com.atlan.exception.InvalidRequestException;
import com.atlan.exception.NotFoundException;
import com.atlan.model.assets.*;
import com.atlan.model.core.AssetMutationResponse;
import com.atlan.model.enums.AtlanConnectorType;
import com.atlan.model.search.CompoundQuery;
import com.atlan.model.search.IndexSearchRequest;
import com.atlan.model.search.IndexSearchResponse;
import com.atlan.net.HttpClient;
import org.w3c.dom.*;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

public class AtlanAssetCreator {

    public static final AtlanConnectorType CONNECTOR_TYPE = AtlanConnectorType.S3;
    public static final String CONNECTION_NAME = "aws-s3-connection-njay-v1";
    public static final String OWNER = "nagajay_";
    static {
        Atlan.setBaseUrl(System.getenv("ATLAN_BASE_URL"));
        Atlan.setApiToken(System.getenv("ATLAN_API_KEY"));
    }

    public static void main(String[] args) {
        System.out.println("Starting Atlan Asset Creation...");

        String xmlFilePath = "/Users/njayakumar/Desktop/ak/naga/workspace/AtlanCustomConnector/src/main/java/s3-buckets.xml";
        try {

            // Create S3 connection
            Connection connection = getOrCreateS3Connection(CONNECTION_NAME, CONNECTOR_TYPE);
            String connectionQualifiedName = connection.getQualifiedName();
            System.out.println("Connection       ::" + connection.getGuid());
            // Read XML content from file
            String xmlContent = new String(Files.readAllBytes(Paths.get(xmlFilePath)), StandardCharsets.UTF_8);

            // Parse the XML
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document doc = builder.parse(new ByteArrayInputStream(xmlContent.getBytes(StandardCharsets.UTF_8)));

            // Normalize the XML structure
            doc.getDocumentElement().normalize();

            // Get bucket name
            String bucketName = doc.getElementsByTagName("Name").item(0).getTextContent();

            S3Bucket bucket = getOrCreateS3Bucket(bucketName, connectionQualifiedName);

            System.out.println("Connection       ::" + connection.getGuid());
            System.out.println("Connection QName ::" + connectionQualifiedName);
            System.out.println("Bucket :: " + bucket.getGuid());
            System.out.println("Bucket Qualified Name :: " + bucket.getQualifiedName());
            System.out.println("Bucket Qualified Name :: " + bucket.getName());

            // Create contents
            createContents(doc, bucket);




        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private static void createContents(Document doc, S3Bucket bucket) throws AtlanException {
        NodeList contentsList = doc.getElementsByTagName("Contents");
        for (int i = 0; i < contentsList.getLength(); i++) {
            Element content = (Element) contentsList.item(i);
            String key = content.getElementsByTagName("Key").item(0).getTextContent();
            String bucketName = bucket.getName();

            final String OBJECT_ARN = "arn:aws:s3:::" + bucketName +"-njay-v1" + "/prefix/" + key;

            try {
                // Try to find existing S3 object
                S3Object existingObject = getS3Object(key, bucket.getQualifiedName());
                System.out.println("Using existing S3 object: " + existingObject.getQualifiedName());
            } catch (NotFoundException e) {
                // If not found, create new S3 object
                S3Object newObject = createS3Object(key, bucket, OBJECT_ARN);
                System.out.println("Created new S3 object: " + newObject.getQualifiedName());
            }
        }
    }

    private static S3Object getS3Object(String key, String bucketQualifiedName) throws AtlanException, NotFoundException {
        AtlanClient client = Atlan.getDefaultClient();
        IndexSearchRequest index = client.assets
                .select()
                .where(CompoundQuery.assetType(S3Object.TYPE_NAME))
                .where(S3Object.NAME.eq(key))
                .where(S3Object.S3BUCKET_QUALIFIED_NAME.eq(bucketQualifiedName))
                .pageSize(1)
                .toRequest();

        IndexSearchResponse response = index.search();

        if (response == null || response.getAssets() == null) {
            throw new NotFoundException(ErrorCode.NOT_FOUND_PASSTHROUGH, "No S3 object found with key: " + key + " in bucket: " + bucketQualifiedName);
        }

        List<S3Object> objects = response.getAssets().stream()
                .filter(asset -> asset instanceof S3Object)
                .map(asset -> (S3Object) asset)
                .collect(Collectors.toList());

        if (objects.isEmpty()) {
            throw new NotFoundException(ErrorCode.NOT_FOUND_PASSTHROUGH, "No S3 object found with key: " + key + " in bucket: " + bucketQualifiedName);
        }

        return objects.get(0);
    }

    private static S3Object createS3Object(String key, S3Bucket bucket, String objectArn) throws AtlanException {
        S3Object object = S3Object.creator(key, bucket, objectArn)
                .description("S3 object " + key)
                .ownerUser(OWNER)
                .build();
        AssetMutationResponse objectResponse = object.save();
        return objectResponse.getResult(object);
    }

    private static S3Bucket getOrCreateS3Bucket(String bucketName, String connectionQualifiedName) throws AtlanException, InterruptedException {
        try {
            List<S3Bucket> existingBuckets = findS3BucketByName(bucketName, connectionQualifiedName);
            System.out.println("Using existing bucket: " + bucketName);
            return existingBuckets.get(0);
        } catch (NotFoundException e) {
            System.out.println("Creating new bucket: " + bucketName);
            return createS3Bucket(bucketName, connectionQualifiedName);
        }
    }


    private static List<S3Bucket> findS3BucketByName(String bucketName, String connectionQualifiedName) throws AtlanException,  InterruptedException {
        AtlanClient client = Atlan.getDefaultClient();

        IndexSearchRequest index = client.assets
                .select()
                .where(CompoundQuery.superType(IS3.TYPE_NAME))
                .where(Asset.QUALIFIED_NAME.startsWith(connectionQualifiedName))
                .where(Asset.NAME.eq(bucketName))
                .pageSize(1)
                .sort(Asset.CREATE_TIME.order(SortOrder.Asc))
                .includeOnResults(Asset.NAME)
                .includeOnResults(Asset.CONNECTION_QUALIFIED_NAME)
                .toRequest();

        IndexSearchResponse response = retrySearchUntil(index, 0L);

        if (response.getAssets() == null) {
            throw new NotFoundException(ErrorCode.NOT_FOUND_PASSTHROUGH,"No buckets found with name: " + bucketName + " and connectionQualifiedName: " + connectionQualifiedName);
        }
        List<S3Bucket> buckets = response.getAssets().stream()
                .filter(entity -> entity instanceof S3Bucket)
                .map(entity -> (S3Bucket) entity)
                .collect(Collectors.toList());

        if (buckets.isEmpty()) {
            throw new NotFoundException(ErrorCode.NOT_FOUND_PASSTHROUGH,"No buckets found with name: " + bucketName + " and connectionQualifiedName: " + connectionQualifiedName);
        }

        return buckets;
    }

    private static S3Bucket createS3Bucket(String bucketName, String connectionQualifiedName) throws AtlanException {

        final String BUCKET_ARN = "arn:aws:s3:::" + bucketName+"-njay-v1";
        S3Bucket bucket = S3Bucket.creator(bucketName, connectionQualifiedName, BUCKET_ARN)
                .description("S3 bucket for " + bucketName+"-njay-v1")
                .ownerUser(OWNER)
                .build();
        AssetMutationResponse response = bucket.save();

        if (response == null || response.getCreatedAssets().isEmpty()) {
            throw new RuntimeException("Failed to create bucket");
        }

        return response.getResult(bucket);
    }


    private static Connection getOrCreateS3Connection( String connectionName, AtlanConnectorType connectorType) throws AtlanException, InterruptedException {

        try {
            List<Connection> existingConnections = Connection.findByName(connectionName, connectorType);
            System.out.println("Using existing connection: " + connectionName);
            return existingConnections.get(0);
        } catch (NotFoundException e) {
            System.out.println("Creating new connection: " + connectionName);
            return createS3Connection(connectionName, connectorType);
        }

    }

    private static Connection createS3Connection(String connectionName,
                                                 AtlanConnectorType connectorType) throws AtlanException, InterruptedException {
        Connection connection = Connection.creator(connectionName, connectorType)
                .build();
        AssetMutationResponse response = connection.save();

        int retryCount = 0;
        while (response == null && retryCount < Atlan.getMaxNetworkRetries()) {
            retryCount++;
            try {
                response = connection.save().block();
            } catch (InvalidRequestException e) {
                if (retryCount < Atlan.getMaxNetworkRetries()) {
                    if (e.getCode() != null
                            && e.getCode().equals("ATLAN-JAVA-400-000")
                            && e.getMessage().equals("Server responded with ATLAS-400-00-029: Auth request failed")) {
                        Thread.sleep(HttpClient.waitTime(retryCount).toMillis());
                    }
                } else {
                    System.err.println("Overran retry limit (" + Atlan.getMaxNetworkRetries() + "), rethrowing exception.");
                    throw e;
                }
            }
        }

        Asset created_asset = response.getCreatedAssets().get(0);
        Connection result_connection = (Connection) created_asset;
        return result_connection;
    }

    private static IndexSearchResponse retrySearchUntil(IndexSearchRequest request, long expectedCount) throws AtlanException, InterruptedException {
        int retryCount = 0;
        IndexSearchResponse response = null;
        while (retryCount < Atlan.getMaxNetworkRetries()) {
            response = request.search();
            if (response.getApproximateCount() >= expectedCount) {
                return response;
            }
            Thread.sleep(HttpClient.waitTime(++retryCount).toMillis());
        }
        throw new RuntimeException("Failed to get expected search results after " + retryCount + " retries");
    }

}
