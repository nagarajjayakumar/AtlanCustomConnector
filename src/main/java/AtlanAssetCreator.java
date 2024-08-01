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
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AtlanAssetCreator {

    private static final Logger logger = LoggerFactory.getLogger(AtlanAssetCreator.class);

    public static final AtlanConnectorType CONNECTOR_TYPE = AtlanConnectorType.S3;
    public static final String CONNECTION_NAME = "aws-s3-connection-njay-v1";
    public static final String OWNER = "nagajay_";
    static {
        Atlan.setBaseUrl(System.getenv("ATLAN_BASE_URL"));
        Atlan.setApiToken(System.getenv("ATLAN_API_KEY"));
    }

    public static final String xmlFileName = "s3-buckets.xml";

    public static void main(String[] args) {
        logger.info("Starting Atlan Asset Creation...");

        try {
            // Create S3 connection
            Connection connection = getOrCreateS3Connection(CONNECTION_NAME, CONNECTOR_TYPE);
            String connectionQualifiedName = connection.getQualifiedName();
            logger.info("Connection       ::" + connection.getGuid());

            // Read XML content from resource folder
            String xmlContent = readXmlFromResource(xmlFileName);

            // Parse the XML
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document doc = builder.parse(new ByteArrayInputStream(xmlContent.getBytes(StandardCharsets.UTF_8)));

            // Normalize the XML structure
            doc.getDocumentElement().normalize();

            // Get bucket name
            String bucketName = doc.getElementsByTagName("Name").item(0).getTextContent();

            S3Bucket bucket = getOrCreateS3Bucket(bucketName, connectionQualifiedName);

            logger.info("Connection       ::" + connection.getGuid());
            logger.info("Connection QName ::" + connectionQualifiedName);
            logger.info("Bucket :: " + bucket.getGuid());
            logger.info("Bucket Qualified Name :: " + bucket.getQualifiedName());
            logger.info("Bucket Qualified Name :: " + bucket.getName());

            // Create contents
            createContents(doc, bucket);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * Create the assest - individual s3 objects
     * @param doc
     * @param bucket
     * @throws AtlanException
     */
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
                logger.info("Using existing S3 object: " + existingObject.getQualifiedName());
            } catch (NotFoundException e) {
                // If not found, create new S3 object
                S3Object newObject = createS3Object(key, bucket, OBJECT_ARN);
                logger.info("Created new S3 object: " + newObject.getQualifiedName());
            }
        }
    }

    /**
     * Search for existing s3 object
     * @param key
     * @param bucketQualifiedName
     * @return
     * @throws AtlanException
     * @throws NotFoundException
     */
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

    /**
     * Create s3 object
     * @param key
     * @param bucket
     * @param objectArn
     * @return
     * @throws AtlanException
     */
    private static S3Object createS3Object(String key, S3Bucket bucket, String objectArn) throws AtlanException {
        S3Object object = S3Object.creator(key, bucket, objectArn)
                .description("S3 object " + key)
                .ownerUser(OWNER)
                .build();
        AssetMutationResponse objectResponse = object.save();
        return objectResponse.getResult(object);
    }

    /**
     * this method used to get s3 bucket or create s3 buket
     * @param bucketName
     * @param connectionQualifiedName
     * @return
     * @throws AtlanException
     * @throws InterruptedException
     */
    private static S3Bucket getOrCreateS3Bucket(String bucketName, String connectionQualifiedName) throws AtlanException, InterruptedException {
        try {
            List<S3Bucket> existingBuckets = findS3BucketByName(bucketName, connectionQualifiedName);
            logger.info("Using existing bucket: " + bucketName);
            return existingBuckets.get(0);
        } catch (NotFoundException e) {
            logger.info("Creating new bucket: " + bucketName);
            return createS3Bucket(bucketName, connectionQualifiedName);
        }
    }


    /**
     * Find the s3 bucket by name
     * @param bucketName
     * @param connectionQualifiedName
     * @return
     * @throws AtlanException
     * @throws InterruptedException
     */
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

    /**
     * Create S3 bucket
     * @param bucketName
     * @param connectionQualifiedName
     * @return
     * @throws AtlanException
     */
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


    /**
     * Get or create S3 connection based on the connection name
     * @param connectionName
     * @param connectorType
     * @return
     * @throws AtlanException
     * @throws InterruptedException
     */
    private static Connection getOrCreateS3Connection( String connectionName, AtlanConnectorType connectorType) throws AtlanException, InterruptedException {

        try {
            List<Connection> existingConnections = Connection.findByName(connectionName, connectorType);
            logger.info("Using existing connection: " + connectionName);
            return existingConnections.get(0);
        } catch (NotFoundException e) {
            logger.info("Creating new connection: " + connectionName);
            return createS3Connection(connectionName, connectorType);
        }

    }

    /**
     * Create s3 connection object
     * @param connectionName
     * @param connectorType
     * @return
     * @throws AtlanException
     * @throws InterruptedException
     */
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

    /**
     * Util method to retry the search until expected counted is reached
     * @param request
     * @param expectedCount
     * @return
     * @throws AtlanException
     * @throws InterruptedException
     */
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

    /**
     * Reads XML content from a resource file.
     * @param fileName Name of the XML file in the resources folder
     * @return String containing the XML content
     * @throws IOException if there's an error reading the file
     */
    private static String readXmlFromResource(String fileName) throws IOException {
        try (InputStream inputStream = AtlanAssetCreator.class.getClassLoader().getResourceAsStream(fileName)) {
            if (inputStream == null) {
                throw new IOException("Resource not found: " + fileName);
            }
            return new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

}
