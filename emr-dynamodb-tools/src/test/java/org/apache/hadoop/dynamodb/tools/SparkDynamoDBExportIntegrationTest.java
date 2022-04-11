package org.apache.hadoop.dynamodb.tools;

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.local.main.ServerRunner;
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.dynamodb.DynamoDBConstants;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class SparkDynamoDBExportIntegrationTest {

  private static final String TABLE_NAME = "test_table";
  private static final String INDEX_NAME = "test_index";

  private static int dynamoDbServerPort;
  private static DynamoDBProxyServer dynamoDbServer;
  private static AmazonDynamoDB dynamoDbClient;

  @BeforeClass
  public static void beforeClass() throws Exception {
    dynamoDbClient = startDynamoDb();
    createTestTable(dynamoDbClient);
    addTestData(dynamoDbClient);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    if (dynamoDbServer != null) {
      dynamoDbServer.stop();
    }

    if (dynamoDbClient != null) {
      dynamoDbClient.shutdown();
    }
  }

  @Test
  public void simpleTest() throws Exception {
    // returns all rows in timeframe since sampling is currently range-based
    // and only guaranteed with > 10k uniform values.
    // Should have 4 records (12:00, 12:15, 12:30, 12:45) for 6 ids, or 24 total
    String[] args = new String[] {
        "--table_name", TABLE_NAME,
        "--index_name", INDEX_NAME,
        "--row_key", "index_key",
        "--sort_key", "last_modified",
        "--min_sort_key", "2022-03-02T12:00:00",
        "--max_sort_key", "2022-03-02T13:00:00",
        "--sample_percent", "0.01",
        "--local_mode",
    };

    doSuccessTest(getTestName(), args);
  }

  @Test
  public void sampleTest() throws Exception {
    // Should have 4 records (12:00, 12:15, 12:30, 12:45) for 1 ids, or 4 total
    String[] args = new String[] {
        "--table_name", TABLE_NAME,
        "--index_name", INDEX_NAME,
        "--row_key", "index_key",
        "--sort_key", "last_modified",
        "--min_sort_key", "2022-03-02T12:00:00",
        "--max_sort_key", "2022-03-02T13:00:00",
        "--sample_percent", "0.0001",
        "--local_mode",
    };

    doSuccessTest(getTestName(), args);
  }

  @Test
  public void attributeTest() throws Exception {
    // Should have 4 records (12:00, 12:15, 12:30, 12:45) for 1 ids, or 4 total
    String[] args = new String[] {
        "--table_name", TABLE_NAME,
        "--index_name", INDEX_NAME,
        "--row_key", "index_key",
        "--sort_key", "last_modified",
        "--min_sort_key", "2022-03-02T12:00:00",
        "--max_sort_key", "2022-03-02T13:00:00",
        "--sample_percent", "0.0001",
        "--attributes", "version,last_modified",
        "--local_mode",
    };

    doSuccessTest(getTestName(), args);
  }

  /**
   * Test data is as follows:
   * - 5 ids, from 10000 to 100005
   * - For each id, we make 6 versions from 0 to 6 with these last_modified times:
   *   - 0 2022-03-02T11:45:00Z (1646221500)
   *   - 1 2022-03-02T12:00:00Z (1646222400)
   *   - 2 2022-03-02T12:15:00Z (1646223300)
   *   - 3 2022-03-02T12:30:00Z (1646224200)
   *   - 4 2022-03-02T12:45:00Z (1646225100)
   *   - 5 2022-03-02T13:00:00Z (1646226000)
   *   - 6 2022-03-02T13:15:00Z (1646226900)
   */
  private static void addTestData(AmazonDynamoDB dynamoDbClient) {

    for (int id = 10000; id < 10006; id++) {
      int indexKey = id % 10000 + 1;
      int version = 0;
      for (long lastModified = toSeconds("2022-03-02T11:45:00Z");
          lastModified <= toSeconds("2022-03-02T13:15:00Z");
          lastModified += Duration.ofMinutes(15).getSeconds()) {
        Map<String, AttributeValue> attributes = new HashMap<>();
        attributes.put("id", new AttributeValue().withN(Integer.toString(id)));
        attributes.put("version", new AttributeValue().withN(Integer.toString(version++)));
        attributes.put("index_key", new AttributeValue().withN(Integer.toString(indexKey)));
        attributes.put("last_modified", new AttributeValue().withN(Long.toString(lastModified)));

        dynamoDbClient.putItem(new PutItemRequest().withTableName(TABLE_NAME).withItem(attributes));
      }
    }
  }

  private void doSuccessTest(String testName, String[] args) throws Exception {
    Path outputDir = getOutputDir(testName);
    List<String> argsList = new ArrayList<>(Arrays.asList(args));

    argsList.add("--output_path");
    argsList.add("file://" + outputDir);
    argsList.add("--dynamo_endpoint");
    argsList.add("http://localhost:" + dynamoDbServerPort);

    SparkDynamoDBExport exporter = new SparkDynamoDBExport();
    assertEquals(0, exporter.run(argsList.toArray(new String[0])));

    String foundData = getOutputData(outputDir);
    String expectedData = resourceToString(format("/export/spark/expected/%s.json", testName));
    assertEquals(expectedData, sortLines(foundData.toString()));
  }

  private static void createTestTable(AmazonDynamoDB dynamoDbClient) {
    CreateTableRequest createTableRequest =
        new CreateTableRequest()
            .withTableName(TABLE_NAME)
            .withProvisionedThroughput(getSimpleProvisionedThroughput())
            .withKeySchema(
                Arrays.asList(
                    createPartitionKeySchemaElement("id"),
                    createSortKeySchemaElement("version")))
            .withAttributeDefinitions(
                Arrays.asList(
                    createNumericAttributeDefinition("id"),
                    createNumericAttributeDefinition("version"),
                    createNumericAttributeDefinition("index_key"),
                    createNumericAttributeDefinition("last_modified")))
            .withGlobalSecondaryIndexes(Collections.singletonList(new GlobalSecondaryIndex()
                    .withIndexName(INDEX_NAME)
                    .withProvisionedThroughput(getSimpleProvisionedThroughput())
                    .withKeySchema(
                        Arrays.asList(
                            createPartitionKeySchemaElement("index_key"),
                            createSortKeySchemaElement("last_modified")))
                    .withProjection(
                        new Projection().withProjectionType(ProjectionType.ALL)
                    )
                )
            );

    dynamoDbClient.createTable(createTableRequest);
  }

  private static AmazonDynamoDB startDynamoDb() throws Exception {
    // need to set libsqlite4java native library path for embedded DDB
    System.setProperty("sqlite4java.library.path", "target/native-test-libs");

    // if these creds don't align with those used by the job, the job won't find the table and
    // we get "Cannot do operations on a non-existent table" errors
    System.setProperty("aws.accessKeyId", "foo");
    System.setProperty("aws.secretKey", "bar");

    dynamoDbServerPort = getAvailablePort();
    dynamoDbServer = ServerRunner.createServerFromCommandLineArgs(
        new String[] { "-inMemory", "-port", Integer.toString(dynamoDbServerPort) });
    dynamoDbServer.start();

    return AmazonDynamoDBClientBuilder.standard()
        .withEndpointConfiguration(
            new AwsClientBuilder.EndpointConfiguration("http://localhost:" + dynamoDbServerPort,
                DynamoDBConstants.DEFAULT_AWS_REGION))
        .build();
  }

  private static ProvisionedThroughput getSimpleProvisionedThroughput() {
    return new ProvisionedThroughput().withReadCapacityUnits(1L).withWriteCapacityUnits(1L);
  }

  private static AttributeDefinition createNumericAttributeDefinition(String attributeName) {
    return new AttributeDefinition(attributeName, "N");
  }

  private static KeySchemaElement createPartitionKeySchemaElement(String keyName) {
    return new KeySchemaElement(keyName, KeyType.HASH);
  }

  private static KeySchemaElement createSortKeySchemaElement(String keyName) {
    return new KeySchemaElement(keyName, KeyType.RANGE);
  }

  private String getTestName() {
    return Thread.currentThread().getStackTrace()[2].getMethodName();
  }

  private Path getOutputDir(String testName) throws IOException {
    return Files.createTempDirectory(this.getClass().getSimpleName()).resolve(testName);
  }

  private static int getAvailablePort() throws IOException {
    ServerSocket serverSocket = new ServerSocket(0);
    int port = serverSocket.getLocalPort();
    serverSocket.close();
    return port;
  }

  private static long toSeconds(String timestamp) {
    return Instant.parse(timestamp).getEpochSecond();
  }

  private static String getOutputData(Path outputDir) {
    // cat found data from all part files
    File[] partFiles = outputDir.toFile().listFiles((d, name) -> name.startsWith("part-"));
    assertNotNull("No output data found", partFiles);

    StringBuilder foundData = new StringBuilder();
    for (File partFile : partFiles) {
      foundData.append(fileToString(partFile.getAbsolutePath()));
    }
    return foundData.toString();
  }

  private static String sortLines(String input) {
    String[] lines = input.split("\n");
    Arrays.sort(lines);
    return String.join("\n", lines);
  }

  private static String fileToString(String fileName) {
    try {
      return IOUtils.toString(URI.create("file://" + fileName), UTF_8);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  private static String resourceToString(String resourceName) {
    try {
      return IOUtils.resourceToString(resourceName, UTF_8);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }
}

