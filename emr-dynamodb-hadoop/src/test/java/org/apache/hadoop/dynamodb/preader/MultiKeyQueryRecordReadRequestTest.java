package org.apache.hadoop.dynamodb.preader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.dynamodb.DynamoDBClient;
import org.apache.hadoop.dynamodb.DynamoDBConstants;
import org.apache.hadoop.dynamodb.DynamoDBFibonacciRetryer.RetryResult;
import org.apache.hadoop.dynamodb.filter.DynamoDBFilterOperator;
import org.apache.hadoop.dynamodb.filter.DynamoDBNAryFilter;
import org.apache.hadoop.dynamodb.filter.DynamoDBQueryFilter;
import org.apache.hadoop.dynamodb.preader.RateController.RequestLimit;
import org.apache.hadoop.dynamodb.split.DynamoDBSegmentsSplit;
import org.apache.hadoop.dynamodb.type.DynamoDBTypeFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public final class MultiKeyQueryRecordReadRequestTest {

  public static final String ROW_KEY = "some_row_key";
  public static final String TABLE_NAME = "some_table";
  public static final String ATTRIBUTES = "color,size";

  @Mock
  DynamoDBRecordReaderContext context;
  @Mock
  DynamoDBClient client;
  @Mock
  DynamoDBSegmentsSplit split;
  @Mock
  ConsumedCapacity consumedCapacity;
  @Mock
  DynamoDBQueryFilter filter;

  private JobConf jobConf;

  @Before
  public void initTest() {
    jobConf = new JobConf();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRequireRowKeyName() {
    doTest(jobConf);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRequireTableName() {
    jobConf.set(DynamoDBConstants.ROW_KEY_NAME, ROW_KEY);
    doTest(jobConf);
  }

  @Test
  public void testSuccess() {
    jobConf.set(DynamoDBConstants.ROW_KEY_NAME, ROW_KEY);
    jobConf.set(DynamoDBConstants.INPUT_TABLE_NAME, TABLE_NAME);
    doTest(jobConf);
  }

  @Test
  public void testSuccessWithAttributes() {
    jobConf.set(DynamoDBConstants.ROW_KEY_NAME, ROW_KEY);
    jobConf.set(DynamoDBConstants.INPUT_TABLE_NAME, TABLE_NAME);
    jobConf.set(DynamoDBConstants.ATTRIBUTES_TO_GET, ATTRIBUTES);
    doTest(jobConf, ATTRIBUTES.split(","));
  }

  private void doTest(JobConf jobConf) {
    doTest(jobConf, null);
  }

  private void doTest(JobConf jobConf, String... expectedAttributes) {
    RetryResult stubbedResult =
        new RetryResult<>(new QueryResult().withConsumedCapacity(consumedCapacity)
            .withItems(new HashMap<String, AttributeValue>()), 0);
    stubScanTableWith(stubbedResult);

    when(context.getClient()).thenReturn(client);
    when(context.getConf()).thenReturn(jobConf);
    when(context.getSplit()).thenReturn(split);
    when(split.getFilterPushdown()).thenReturn(filter);
    when(filter.clone()).thenReturn(filter);
    when(consumedCapacity.getCapacityUnits()).thenReturn(0.6);

    int segment = 7;
    MultiKeyQueryReadManager readManager = Mockito.mock(MultiKeyQueryReadManager.class);
    AbstractRecordReadRequest readRequest =
        new MultiKeyQueryRecordReadRequest(readManager, context, segment, null);
    PageResults<Map<String, AttributeValue>> pageResults =
        readRequest.fetchPage(new RequestLimit(0, 0));
    assertEquals(0.6, pageResults.consumedRcu, 0.0);

    if (expectedAttributes == null) {
      verify(client, times(1)).queryTable(
          eq(TABLE_NAME),
          eq(filter),
          any(Map.class),
          eq(0L),
          any(Reporter.class),
          anyString());
    } else if (expectedAttributes.length == 2) {
      verify(client, times(1)).queryTable(
          eq(TABLE_NAME),
          eq(filter),
          any(Map.class),
          eq(0L),
          any(Reporter.class),
          eq(expectedAttributes[0]),
          eq(expectedAttributes[1]));
    } else {
      fail("Unimplemented number of attributes passed");
    }

    ArgumentCaptor<DynamoDBNAryFilter> foundFilter = ArgumentCaptor.forClass(DynamoDBNAryFilter.class);
    verify(filter, times(1)).addKeyCondition(foundFilter.capture());
    assertEquals(ROW_KEY, foundFilter.getValue().getColumnName());
    assertEquals(DynamoDBTypeFactory.NUMBER_TYPE.getDynamoDBType(),
        foundFilter.getValue().getColumnType());
    assertEquals(DynamoDBFilterOperator.EQ, foundFilter.getValue().getOperator());
    assertEquals(1, foundFilter.getValue().getColumnValues().length);
    assertEquals(Integer.toString(segment), foundFilter.getValue().getColumnValues()[0]);
  }

  private void stubScanTableWith(RetryResult<QueryResult> queryResultRetryResult) {
    //String tableName, DynamoDBQueryFilter dynamoDBQueryFilter, Map<String, AttributeValue>
    //      exclusiveStartKey, long limit, Reporter reporter, String... attributesToGet
    when(client.queryTable(
        anyString(),
        any(DynamoDBQueryFilter.class),
        any(Map.class),
        anyLong(),
        any(Reporter.class),
        any())
    ).thenReturn(queryResultRetryResult);

    when(client.queryTable(
        anyString(),
        any(DynamoDBQueryFilter.class),
        any(Map.class),
        anyLong(),
        any(Reporter.class),
        any(),
        any())
    ).thenReturn(queryResultRetryResult);
  }

}
