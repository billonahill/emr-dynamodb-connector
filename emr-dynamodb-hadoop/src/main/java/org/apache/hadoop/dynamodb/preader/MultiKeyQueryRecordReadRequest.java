/**
 * Copyright 2012-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file
 * except in compliance with the License. A copy of the License is located at
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "LICENSE.TXT" file accompanying this file. This file is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under the License.
 */

package org.apache.hadoop.dynamodb.preader;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import java.util.Map;
import org.apache.hadoop.dynamodb.DynamoDBConstants;
import org.apache.hadoop.dynamodb.DynamoDBFibonacciRetryer.RetryResult;
import org.apache.hadoop.dynamodb.filter.DynamoDBFilterOperator;
import org.apache.hadoop.dynamodb.preader.RateController.RequestLimit;
import org.apache.hadoop.dynamodb.split.DynamoDBNAryFilter;
import org.apache.hadoop.dynamodb.type.DynamoDBTypeFactory;

public class MultiKeyQueryRecordReadRequest extends AbstractRecordReadRequest {

  public MultiKeyQueryRecordReadRequest(AbstractReadManager readMgr, DynamoDBRecordReaderContext context,
      int segment, Map<String, AttributeValue> lastEvaluatedKey) {
    super(readMgr, context, segment, lastEvaluatedKey);
  }

  @Override
  protected AbstractRecordReadRequest buildNextReadRequest(PageResults<Map<String,
      AttributeValue>> pageResults) {
    return new MultiKeyQueryRecordReadRequest(readMgr, context, segment, pageResults.lastEvaluatedKey);
  }

  @Override
  protected PageResults<Map<String, AttributeValue>> fetchPage(RequestLimit lim) {
    String rowKeyName = context.getConf().get(DynamoDBConstants.ROW_KEY_NAME);
    if (rowKeyName == null || rowKeyName.length() == 0) {
      throw new IllegalArgumentException(
          "required job config not found: " + DynamoDBConstants.ROW_KEY_NAME);
    }

    context.getSplit().getFilterPushdown().addKeyCondition(
        new DynamoDBNAryFilter(rowKeyName, DynamoDBFilterOperator.EQ,
            DynamoDBTypeFactory.NUMBER_TYPE, Long.toString(segment)));

    String attributesCsv = context.getConf().get(DynamoDBConstants.ATTRIBUTES_TO_GET);
    String[] attributes = attributesCsv != null && attributesCsv.trim().length() > 0
        ? attributesCsv.trim().split(",")
        : null;

    // Read from DynamoDB
    RetryResult<QueryResult> retryResult = context.getClient().queryTable(tableName, context
            .getSplit().getFilterPushdown(), lastEvaluatedKey, lim.items, context.getReporter(),
        attributes);

    QueryResult result = retryResult.result;
    int retries = retryResult.retries;

    return new PageResults<>(result.getItems(), result.getLastEvaluatedKey(), result
        .getConsumedCapacity().getCapacityUnits(), retries);
  }
}
