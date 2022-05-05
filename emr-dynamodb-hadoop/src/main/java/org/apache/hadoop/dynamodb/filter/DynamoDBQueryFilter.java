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

package org.apache.hadoop.dynamodb.filter;

import com.amazonaws.services.dynamodbv2.model.Condition;
import java.util.HashMap;
import java.util.Map;

public class DynamoDBQueryFilter {

  private final Map<String, Condition> keyConditions;
  private final Map<String, Condition> scanFilter;

  private DynamoDBIndexInfo index;

  public DynamoDBQueryFilter() {
    this(new HashMap<>(), new HashMap<>(), null);
  }

  private DynamoDBQueryFilter(Map<String, Condition> keyConditions,
      Map<String, Condition> scanFilter, DynamoDBIndexInfo index) {
    this.keyConditions = keyConditions;
    this.scanFilter = scanFilter;
    this.index = index;
  }

  public DynamoDBIndexInfo getIndex() {
    return index;
  }

  public void setIndex(DynamoDBIndexInfo index) {
    this.index = index;
  }

  public Map<String, Condition> getKeyConditions() {
    return keyConditions;
  }

  public void addKeyCondition(DynamoDBFilter filter) {
    this.keyConditions.put(filter.getColumnName(), filter.getDynamoDBCondition());
  }

  public Map<String, Condition> getScanFilter() {
    return scanFilter;
  }

  public void addScanFilter(DynamoDBFilter filter) {
    this.scanFilter.put(filter.getColumnName(), filter.getDynamoDBCondition());
  }

  public DynamoDBQueryFilter clone() {
    return new DynamoDBQueryFilter(clone(keyConditions), clone(scanFilter), index);
  }

  private Map<String, Condition> clone(Map<String, Condition> conditionMap) {
    Map<String, Condition> clone = new HashMap<>();
    for (String key : conditionMap.keySet()) {
      clone.put(key, conditionMap.get(key).clone());
    }
    return clone;
  }
}
