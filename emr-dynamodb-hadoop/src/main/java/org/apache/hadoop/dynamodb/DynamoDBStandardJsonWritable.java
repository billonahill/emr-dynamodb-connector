package org.apache.hadoop.dynamodb;

import com.amazonaws.services.dynamodbv2.document.ItemUtils;
import java.io.DataInput;

/**
 * Writable that produces standard json without type annotations in a way that can be consumed by
 * JsonSerde, unlike DynamoDBItemWritable which produces typed json in the DynamoDB format.
 *
 * See https://stackoverflow.com/questions/43812278/converting-dynamodb-json-to-standard-json-with-java
 */
public class DynamoDBStandardJsonWritable extends DynamoDBItemWritable {

  @Override
  public String writeStream() {
    return ItemUtils.toItem(getItem()).toJSON();
  }

  @Override
  public void readFields(DataInput in) throws UnsupportedOperationException {
    throw new UnsupportedOperationException(
        "This class supports serializing DynamoDB items as json, "
            + "but not deserializing json back to items");
  }
}
