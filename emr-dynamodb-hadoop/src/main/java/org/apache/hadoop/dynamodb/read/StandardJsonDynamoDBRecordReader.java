package org.apache.hadoop.dynamodb.read;

import org.apache.hadoop.dynamodb.DynamoDBItemWritable;
import org.apache.hadoop.dynamodb.DynamoDBStandardJsonWritable;
import org.apache.hadoop.dynamodb.preader.DynamoDBRecordReaderContext;
import org.apache.hadoop.io.Text;

public class StandardJsonDynamoDBRecordReader extends AbstractDynamoDBRecordReader<Text,
    DynamoDBStandardJsonWritable> {

  public StandardJsonDynamoDBRecordReader(DynamoDBRecordReaderContext context) {
    super(context);
  }

  @Override
  protected void convertDynamoDBItemToValue(DynamoDBItemWritable item,
      DynamoDBStandardJsonWritable toValue) {
    toValue.setItem(item.getItem());
  }

  @Override
  public Text createKey() {
    return new Text();
  }

  @Override
  public DynamoDBStandardJsonWritable createValue() {
    return new DynamoDBStandardJsonWritable();
  }
}
