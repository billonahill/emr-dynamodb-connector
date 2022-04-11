package org.apache.hadoop.dynamodb.read;

import org.apache.hadoop.dynamodb.DynamoDBStandardJsonWritable;
import org.apache.hadoop.dynamodb.preader.DynamoDBRecordReaderContext;
import org.apache.hadoop.dynamodb.split.DynamoDBSplitGenerator;
import org.apache.hadoop.dynamodb.split.MultipleRowKeyDynamoDBSplitGenerator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class MultipleRowKeyExportInputFormat extends
    AbstractDynamoDBInputFormat<Text, DynamoDBStandardJsonWritable> {

  @Override
  protected DynamoDBSplitGenerator getSplitGenerator(JobConf conf) {
    return new MultipleRowKeyDynamoDBSplitGenerator(conf);
  }

  @Override
  public RecordReader<Text, DynamoDBStandardJsonWritable> getRecordReader(InputSplit split,
      JobConf conf, Reporter reporter) {
    DynamoDBRecordReaderContext context = buildDynamoDBRecordReaderContext(split, conf, reporter);
    return new StandardJsonDynamoDBRecordReader(context);
  }
}
