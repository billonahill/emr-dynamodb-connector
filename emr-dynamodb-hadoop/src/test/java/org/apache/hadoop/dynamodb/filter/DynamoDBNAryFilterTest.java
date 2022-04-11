package org.apache.hadoop.dynamodb.filter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.Condition;
import java.util.Iterator;
import org.apache.hadoop.dynamodb.type.DynamoDBNumberType;
import org.apache.hadoop.dynamodb.type.DynamoDBStringType;
import org.apache.hadoop.dynamodb.type.DynamoDBType;
import org.apache.hadoop.dynamodb.type.DynamoDBTypeConstants;
import org.junit.Test;

public class DynamoDBNAryFilterTest {
  private static final String COLUMN_NAME = "color";

  @Test
  public void testSingleStringValue() {
    doTest(DynamoDBFilterOperator.EQ, new DynamoDBStringType(), "blue");
  }

  @Test
  public void testMultipleStringValues() {
    doTest(DynamoDBFilterOperator.IN, new DynamoDBStringType(), "red", "green");
  }

  @Test
  public void testSingleNumberValue() {
    doTest(DynamoDBFilterOperator.EQ, new DynamoDBNumberType(), "135");
  }

  @Test
  public void testMultipleNumberValue() {
    doTest(DynamoDBFilterOperator.BETWEEN, new DynamoDBNumberType(), "107", "145");
  }

  private void doTest(DynamoDBFilterOperator operator, DynamoDBType columnType,
      String... values) {
    DynamoDBFilter filter = new DynamoDBNAryFilter(COLUMN_NAME, operator, columnType, values);
    assertEquals(COLUMN_NAME, filter.getColumnName());
    assertEquals(operator, filter.getOperator());
    assertEquals(columnType.getDynamoDBType(), filter.getColumnType());

    Condition condition = filter.getDynamoDBCondition();
    assertEquals(values.length, condition.getAttributeValueList().size());
    Iterator<AttributeValue> attributes = condition.getAttributeValueList().iterator();

    for (String value : values) {
      AttributeValue attributeValue = attributes.next();
      String type = columnType.getDynamoDBType();

      if (type.equals(DynamoDBTypeConstants.STRING)) {
        assertEquals(value, attributeValue.getS());
      } else if (type.equals(DynamoDBTypeConstants.NUMBER)) {
        assertEquals(value, attributeValue.getN());
      } else {
        fail("Unrecognized type: " + columnType);
      }
    }
  }
}
