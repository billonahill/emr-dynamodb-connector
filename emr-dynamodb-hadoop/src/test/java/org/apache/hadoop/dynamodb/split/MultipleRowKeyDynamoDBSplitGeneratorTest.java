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

package org.apache.hadoop.dynamodb.split;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertThrows;

import org.apache.hadoop.dynamodb.DynamoDBConstants;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Before;
import org.junit.Test;

public class MultipleRowKeyDynamoDBSplitGeneratorTest {

  private static final int IGNORED_NUM_SEGEMENTS = 999;

  private JobConf jobConf;
  private DynamoDBSplitGenerator splitGenerator;

  @Before
  public void init() {
    jobConf = getTestConf();
    splitGenerator = new MultipleRowKeyDynamoDBSplitGenerator(jobConf);
  }

  @Test
  public void testRequiredSettings() {
    JobConf jobConf = new JobConf();
    String[] requiredStringSettings = new String[] {
        DynamoDBConstants.INDEX_NAME,
        DynamoDBConstants.ROW_KEY_NAME,
        DynamoDBConstants.SORT_KEY_NAME,
    };

    String[] requiredLongSettings = new String[] {
        DynamoDBConstants.SORT_KEY_MIN_VALUE,
        DynamoDBConstants.SORT_KEY_MAX_VALUE,
    };

    for (String setting : requiredStringSettings) {
      assertIllegalArgumentException(jobConf, setting);
      jobConf.set(setting, setting + "_value");
    }

    for (String setting : requiredLongSettings) {
      assertIllegalArgumentException(jobConf, setting);
      jobConf.setLong(setting, 8);
    }

    new MultipleRowKeyDynamoDBSplitGenerator(jobConf);
  }

  private void assertIllegalArgumentException(JobConf jobConf, String messageContains) {
    IllegalArgumentException e = assertThrows(
        IllegalArgumentException.class, () -> new MultipleRowKeyDynamoDBSplitGenerator(jobConf));
    assertTrue(e.getMessage().contains(messageContains));
  }

  @Test
  public void testGenerateSplitsDefaultSampling() {
    // default sampling is 0.001 of 10k key space, or 10 keys/segments
    InputSplit[] splits = splitGenerator.generateSplits(1, IGNORED_NUM_SEGEMENTS, jobConf);
    verifySplits(splits, 10, 1);

    splits = splitGenerator.generateSplits(1000, IGNORED_NUM_SEGEMENTS, jobConf);
    verifySplits(splits, 10, 10);
  }

  @Test
  public void testGenerateSplitsTenPercentSampling() {
    jobConf.setDouble(DynamoDBConstants.ROW_SAMPLE_PERCENT, 0.1);
    splitGenerator = new MultipleRowKeyDynamoDBSplitGenerator(jobConf);

    // 10% sampling 10k key space, or 1000 keys/segments
    InputSplit[] splits = splitGenerator.generateSplits(1, IGNORED_NUM_SEGEMENTS, jobConf);
    verifySplits(splits, 1000, 1);

    splits = splitGenerator.generateSplits(1000, IGNORED_NUM_SEGEMENTS, jobConf);
    verifySplits(splits, 1000, 1000);
  }

  @Test
  public void testGenerateSplitsNoSampling() {
    jobConf.setDouble(DynamoDBConstants.ROW_SAMPLE_PERCENT, 1.0);
    splitGenerator = new MultipleRowKeyDynamoDBSplitGenerator(jobConf);

    // no sampling is 100% sampling 10k key space, or 10k keys/segments
    InputSplit[] splits = splitGenerator.generateSplits(1, IGNORED_NUM_SEGEMENTS, jobConf);
    verifySplits(splits, 10000, 1);

    splits = splitGenerator.generateSplits(1000, IGNORED_NUM_SEGEMENTS, jobConf);
    verifySplits(splits, 10000, 1000);
  }

  private JobConf getTestConf() {
    JobConf conf = new JobConf();
    conf.set(DynamoDBConstants.INDEX_NAME, "some_index");
    conf.set(DynamoDBConstants.ROW_KEY_NAME, "some_row_key_name");
    conf.set(DynamoDBConstants.SORT_KEY_NAME, "some_sort_key_name");
    conf.setLong(DynamoDBConstants.SORT_KEY_MIN_VALUE, 3L);
    conf.setLong(DynamoDBConstants.SORT_KEY_MAX_VALUE, 5L);
    return conf;
  }

  private void verifySplits(InputSplit[] splits, int numSegments, int numMappers) {
    assertEquals("Unexpected number of mappers", numMappers, splits.length);

    boolean[] segments = new boolean[numSegments];
    for (int i = 0; i < segments.length; i++) {
      segments[i] = false;
    }

    int numSegmentsPerSplit = numSegments / splits.length;
    for (InputSplit split1 : splits) {
      DynamoDBSplit split = (DynamoDBSplit) split1;
      assertEquals("Unexpected number of segments in split", segments.length,
          split.getTotalSegments());
      for (Integer segment : split.getSegments()) {
        assertFalse(segments[segment - 1]); // Segments start at 1 by default
        segments[segment - 1] = true;
      }
      // Make sure no segment has way more than anyone else
      int numSegmentsThisSplit = split.getSegments().size();
      assertTrue(Math.abs(numSegmentsThisSplit - numSegmentsPerSplit) <= 1);
    }

    // Make sure every segment is accounted for
    for (int i = 0; i < segments.length; i++) {
      assertTrue(segments[i]);
    }
  }
}
