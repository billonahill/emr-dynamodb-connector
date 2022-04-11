package org.apache.hadoop.dynamodb.tools;

import static java.lang.String.format;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.UnrecognizedOptionException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.dynamodb.DynamoDBConstants;
import org.apache.hadoop.dynamodb.DynamoDBStandardJsonWritable;
import org.apache.hadoop.dynamodb.read.MultipleRowKeyExportInputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkDynamoDBExport extends Configured implements Tool {
  public static final Log log = LogFactory.getLog(SparkDynamoDBExport.class);
  public static final String AWS_CREDENTIAL_PROVIDER =
      DefaultAWSCredentialsProviderChain.class.getName();

  private final Option helpOption = new Option("h", "help", false, "Print usage info and exit");

  public Options buildOptions() {
    Options options = new Options()
        .addOption("t", "table_name", true, "Dynamo table name (required)")
        .addOption("i", "index_name", true, "Dynamo index name (required)")
        .addOption("r", "row_key", true,"Dynamo index row key name (required)")
        .addOption("s", "sort_key", true, "Dynamo index sort key name (required)")
        .addOption("n", "min_sort_key", true,
            "Minimum sort key value (e.g., 2022-03-02T12:00:00) (required)")
        .addOption("x", "max_sort_key", true,
            "Maximum sort key value  (e.g., 2022-03-02T13:00:00) (required)")
        .addOption("p", "sample_percent", true, "Percentage of records to sample")
        .addOption("o", "output_path", true, "Output path location for exported data (required)")
        .addOption("a", "attributes", true,
            "Dynamo record attributes to fetch, comma-separated. Default is all")
        .addOption("R", "read_ratio", true,"Maximum percent of the specified "
            + "DynamoDB table's read capacity to use for export, default = 0.5")
        .addOption("l", "local_mode", false, "Whether to run Spark in local mode, default is false")
        .addOption("D", "dynamo_endpoint", true,
            "Override the default dynamo endpoint resolution, for testing")
        .addOption(helpOption);

    for (String opt : new String[] { "t", "i", "r", "s", "n", "x", "p", "o" }) {
      options.getOption(opt).setRequired(true);
    }

    for (String opt : new String[] { "p", "R" }) {
      options.getOption(opt).setType(Number.class);
    }

    return options;
  }

  public static void main(String[] args) throws Exception {
    // our spark log4j configs set logging to WARN by default, so override that to get main logging
    LogManager.getLogger(SparkDynamoDBExport.class).setLevel(Level.INFO);
    LogManager.getLogger(DynamoDBExport.class).setLevel(Level.INFO);

    int res = ToolRunner.run(new Configuration(), new SparkDynamoDBExport(), args);
    System.exit(res);
  }

  private void usage(Options options) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.setWidth(150);
    String footer = format(
        "\nTo run in localmode, first comment out <scope>provided</scope> for spark and hadoop-aws "
            + "in pom.xml, then build: \n\n"
            + " mvn clean install -pl emr-dynamodb-hadoop,emr-dynamodb-tools -DskipTests \n\n"
            + "Then run: \n\n"
            + " java -cp `ls ./emr-dynamodb-tools/target/emr-dynamodb-tools-*-SNAPSHOT-jar-"
            + "with-dependencies.jar`:emr-dynamodb-hadoop/src/test/resources/. \\\n"
            + "  org.apache.hadoop.dynamodb.tools.SparkDynamoDBExport \\\n"
            + "  -t products \\\n"
            + "  -i products-updated-at-timestamp-index-idx \\\n"
            + "  -r updated_at_hk_field -s updated_timestamp \\\n"
            + "  -n 2022-03-02T12:00:00 -x 2022-03-02T13:00:00 \\\n"
            + "  -o s3a://[my-bucket]/products_spark_ddb_export_json -a id -p 0.01 -l",
        System.getenv("USER"));
    formatter.printHelp("java " + this.getClass().getName() + " [OPTIONS]", "\n", options, footer);
  }

  private boolean checkHelp(String[] args) throws org.apache.commons.cli.ParseException {
    Options options = new Options().addOption(helpOption);
    CommandLineParser parser = new BasicParser();
    try {
      CommandLine cmd = parser.parse(options, args);
      return cmd.hasOption(helpOption.getOpt());
    } catch (UnrecognizedOptionException e) {
      return false;
    }
  }

  public int run(String[] args) throws Exception {
    Options options = buildOptions();
    if (checkHelp(args)) {
      usage(options);
      return 0;
    }

    CommandLine cmd;
    try {
      CommandLineParser parser = new BasicParser();
      cmd = parser.parse(options, args);
    } catch (org.apache.commons.cli.ParseException e) {
      System.out.println(e.getMessage());
      usage(options);
      return 1;
    }

    if (cmd.hasOption(helpOption.getOpt())) {
      usage(options);
      return 0;
    }

    log.info("Command line options:");
    for (Option option : cmd.getOptions()) {
      log.info(format("%s/%s: %s", option.getOpt(), option.getLongOpt(), option.getValue()));
    }

    String outputPath = cmd.getOptionValue("o");
    boolean localMode = cmd.hasOption("l");
    JobConf jobConf = initJobConf(cmd, outputPath, localMode);

    // default behavior is for the BasicAWSCredentialsProvider to be used, which does not work
    SparkConf sparkConf = new SparkConf();
    sparkConf.set("spark.hadoop.fs.s3a.aws.credentials.provider", AWS_CREDENTIAL_PROVIDER);
    sparkConf.set("spark.hadoop.fs.s3a.downgrade.syncable.exceptions", "true");

    JavaSparkContext sc =
        localMode
            ? new JavaSparkContext("local", this.getClass().getSimpleName(), sparkConf)
            : new JavaSparkContext(sparkConf);

    JavaRDD<DynamoDBStandardJsonWritable> rdd = sc
        .hadoopRDD(jobConf, MultipleRowKeyExportInputFormat.class, Text.class,
            DynamoDBStandardJsonWritable.class)
        .values();
    rdd.saveAsTextFile(outputPath);

    sc.stop();

    return 0;
  }

  private JobConf initJobConf(CommandLine cmd, String outputPath, boolean localMode)
      throws org.apache.commons.cli.ParseException, ParseException {

    JobConf jobConf = new JobConf(this.getClass());

    jobConf.set("dynamodb.customAWSCredentialsProvider", AWS_CREDENTIAL_PROVIDER);
    if (localMode) {
      jobConf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

      // for testing in CI. When running locally env variables will supersede
      System.setProperty("aws.accessKeyId", "foo");
      System.setProperty("aws.secretKey", "bar");
    }

    if (cmd.hasOption("D")) {
      jobConf.set("dynamodb.endpoint", cmd.getOptionValue("D"));
    }

    String tableName = cmd.getOptionValue("t");
    Double readRatio = cmd.hasOption("R")
        ? (Double) cmd.getParsedOptionValue("R")
        : Double.parseDouble(DynamoDBConstants.DEFAULT_THROUGHPUT_PERCENTAGE);

    jobConf =
        new DynamoDBExport().initJobConf(jobConf, new Path(outputPath), tableName, readRatio, null);

    jobConf.set(DynamoDBConstants.INDEX_NAME, cmd.getOptionValue("i"));
    jobConf.set(DynamoDBConstants.ROW_KEY_NAME, cmd.getOptionValue("r"));
    jobConf.set(DynamoDBConstants.SORT_KEY_NAME, cmd.getOptionValue("s"));
    jobConf.setDouble(DynamoDBConstants.ROW_SAMPLE_PERCENT, (Double) cmd.getParsedOptionValue("p"));
    jobConf.setLong(DynamoDBConstants.SORT_KEY_MIN_VALUE, toSeconds(cmd.getOptionValue("n")));
    jobConf.setLong(DynamoDBConstants.SORT_KEY_MAX_VALUE, toSeconds(cmd.getOptionValue("x")));
    jobConf.setInt(MRJobConfig.MAP_MEMORY_MB, MRJobConfig.DEFAULT_MAP_MEMORY_MB);

    if (cmd.hasOption("a")) {
      jobConf.set(DynamoDBConstants.ATTRIBUTES_TO_GET, cmd.getOptionValue("a"));
    }

    return jobConf;
  }

  private static long toSeconds(String dateTime) throws ParseException {
    return parseDate(dateTime).getTime() / 1000;
  }

  private static Date parseDate(String dateTime) throws ParseException {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
    TimeZone gmt = TimeZone.getTimeZone("GMT");
    sdf.setTimeZone(gmt);
    return sdf.parse(dateTime);
  }
}
