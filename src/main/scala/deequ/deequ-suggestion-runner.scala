// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0
import com.amazonaws.services.glue.ChoiceOption
import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.MappingSpec
import com.amazonaws.services.glue.ResolveSpec
import com.amazonaws.services.glue.errors.CallSite
import com.amazonaws.services.glue.util.GlueArgParser
import com.amazonaws.services.glue.util.Job
import com.amazonaws.services.glue.util.JsonOptions
import org.apache.spark.SparkContext

import scala.collection.JavaConverters._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd.RDD

import scala.util.matching.Regex
import java.util.HashMap

import GlueApp.getTimestamp
import org.slf4j.LoggerFactory
import com.amazonaws.services.dynamodbv2.model.AttributeValue
import org.apache.hadoop.io.Text
import org.apache.hadoop.dynamodb.DynamoDBItemWritable

/* Importing DynamoDBInputFormat and DynamoDBOutputFormat */
import org.apache.hadoop.dynamodb.read.DynamoDBInputFormat
import org.apache.hadoop.dynamodb.write.DynamoDBOutputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.io.LongWritable

//Amazon Deequ Import
import com.amazon.deequ.suggestions.{ConstraintSuggestionRunner, Rules}
import com.amazon.deequ.analyzers._
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Properties
import java.util.UUID.randomUUID

/** *
 *
 */
object GlueApp {

  val sparkContext: SparkContext = new SparkContext()
  val glueContext: GlueContext = new GlueContext(sparkContext)
  val sqlContext = new SQLContext(sparkContext)
  val spark = glueContext.getSparkSession

  val getYear = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy"))
  val getMonth = LocalDate.now().format(DateTimeFormatter.ofPattern("MM"))
  val getDay = LocalDate.now().format(DateTimeFormatter.ofPattern("dd"))
  val getTimestamp = new SimpleDateFormat("HH-mm-ss").format(new Date)
  val col_extractValue = udf(extractValue)
  import spark.implicits._

  def main(sysArgs: Array[String]) {

    //***********************************************************************//
    // Step1: Create Glue Context and extract Args
    //***********************************************************************//
    val args = GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME",
      "inputDynamoTable",
      "outputDynamoTable",
      "targetBucketName",
      "targetBucketPrefix").toArray)
    Job.init(args("JOB_NAME"), glueContext, args.asJava)
    val logger = LoggerFactory.getLogger(args("JOB_NAME"))

    logger.info("Start Job")

    //***********************************************************************//
    // Step2: Get the list of Glue Tables from DynamoDB table
    //***********************************************************************//
    val glueTableListDF = readGlueTablesFromDynamoDB(args("inputDynamoTable"))

    //***********************************************************************//
    // Step3: Read each Glue table from dyanmo DB, run suggestions and write to Dynamo and S3
    //***********************************************************************//
    glueTableListDF.collect().foreach {
      row =>
        var glueDB = row.mkString(",").split(",")(0)
        var glueTable = row.mkString(",").split(",")(1)
        val glueTableDF = loadGlueTable(glueDB, glueTable)
        val finalSuggestionsDF = processSuggestions(glueTableDF, glueDB, glueTable)
        writeSuggestionsToDynamo(finalSuggestionsDF, args("outputDynamoTable"))
        writeDStoS3(finalSuggestionsDF, glueDB, glueTable, args("targetBucketName"), args("targetBucketPrefix"))
    }

    Job.commit()

  }

  /***
   * Reading GLUE tables from DynamoDB table
   * @param dynoTable
   * @return
   */
  def readGlueTablesFromDynamoDB(dynoTable: String): DataFrame = {

    var jobConf_add = new JobConf(spark.sparkContext.hadoopConfiguration)
    jobConf_add.set("dynamodb.input.tableName", dynoTable)
    jobConf_add.set("mapred.output.format.class", "org.apache.hadoop.dynamodb.write.DynamoDBOutputFormat")
    jobConf_add.set("mapred.input.format.class", "org.apache.hadoop.dynamodb.read.DynamoDBInputFormat")

    var hadoopRDD = sparkContext.hadoopRDD(jobConf_add, classOf[DynamoDBInputFormat], classOf[Text], classOf[DynamoDBItemWritable])

    val glueRDD: RDD[(String, String, String)] = hadoopRDD.map {
      case (text, dbwritable) => (dbwritable.getItem().get("database").toString(),
        dbwritable.getItem().get("tablename").toString(),
        dbwritable.getItem().get("enable").toString())
    }

    glueRDD.toDF
      .withColumn("database", col_extractValue($"_1"))
      .withColumn("tablename", col_extractValue($"_2"))
      .withColumn("enable", col_extractValue($"_3"))
      .select("database", "tablename", "enable")
      .where($"enable" === "Y")

  }

  /***
   * Create DF from Glue Table
   * @param glueDB
   * @param glueTable
   * @return
   */
  def loadGlueTable(glueDB: String, glueTable: String): DataFrame = {

    glueContext.getCatalogSource(database = glueDB,
      tableName = glueTable,
      redshiftTmpDir = "",
      transformationContext = "dataset").getDynamicFrame().toDF()

  }

  /***
   * Process constraint suggestions for each Glue table
   * @param glueDF
   * @param glueDB
   * @param glueTable
   * @return
   */
  def processSuggestions(glueDF: DataFrame, glueDB: String, glueTable: String): DataFrame = {

    val suggestionResult = {
      ConstraintSuggestionRunner()
        .onData(glueDF)
        .addConstraintRules(Rules.DEFAULT)
        .run()
    }

    val suggestionDataFrame = suggestionResult.constraintSuggestions.flatMap {
      case (column, suggestions) =>
        suggestions.map { constraint =>
          (column, constraint.description, constraint.codeForConstraint)
        }
    }.toSeq.toDS()

    val uuid = udf(() => java.util.UUID.randomUUID().toString)
    val now = LocalDateTime.now().toString()+"Z"
    suggestionDataFrame
      .withColumn("id", uuid())      
      .withColumn("database", lit(glueDB))
      .withColumn("tablename", lit(glueTable))
      .withColumnRenamed("_1", "column")
      .withColumnRenamed("_2", "constraint")
      .withColumnRenamed("_3", "constraintCode")
      .withColumn("enable", lit("N"))
      .withColumn("__typename", lit("DataQualitySuggestion"))
      .withColumn("createdAt", lit(now))
      .withColumn("updatedAt", lit(now))
  }

  /***
   * Write suggestions dataframe to DynamoDB table
   * @param finalSuggestionsDF
   * @param outputDynamoTable
   * @return
   */
  def writeSuggestionsToDynamo(finalSuggestionsDF: DataFrame, outputDynamoTable: String) = {

    val dynoWriteConf = new JobConf(spark.sparkContext.hadoopConfiguration)
    dynoWriteConf.set("dynamodb.output.tableName", outputDynamoTable)
    dynoWriteConf.set("dynamodb.throughput.write.percent", "1.5")
    dynoWriteConf.set("mapred.input.format.class", "org.apache.hadoop.dynamodb.read.DynamoDBInputFormat")
    dynoWriteConf.set("mapred.output.format.class", "org.apache.hadoop.dynamodb.write.DynamoDBOutputFormat")
    dynoWriteConf.set("mapred.output.committer.class", "org.apache.hadoop.mapred.FileOutputCommitter")

    val schema_ddb = finalSuggestionsDF.dtypes

    var dynoInsertFormattedRDD = finalSuggestionsDF.rdd.map(a => {
      val ddbMap = new HashMap[String, AttributeValue]()
      for (i <- 0 to schema_ddb.length - 1) {
        val value = a.get(i)
        if (value != null) {
          val att = new AttributeValue()
          att.setS(value.toString)
          ddbMap.put(schema_ddb(i)._1, att)
        }
      }
      val item = new DynamoDBItemWritable()
      item.setItem(ddbMap)
      (new Text(""), item)
    }
    )
    dynoInsertFormattedRDD.saveAsHadoopDataset(dynoWriteConf)

  }

  /***
   * Write results data set to S3
   * @param resultDF
   * @param dbName
   * @param tableName
   * @param s3Bucket
   * @param s3Prefix
   * @return
   */
  def writeDStoS3(resultDF: DataFrame, dbName: String, tableName: String, s3Bucket: String, s3Prefix: String) = {

    resultDF.write.mode("append").parquet(s3Bucket + "/"
      + "deequ" + "/"
      + s3Prefix + "/"
      + "glue_db=" + dbName + "/"
      + "glue_table=" + tableName + "/"
      + "year=" + getYear + "/"
      + "month=" + getMonth + "/"
      + "day=" + getDay + "/"
      + "hour=" + getTimestamp.split("-")(0) + "/"
      + "minute=" + getTimestamp.split("-")(1) + "/"
    )

  }

  /***
   * extarct value UDF
   * @return
   */
  def extractValue: (String => String) = (aws: String) => {
    val pat_value = "\\s(.*),".r

    val matcher = pat_value.findFirstMatchIn(aws)
    matcher match {
      case Some(number) => number.group(1).toString
      case None => ""
    }
  }

}