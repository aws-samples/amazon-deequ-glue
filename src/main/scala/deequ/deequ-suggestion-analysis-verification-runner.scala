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
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD

import scala.util.matching.Regex
import java.util.HashMap

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
import com.amazon.deequ.constraints.Constraint
import com.amazon.deequ.suggestions.{ConstraintSuggestion, ConstraintSuggestionResult, ConstraintSuggestionRunner, Rules}
//Verification and Analysis Imports
import com.amazon.deequ.analyzers.runners.AnalysisRunner
import com.amazon.deequ.{VerificationSuite, VerificationResult}
import com.amazon.deequ.VerificationResult.checkResultsAsDataFrame
import com.amazon.deequ.checks.{Check, CheckLevel}
import com.amazon.deequ.constraints.ConstrainableDataTypes
import com.amazon.deequ.analyzers.runners.{AnalysisRunner, AnalyzerContext}
import com.amazon.deequ.analyzers.runners.AnalyzerContext.successMetricsAsDataFrame
import com.amazon.deequ.analyzers._
import com.amazon.deequ.analyzers.Analyzer
import com.amazon.deequ.metrics.Metric

import java.time.LocalDate
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Properties
import java.util.UUID.randomUUID

object GlueApp {
  def main(sysArgs: Array[String]) {
    val sparkContext: SparkContext = new SparkContext()
    val glueContext: GlueContext = new GlueContext(sparkContext)
    val sqlContext = new SQLContext(sparkContext)
    val spark = glueContext.getSparkSession
    val args = GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME", "dynamodbSuggestionTableName", "dynamodbAnalysisTableName", "glueDatabase", "glueTables", "targetBucketName").toArray)
    Job.init(args("JOB_NAME"), glueContext, args.asJava)
    val logger = LoggerFactory.getLogger(args("JOB_NAME"))

    val dynamodbSuggestionTableName = args("dynamodbSuggestionTableName")
    val dynamodbAnalysisTableName = args("dynamodbAnalysisTableName")
    val dbName = args("glueDatabase")
    val tabNames = args("glueTables").split(",")
    val getYear = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy"))
    val getMonth = LocalDate.now().format(DateTimeFormatter.ofPattern("MM"))
    val getDay = LocalDate.now().format(DateTimeFormatter.ofPattern("dd"))
    val getTimestamp = new SimpleDateFormat("HH-mm-ss").format(new Date)

    // Configure connection to DynamoDB
    var jobConf_add = new JobConf(spark.sparkContext.hadoopConfiguration)
    jobConf_add.set("mapred.output.format.class", "org.apache.hadoop.dynamodb.write.DynamoDBOutputFormat")
    jobConf_add.set("mapred.input.format.class", "org.apache.hadoop.dynamodb.read.DynamoDBInputFormat")

    import spark.implicits._

    logger.info("Start Job")
    for (tabName <- tabNames) {
      logger.info("Reading Source database: " + dbName + " and table: " + tabName)

      val glueDF = glueContext.getCatalogSource(database = dbName, tableName = tabName, redshiftTmpDir = "", transformationContext = "dataset").getDynamicFrame().toDF()
      logger.info("Running Constraint Suggestor for database: " + dbName + "and table: " + tabName)

      val suggestionResult = {
        ConstraintSuggestionRunner()
          .onData(glueDF)
          .addConstraintRules(Rules.DEFAULT)
          .run()
      }

      val allConstraints: Seq[com.amazon.deequ.constraints.Constraint] = suggestionResult.constraintSuggestions.flatMap
      { case (_, suggestions) => suggestions.map {
        _.constraint
      }
      }.toSeq

      val suggestionDataFrame = suggestionResult.constraintSuggestions.flatMap {
        case (column, suggestions) =>
          suggestions.map { constraint =>
            (column, constraint.description, constraint.codeForConstraint)
          }
      }.toSeq.toDS()

      val uuid = udf(() => java.util.UUID.randomUUID().toString)
      val now = LocalDateTime.now().toString()+"Z"
      val suggestionDataFrameRenamed = suggestionDataFrame
        .withColumn("id", uuid())
        .withColumnRenamed("_1", "column")
        .withColumnRenamed("_2", "constraint")
        .withColumnRenamed("_3", "constraintCode")
        .withColumn("enable", lit("N"))
        .withColumn("tableHashKey", concat(lit(dbName), lit("-"), lit(tabName)))
        .withColumn("__typename", lit("DataQualitySuggestion"))
        .withColumn("createdAt", lit(now))
        .withColumn("updatedAt", lit(now))

      writeDStoS3(suggestionDataFrameRenamed, args("targetBucketName"), "constraint-suggestion-results", dbName, tabName, getYear, getMonth, getDay, getTimestamp)

      logger.info("Write Suggested Constraints Into Dynamo DB:" + args("dynamodbSuggestionTableName"))

      writeToDynamoDB(suggestionDataFrameRenamed, dynamodbSuggestionTableName)

      verificationRunner(glueDF, allConstraints, dbName, tabName, getYear, getMonth, getDay, getTimestamp)
      analysisRunner(glueDF, allConstraints, dbName, tabName, getYear, getMonth, getDay, getTimestamp)
    }

    logger.info("Stop Job")
    Job.commit()


    def verificationRunner(glueDF: DataFrame, allConstraints: Seq[com.amazon.deequ.constraints.Constraint], dbName: String, tabName: String, getYear: String, getMonth: String, getDay: String, getTimestamp: String) = {
      val autoGeneratedChecks = Check(CheckLevel.Error, "data constraints", allConstraints)
      val verificationResult = VerificationSuite().onData(glueDF).addChecks(Seq(autoGeneratedChecks)).run()
      val resultDataFrame = checkResultsAsDataFrame(spark, verificationResult)
      writeDStoS3(resultDataFrame, args("targetBucketName"), "constraints-verification-results", dbName, tabName, getYear, getMonth, getDay, getTimestamp)
    }


    def analysisRunner(glueDF: DataFrame, allConstraints: Seq[com.amazon.deequ.constraints.Constraint], dbName: String, tabName: String, getYear: String, getMonth: String, getDay: String, getTimestamp: String) = {
      val autoGeneratedChecks = Check(CheckLevel.Error, "data constraints", allConstraints)
      val analyzersFromChecks = Seq(autoGeneratedChecks).flatMap { _.requiredAnalyzers() }
      val analysisResult: AnalyzerContext = {
        AnalysisRunner
          .onData(glueDF)
          .addAnalyzers(analyzersFromChecks)
          .run()
      }
      val resultDataFrame = successMetricsAsDataFrame(spark, analysisResult)
      writeDStoS3(resultDataFrame, args("targetBucketName"), "constraints-analysis-results", dbName, tabName, getYear, getMonth, getDay, getTimestamp)
    }

    def writeToDynamoDB(dataFrameRenamed: DataFrame, dynoTable: String) = {

      val ddbWriteConf = new JobConf(spark.sparkContext.hadoopConfiguration)
      ddbWriteConf.set("dynamodb.output.tableName", dynoTable)
      ddbWriteConf.set("dynamodb.throughput.write.percent", "1.5")
      ddbWriteConf.set("mapred.input.format.class", "org.apache.hadoop.dynamodb.read.DynamoDBInputFormat")
      ddbWriteConf.set("mapred.output.format.class", "org.apache.hadoop.dynamodb.write.DynamoDBOutputFormat")
      ddbWriteConf.set("mapred.output.committer.class", "org.apache.hadoop.mapred.FileOutputCommitter")

      val schema_ddb = dataFrameRenamed.dtypes

      var ddbInsertFormattedRDD = dataFrameRenamed.rdd.map(a => {
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

      ddbInsertFormattedRDD.saveAsHadoopDataset(ddbWriteConf)
    }


    def writeDStoS3(resultDF: DataFrame, s3Bucket: String, s3Prefix: String, dbName: String, tabName: String, getYear: String, getMonth: String, getDay: String, getTimestamp: String) = {

      resultDF.write.mode("append").parquet(s3Bucket + "/"
        + s3Prefix + "/"
        + "database=" + dbName + "/"
        + "table=" + tabName + "/"
        + "year=" + getYear + "/"
        + "month=" + getMonth + "/"
        + "day=" + getDay + "/"
        + "hour=" + getTimestamp.split("-")(0) + "/"
        + "min=" + getTimestamp.split("-")(1) + "/"
      )

    }

  }


}
