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
import org.apache.spark.sql.functions.split
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import scala.util.matching.Regex
import java.util.HashMap

import java.time.LocalDate
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.text.SimpleDateFormat
import java.util.Date

import com.amazonaws.auth.BasicAWSCredentials
import java.io.File
import java.io.PrintWriter

import GlueApp.getTimestamp
import com.amazonaws.services.dynamodbv2.model.AttributeValue
import org.apache.hadoop.io.Text
import org.apache.hadoop.dynamodb.DynamoDBItemWritable
import org.apache.hadoop.dynamodb.read.DynamoDBInputFormat
import org.apache.hadoop.dynamodb.write.DynamoDBOutputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.io.LongWritable

import com.amazonaws.services.dynamodbv2.{AmazonDynamoDB, AmazonDynamoDBClientBuilder}
import com.amazonaws.services.dynamodbv2.model.{AttributeDefinition, GlobalSecondaryIndex}
import com.amazonaws.services.dynamodbv2.document.{DynamoDB, Index, Item, ItemCollection, QueryOutcome, Table}
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap

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

import scala.reflect.runtime.currentMirror
import scala.tools.reflect.ToolBox
import java.io._
import java.util.ArrayList
import java.util.Iterator

object GlueApp {

  val sparkContext: SparkContext = new SparkContext()
  val glueContext: GlueContext = new GlueContext(sparkContext)
  val spark = glueContext.getSparkSession
  val sqlContext = new SQLContext(sparkContext)
  val toolbox = currentMirror.mkToolBox()

  val getYear = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy"))
  val getMonth = LocalDate.now().format(DateTimeFormatter.ofPattern("MM"))
  val getDay = LocalDate.now().format(DateTimeFormatter.ofPattern("dd"))
  val getTimestamp = new SimpleDateFormat("HH-mm-ss").format(new Date)
  import spark.implicits._

  def main(sysArgs: Array[String]) {

    //***********************************************************************//
    // Step1: Create Glue Context and extract Args
    //***********************************************************************//
    val args = GlueArgParser.getResolvedOptions(sysArgs,
      Seq("JOB_NAME",
        "dynamodbSuggestionTableName",
        "dynamodbAnalysisTableName",
        "glueDatabase",
        "glueTables",
        "targetBucketName").toArray)

    Job.init(args("JOB_NAME"), glueContext, args.asJava)

    val dynamodbSuggestionTableName = args("dynamodbSuggestionTableName")
    val dynamodbAnalysisTableName = args("dynamodbAnalysisTableName")
    val dbName = args("glueDatabase")
    val tabNames = args("glueTables").split(",").map(_.trim)
    // Empty dataframes required for successful job compilation
    var suggestionCheckDF: DataFrame = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], StructType(Seq()))
    var analysisCheckDF: DataFrame = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], StructType(Seq()))
    // Empty dataframe required for successful job compilation
    var verificationDataFrame: Seq[DataFrame] = Seq(spark.createDataFrame(spark.sparkContext.emptyRDD[Row], StructType(Seq())))
    var analysisDataFrame: Seq[DataFrame] = Seq(spark.createDataFrame(spark.sparkContext.emptyRDD[Row], StructType(Seq())))

    for (tabName <- tabNames) {
      //***********************************************************************//
      // Step2: Extracting suggestions from DynamoDB using input GLUE table
      //***********************************************************************//
      val suggestionConstraintFullDF: DataFrame = extractSuggestionsFromDynamo(dynamodbSuggestionTableName, dbName, tabName)
      val analysisConstraintFullDF: DataFrame = extractSuggestionsFromDynamo(dynamodbAnalysisTableName, dbName, tabName)
      //***********************************************************************//
      // Step3: Create Dataframe from GLUE tables to run the Verification result
      //***********************************************************************//
      val glueTableDF: DataFrame = readGlueTablesToDF(dbName, tabName)

      //***********************************************************************//
      // Step4: Build validation code dataframe
      //***********************************************************************//
      if (!(suggestionConstraintFullDF.isEmpty)) {
        suggestionCheckDF = buildSuggestionCheckConstraints(suggestionConstraintFullDF, dbName, tabName)
      }
      if (!(analysisConstraintFullDF.isEmpty)) {
        analysisCheckDF = buildAnalysisCheckConstraints(analysisConstraintFullDF, dbName, tabName)
      }

      //***********************************************************************//
      // Step5: Create Check class with scala constraints from Dynamo
      // Step6: Execute Verification Runner and Analysis
      //***********************************************************************//
      if (!(suggestionCheckDF.isEmpty)) {
        verificationDataFrame = createCheckClass(suggestionCheckDF, glueTableDF)
      }
      if (!(analysisCheckDF.isEmpty)) {
        analysisDataFrame = createAnalyzersClass(analysisCheckDF, glueTableDF)
      }

      //***********************************************************************//
      // Step7: Write result dataframe to S3 bucket
      //***********************************************************************//
      if (!(suggestionCheckDF.isEmpty)) {
        verificationDataFrame.foreach{
          resultDF => writeDStoS3(resultDF, args("targetBucketName"), "constraints-verification-results", dbName, tabName, getYear, getMonth, getDay, getTimestamp)
        }
      }
      if (!(analysisCheckDF.isEmpty)) {
        analysisDataFrame.foreach{
          resultDF => writeDStoS3(resultDF, args("targetBucketName"), "constraints-analysis-results", dbName, tabName, getYear, getMonth, getDay, getTimestamp)
        }
      }
    }

    Job.commit()
  }

  /***
   * Step2: Extracting suggestions from DynamoDB using input GLUE table
   * @param dynoTable
   * @param dbName
   * @param table
   * @return
   */
  def extractSuggestionsFromDynamo(dynoTable: String, dbName: String, table: String): DataFrame = {

    val client = AmazonDynamoDBClientBuilder.standard().build()
    val dynamoDB: DynamoDB = new DynamoDB(client)
    val tableSuggestions: Table = dynamoDB.getTable(dynoTable)
    val index: Index = tableSuggestions.getIndex("table-index")

    val querySpec: QuerySpec = new QuerySpec()
    querySpec.withKeyConditionExpression("tableHashKey = :v_table").withValueMap(new ValueMap().withString(":v_table", dbName + "-" + table))

    val items = index.query(querySpec)
    val iterator = items.iterator()

    var listSuggestions = List.empty[String]
    while (iterator.hasNext()) {
        listSuggestions = listSuggestions :+ iterator.next().toJSON()
    }

    val rddDynamoSuggestions = spark.sparkContext.parallelize(listSuggestions)
    val dfDynamoSuggestions = spark.read.json(rddDynamoSuggestions)
    return dfDynamoSuggestions
  }

  /***
   * Step3: Create Dataframe from GLUE tables to run the Verification result
   * @param glueDB
   * @param glueTable
   * @return
   */
  def readGlueTablesToDF(glueDB: String, glueTable: String): DataFrame = {

    glueContext.getCatalogSource(database = glueDB,
      tableName = glueTable,
      redshiftTmpDir = "",
      transformationContext = "datasource0")
      .getDynamicFrame().toDF()

  }


  /***
   * Step4: Build validation code dataframe
   * @param constraintSuggestions
   * @param glueDB
   * @param glueTable
   * @return
   */
  def buildSuggestionCheckConstraints(constraintSuggestions: DataFrame, glueDB: String, glueTable: String): DataFrame = {
    constraintSuggestions.createOrReplaceTempView("constraintSuggestions")
    sqlContext.sql(
      s"""
         |select
         |concat_ws('', collect_list(constraintCode)) as combinedValidationCode
         |from constraintSuggestions
         |where enable = 'Y'
         |""".stripMargin
    )

  }

  def buildAnalysisCheckConstraints(constraintAnalysis: DataFrame, glueDB: String, glueTable: String): DataFrame = {
    constraintAnalysis.createOrReplaceTempView("constraintAnalysis")
    sqlContext.sql(
      s"""
         |select
         |concat_ws(' :: ', collect_list(analyzerCode)) as combinedAnalyzerCode
         |from constraintAnalysis
         |where enable = 'Y'
         |""".stripMargin
    )

  }


  /***
   * Step5: Create Check class with scala constraints from Dynamo
   * Step6: Exeucte Verification Runner
   * @param checksDF
   * @param dataDF
   * @return
   */
  def createCheckClass(checksDF: DataFrame, dataDF: DataFrame): Seq[DataFrame] = {

    checksDF.collect.map { row =>

      var checkValidationCode =
        "_root_.com.amazon.deequ.checks.Check(_root_.com.amazon.deequ.checks.CheckLevel.Error, \"Review Check\")" + row
          .mkString("@")
          .split("@")(0)
      checkValidationCode = checkValidationCode.replace(
        "ConstrainableDataTypes",
        "_root_.com.amazon.deequ.constraints.ConstrainableDataTypes"
      )

      val _checks =
        toolbox
          .eval(toolbox.parse(checkValidationCode))
          .asInstanceOf[Check]

      val verificationResult: VerificationResult = {
        VerificationSuite()
          .onData(dataDF)
          .addCheck(_checks)
          .run()
      }

      checkResultsAsDataFrame(spark, verificationResult)
    }

  }

  /***
   *
   * @param analyDF
   * @param dataDF
   * @return
   */
  def createAnalyzersClass(analyDF: DataFrame, dataDF: DataFrame): Seq[DataFrame] = {

    analyDF.collect.map { row =>
      val analyzerCol = row.mkString("@").split("@")(0)
      val source = s"""
                      |import com.amazon.deequ.analyzers._
                      |${analyzerCol} :: Nil
                      |""".stripMargin

      val dynoAnalyzers = toolbox.eval(toolbox.parse(source)).asInstanceOf[Seq[com.amazon.deequ.analyzers.Analyzer[_, com.amazon.deequ.metrics.Metric[_]]]]

      val analysisResult = {
        AnalysisRunner
          .onData(dataDF)
          .addAnalyzers(dynoAnalyzers)
          .run()
      }
      successMetricsAsDataFrame(spark, analysisResult)
    }
  }

  /***
   * Write results data set to S3
   * @param resultDF
   * @param s3Bucket
   * @param s3Prefix
   * @param dbName
   * @param tabName
   * @return
   */
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