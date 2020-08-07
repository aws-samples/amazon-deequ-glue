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

import org.slf4j.LoggerFactory
import java.time.LocalDate
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
  val col_extractValue = udf(extractValue)
  import spark.implicits._

  def main(sysArgs: Array[String]) {

    //***********************************************************************//
    // Step1: Create Glue Context and extract Args
    //***********************************************************************//

    val args = GlueArgParser.getResolvedOptions(sysArgs,
      Seq("JOB_NAME",
        "dynoInputAnalysisTable",
        "glueDatabase",
        "glueTable",
        "targetBucketName",
        "targetBucketPrefix").toArray)

    Job.init(args("JOB_NAME"), glueContext, args.asJava)
    val logger = LoggerFactory.getLogger(args("JOB_NAME"))

    logger.info("Start Job")

    //***********************************************************************//
    // Step2: Extracting analysis constraints from DynamoDB using input GLUE table
    //***********************************************************************//
    val suggestionConstraintFullDF: DataFrame = extractInputAnalyzersFromDynamo(args("dynoInputAnalysisTable"))

    //***********************************************************************//
    // Step3: Create Dataframe from GLUE tables to run the Verification result
    //***********************************************************************//
    val glueTableDF: DataFrame = readGlueTablesToDF(args("glueDatabase"), args("glueTable"))

    //***********************************************************************//
    // Step4: Build validation code dataframe
    //***********************************************************************//
    val checkDF: DataFrame = getAnalyzerConstraints(suggestionConstraintFullDF, args("glueDatabase"), args("glueTable"))

    //***********************************************************************//
    // Step5: Create Analyzer class with scala constraints from Dynamo
    // Step6: Exeucte Analysis Runner
    //***********************************************************************//
    val resultDataFrame: Seq[DataFrame] = createAnalyzersClass(checkDF, glueTableDF)

    //***********************************************************************//
    // Step7: Write result dataframe to S3 bucket
    //***********************************************************************//
    resultDataFrame.foreach{
      resultDF => writeDStoS3(resultDF, args("glueDatabase"), args("glueTable"), args("targetBucketName"), args("targetBucketPrefix"))
    }

    Job.commit()

    logger.info("Stop Job")
  }

  /***
   * Step2: Extarcting suggestions from DynamoDB using input GLUE table
   * @param dynoTable
   * @return
   */
  def extractInputAnalyzersFromDynamo(dynoTable: String): DataFrame = {

    var jobConf_add = new JobConf(spark.sparkContext.hadoopConfiguration)
    jobConf_add.set("dynamodb.input.tableName", dynoTable)
    jobConf_add.set("mapred.output.format.class", "org.apache.hadoop.dynamodb.write.DynamoDBOutputFormat")
    jobConf_add.set("mapred.input.format.class", "org.apache.hadoop.dynamodb.read.DynamoDBInputFormat")

    var hadooprdd_add = sparkContext.hadoopRDD(jobConf_add, classOf[DynamoDBInputFormat], classOf[Text], classOf[DynamoDBItemWritable])

    val rdd_add: RDD[(String, String, String, String, String, String)] = hadooprdd_add.map {
      case (text, dbwritable) => (dbwritable.getItem().get("analyzer_hash_key").toString(),
        dbwritable.getItem().get("database").toString(),
        dbwritable.getItem().get("tablename").toString(),
        dbwritable.getItem().get("column").toString(),
        dbwritable.getItem().get("analyzer_code").toString(),
        dbwritable.getItem().get("enable").toString())
    }

    rdd_add.toDF.withColumn("analyzer_hash_key", col_extractValue($"_1"))
      .withColumn("database", col_extractValue($"_2"))
      .withColumn("tablename", col_extractValue($"_3"))
      .withColumn("column", col_extractValue($"_4"))
      .withColumn("analyzer_code", col_extractValue($"_5"))
      .withColumn("enable", col_extractValue($"_6"))
      .select("analyzer_hash_key", "database", "tablename", "column", "analyzer_code", "enable")

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
   *
   * @param constraintSuggestions
   * @param glueDB
   * @param glueTable
   * @return
   */
  def getAnalyzerConstraints(constraintSuggestions: DataFrame, glueDB: String, glueTable: String): DataFrame = {
    constraintSuggestions.createOrReplaceTempView("analyzers")
    sqlContext.sql(
      s"""
         |select
         |database,
         |tablename,
         |concat_ws(' :: ', collect_list(analyzer_code)) as combined_analyzer_code
         |from analyzers
         |where enable = 'Y'
         |and database = '$glueDB'
         |and tablename = '$glueTable'
         |group by database, tablename
         |""".stripMargin
    )

  }


  /***
   *
   * @param analyDF
   * @param dataDF
   * @return
   */
  def createAnalyzersClass(analyDF: DataFrame, dataDF: DataFrame): Seq[DataFrame] = {

    analyDF.collect.map { row =>
      val analyzerCol = row.mkString("@").split("@")(2)
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