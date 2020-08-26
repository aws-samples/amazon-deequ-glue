## Serverless Data Quality with Amazon Deequ on AWS Glue

[Amazon Deequ](https://github.com/awslabs/deequ) is an open source library built on top of Apache Spark for defining “unit tests for data”. It is used internally at Amazon for verifying the quality of large production datasets, particularly to:

* Suggest data quality constraints on input tables/files
* Verify aforementioned suggested constraints
* Compute quality metrics
* Run column profiling

More details on Deequ can be found in this [AWS Blog](https://aws.amazon.com/blogs/big-data/test-data-quality-at-scale-with-deequ/).

A serverless data quality framework based on Amazon Deequ and running on AWS Glue is  showcased in this repository. It takes a Glue database and tables as inputs and outputs various data quality metrics into S3. Additionally, it performs an [automatic generation of constraints](https://github.com/awslabs/deequ/blob/master/src/main/scala/com/amazon/deequ/examples/constraint_suggestion_example.md) on previously unseen data. The suggestions are stored in a DynamoDB table and can be reviewed and amended at any point by data owners in a purpose-built UI. All constraints are **disabled** by default. Once enabled, they are used by the Glue jobs to carry out the data quality checks on the tables.

### Deployment
To deploy the infrastructure and code, you'll need an AWS account and a correctly [configured AWS profile](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html) with enough permissions to create the architecture below (Administrator rights are recommended).

```bash
cd ./src
./deploy.sh -p <aws_profile> -r <aws_region>
```
All arguments to the ```deploy.sh``` script are optional. The default AWS profile and region are used if none are provided. The script will:
1. Create an S3 bucket to host Amazon Deequ Scripts and Jar
2. Create a CodeCommit repository and push the local code copy to it
3. Create a CloudFormation stack named ```amazon-deequ-glue``` holding all the infrastructure listed below
4. Deploy an [AWS Amplify](https://aws.amazon.com/amplify/) UI and monitor the job
5. Upload the Amazon Deequ scripts and Jar to the S3 bucket

The initial deployment can take 10-15 minutes. The same command can be used for both creating and updating infrastructure.

### Detailed step-by-step
![Architecture](./docs/amazon-deequ-glue.png)

1. A time-based CloudWatch Event Rule created by the stack is configured to trigger the data quality step function, passing a JSON with the relevant metadata (i.e glueDatabase and glueTables). It is scheduled to fire every 30 minutes and is **disabled** by default
2. The first step in the data quality step function makes a synchronous call to the AWS Glue Controller job. This Glue job is responsible for determining which data quality checks should be performed
3. Below Glue jobs can be triggered by the Controller:
    - (Phase a) ***Suggestions Job***: is started when working on **previously unseen data** to perform automatic suggestion of constraints. The job would first recommend column-level data quality constraints inferred from a first pass on the data that it then logs in the ```DataQualitySuggestion``` DynamoDB table. It also outputs the quality checks results based on these suggestions into S3. The suggestions can be reviewed and amended at any point by data owners
    - (Phase b) ***Verification Job***: reads the user-defined and automatically suggested constraints from both the ```DataQualitySuggestion``` and ```DataQualityAnalysis``` DynamoDB tables and runs 1) a constraint verification and 2) an analysis metrics computation that it outputs in parquet format to S3
    - ***Profiler Job***: is always run regardless of the phase. It performs single-column profiling of data. It generates a profile for each column in the input data, including the completeness of the column, the approximate number of distinct values and the inferred datatype
4. Once the Controller job succeeds, the second step in the data quality step function is to trigger an AWS Lambda which calls the ```data-quality-crawler```. The metrics in the data quality S3 bucket are crawled, stored in a ```data_quality_db``` Glue catalog and are immediately available to be queried in Athena 

### Testing
We assume you have a Glue database hosting one or more tables in the same region as where you deployed this framework.

1. Navigate to Step Functions in the AWS console and select the ```data-quality-sm```. Start an execution inputting a JSON like the below:
    ```
    {
        "glueDatabase": "my_database",
        "glueTables": "table1,table2"
    }
    ```
    The data quality process described in the previous section begins and you can follow it by looking at the different Glue jobs execution runs. By the end of this process, you should see that data quality suggestions were logged in the ```DataQualitySuggestions``` DynamoDB table and Glue tables were created in the ```data_quality_db``` Glue catalog which can be queried in Athena
2. Navigate to Amplify in the AWS console, and select the ```deequ-constraints``` app. Then click on the highlighted URL (should be listed as ```https://<env>.<appsync_app_id>.amplifyapp.com```) to open the data quality constraints web app. After completing the registration process (i.e. Create Account) and signing in, a UI similar to the below should be visible:
![](docs/amazon-deequ-web-app.png)
it lists data quality suggestions produced by the Glue job in the previous step. Data owners can add/remove and enable/disable these constraints at any point in this UI. Notice how the ```Enabled``` field is set to ```N``` by default for all suggestions. This is to ensure all constraints are human-reviewed before they are processed. Click on the checkbox button to enable a constraint
3. Optionally, you can also provide data quality constraints in the ```Analyzers``` tab. These constraints are used by Deequ to calculate column-level statistics on the dataset (e.g. CountDistinct, DataType, Completeness…) called metrics (Refer to the Data Analysis section of this [blog](https://aws.amazon.com/blogs/big-data/test-data-quality-at-scale-with-deequ/) for more details). Here is an example of an analysis constraint entry:
![](docs/amazon-deequ-web-app-analyzer.png)
4. Once you have you have reviewed both suggestion and analysis constraints, start a new execution of the step function with the same JSON as an input. This time the Glue jobs use the reviewed constraints to perform the data quality checks. Once more, the results can be immediately queried in Athena

An exhaustive list of suggestion and analysis constraints can be found in the [docs](./docs/constraints/).

### Dependencies
- Amazon Deequ 1.0.3-RC1.jar
- Spark 2.2.0 (Scala)

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.

