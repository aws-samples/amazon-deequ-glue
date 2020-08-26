#!/bin/bash
nflag=false
pflag=false
rflag=false
eflag=false

DIRNAME=$(pwd)

usage () { echo "
    -h -- Opens up this help message
    -n -- Name of the CloudFormation stack
    -p -- Name of the AWS profile to use
    -r -- AWS Region to use
    -e -- Name of the environment
"; }
options=':n:p:r:e:h'
while getopts $options option
do
    case "$option" in
        n  ) nflag=true; STACK_NAME=$OPTARG;;
        p  ) pflag=true; PROFILE=$OPTARG;;
        r  ) rflag=true; REGION=$OPTARG;;
        e  ) eflag=true; ENV=$OPTARG;;
        h  ) usage; exit;;
        \? ) echo "Unknown option: -$OPTARG" >&2; exit 1;;
        :  ) echo "Missing option argument for -$OPTARG" >&2; exit 1;;
        *  ) echo "Unimplemented option: -$OPTARG" >&2; exit 1;;
    esac
done

if ! $pflag
then
    echo "-p not specified, using default..." >&2
    PROFILE="default"
fi
if ! $nflag
then
    STACK_NAME="amazon-deequ-glue"
fi
if ! $rflag
then
    echo "-r not specified, using default region..." >&2
    REGION=$(aws configure get region --profile $PROFILE)
fi
if ! $eflag
then
    echo "-e not specified, using dev..." >&2
    ENV="dev"
fi

ACCOUNT=$(aws sts get-caller-identity --query 'Account' --output text --profile $PROFILE)

echo "Creating CloudFormation Artifacts Bucket..."
S3_BUCKET=amazon-deequ-glue-$REGION-$ACCOUNT
if ! aws s3 ls $S3_BUCKET --profile $PROFILE; then
  echo "S3 bucket named $S3_BUCKET does not exist. Creating."
  aws s3 mb s3://$S3_BUCKET --region $REGION --profile $PROFILE
  aws ssm put-parameter --region $REGION --profile $PROFILE --name "/DataQuality/S3/ArtifactsBucket" --value $S3_BUCKET --type String --overwrite
  aws s3api put-bucket-encryption \
    --profile $PROFILE \
    --bucket $S3_BUCKET \
    --server-side-encryption-configuration '{"Rules": [{"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}]}'
  aws s3api put-public-access-block \
    --profile $PROFILE \
    --bucket $S3_BUCKET \
    --public-access-block-configuration "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"
else
  echo "Bucket $S3_BUCKET exists already."
fi

mkdir $DIRNAME/output
aws cloudformation package --profile $PROFILE --region $REGION --template-file $DIRNAME/template.yaml --s3-bucket $S3_BUCKET --s3-prefix deequ/templates --output-template-file $DIRNAME/output/packaged-template.yaml

echo "Checking if stack exists ..."
if ! aws cloudformation describe-stacks --profile $PROFILE  --region $REGION --stack-name $STACK_NAME; then
  echo "Creating CodeCommit Repository"
  aws codecommit create-repository --region $REGION --profile $PROFILE --repository-name amazon-deequ-glue
  git config --global credential.helper '!aws --profile '$PROFILE' codecommit credential-helper $@'
  git config --global credential.UseHttpPath true
  cd ../
  rm -rf .git
  git init
  git add .
  git commit -m "Initial Commit"
  git remote add origin https://git-codecommit.$REGION.amazonaws.com/v1/repos/amazon-deequ-glue
  git checkout -b $ENV
  git push --set-upstream origin $ENV
  cd $DIRNAME

  echo -e "Stack does not exist, creating ..."
  aws cloudformation create-stack \
    --profile $PROFILE \
    --region $REGION \
    --stack-name $STACK_NAME \
    --template-body file://$DIRNAME/output/packaged-template.yaml \
    --parameters \
      ParameterKey=pArtifactsBucket,ParameterValue=$S3_BUCKET \
      ParameterKey=pEnv,ParameterValue=$ENV \
    --tags file://$DIRNAME/tags.json \
    --capabilities "CAPABILITY_NAMED_IAM" "CAPABILITY_AUTO_EXPAND"

  echo "Waiting for stack to be created ..."
  aws cloudformation wait stack-create-complete --profile $PROFILE --region $REGION \
    --stack-name $STACK_NAME

  echo "Starting initial Amplify job ..."
  APP_ID=$(sed -e 's/^"//' -e 's/"$//' <<<"$(aws ssm get-parameter --profile $PROFILE --region $REGION --name /DataQuality/Amplify/AppID --query "Parameter.Value")")
  aws amplify start-job --profile $PROFILE --region $REGION --app-id $APP_ID --branch-name $ENV --job-type RELEASE

  until [ $(aws amplify get-job --profile $PROFILE --region $REGION --app-id $APP_ID --branch-name $ENV --job-id 1 --query 'job.summary.status' --output text) = *"RUNNING"* ];
  do
    echo "Amplify console job is running......"
    sleep 60s
    if [ $(aws amplify get-job --profile $PROFILE --region $REGION --app-id $APP_ID --branch-name $ENV --job-id 1 --query 'job.summary.status' --output text) != "RUNNING" ]; then
      echo "Amplify console job Finished"
      status=$(aws amplify get-job --profile $PROFILE --region $REGION --app-id $APP_ID --branch-name $ENV --job-id 1 --query 'job.summary.status' --output text)
      if [ "$status" == "SUCCEED" ]
      then
          echo "JOB $status"
      else
          echo "JOB $status"
          exit 1;
      fi  
      break
    fi
  done
else
  echo -e "Stack exists, attempting update ..."

  set +e
  update_output=$( aws cloudformation update-stack \
    --profile $PROFILE \
    --region $REGION \
    --stack-name $STACK_NAME \
    --template-body file://$DIRNAME/output/packaged-template.yaml \
    --parameters \
      ParameterKey=pArtifactsBucket,ParameterValue=$S3_BUCKET \
      ParameterKey=pEnv,ParameterValue=$ENV \
    --tags file://$DIRNAME/tags.json \
    --capabilities "CAPABILITY_NAMED_IAM" "CAPABILITY_AUTO_EXPAND" 2>&1)
  status=$?
  set -e

  echo "$update_output"

  if [ $status -ne 0 ] ; then
    # Don't fail for no-op update
    if [[ $update_output == *"ValidationError"* && $update_output == *"No updates"* ]] ; then
      echo -e "\nFinished create/update - no updates to be performed";
      exit 0;
    else
      exit $status
    fi
  fi

  echo "Waiting for stack update to complete ..."
  aws cloudformation wait stack-update-complete --profile $PROFILE --region $REGION \
    --stack-name $STACK_NAME 
  echo "Finished create/update successfully!"
fi

echo "Loading Deequ scripts ..."
aws s3 sync ./main/scala/deequ/ s3://$S3_BUCKET/deequ/scripts/ --profile $PROFILE
aws s3 cp ./main/resources/deequ-1.0.3-RC1.jar s3://$S3_BUCKET/deequ/jars/ --profile $PROFILE
aws s3 cp ./main/utils/deequ-controller/deequ-controller.py s3://$S3_BUCKET/deequ/scripts/ --profile $PROFILE