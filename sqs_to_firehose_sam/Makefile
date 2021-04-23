STACK_NAME := sqs-to-firehose
SQS_QUEUE := https://sqs.us-east-2.amazonaws.com/430229884637/sqs-to-firehose-StreamQueue-1CGY2CJNCP4WL

export STACK_NAME := ${STACK_NAME}
export SQS_QUEUE := ${SQS_QUEUE}

deploy: 
	sam deploy --guided

delete: 
	aws cloudformation delete-stack --stack-name ${STACK_NAME}

local: 
	sam local invoke -d 9999 -e events/event.json LambdaFunction

lambda-test: 
	aws lambda invoke --function-name sqs-to-firehose-LambdaFunction \
          --payload file://input.txt outputfile.txt

sqs-test: 
	aws sqs send-message --queue-url ${SQS_QUEUE} --message-body file://message.json --delay-seconds 10
