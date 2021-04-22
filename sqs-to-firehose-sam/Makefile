STACK_NAME := sqs-to-firehose

export STACK_NAME := ${STACK_NAME}

deploy: 
	sam deploy --guided

delete: 
	aws cloudformation delete-stack --stack-name ${STACK_NAME}

local: 
	sam local invoke -d 9999 -e events/event.json LambdaFunction

lambda-test: 
	aws lambda invoke --function-name sqs-to-firehose-LambdaFunction \
          --payload file://input.txt outputfile.txt

