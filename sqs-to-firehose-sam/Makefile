STACK_NAME := sqs-to-firehose

export STACK_NAME := ${STACK_NAME}

delete: 
	aws cloudformation delete-stack --stack-name ${STACK_NAME}
