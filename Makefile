TEMPLATE_FILE := template.yaml
STACK_NAME := sam-lrs-data-pipeline
S3_BUCKET_PACKAGE := lrs-data-pipeline-package
OUTPUT := packaged.yaml

export TEMPLATE_FILE := ${TEMPLATE_FILE}
export STACK_NAME := ${STACK_NAME}
export S3_BUCKET_PACKAGE := ${S3_BUCKET_PACKAGE}
export OUTPUT := ${OUTPUT}

.DEFAULT_GOAL := deploy
.PHONY: package deploy delete

package: 
	sam package --template-file ${TEMPLATE_FILE} \
	--s3-bucket ${S3_BUCKET_PACKAGE} \
	--output-template-file ${OUTPUT}

deploy: package
	sam deploy --template-file ${OUTPUT} \
	--stack-name ${STACK_NAME} \
	--capabilities CAPABILITY_IAM

delete: 
	aws cloudformation delete-stack --stack-name ${STACK_NAME}
	read enter
	aws cloudformation delete-stack --stack-name ${STACK_NAME}
