bucket="lrs-data-pipeline-package"
template="template.yaml"
output="packaged.yaml"
stack="sam-lrs-data-pipeline"

sam package \
    --template-file $template \
    --s3-bucket $bucket \
    --output-template-file $output

sam deploy \
    --template-file $output \
    --stack-name $stack \
    --capabilities CAPABILITY_IAM
