stack="sam-lrs-data-pipeline"
url=$(aws cloudformation describe-stacks --stack-name $stack --query Stacks[0].Outputs[0].OutputValue --output text)
record='''{
  "sensor": "https://example.edu/sensors/1",
  "sendTime": "2020-01-15T11:05:01.000Z",
  "dataVersion": "http://purl.imsglobal.org/ctx/caliper/v1p2",
  "data": [
      {
        "@context": "http://purl.imsglobal.org/ctx/caliper/v1p2",
        "id": "urn:uuid:7e10e4f3-a0d8-4430-95bd-783ffae4d916",
        "type": "ToolUseEvent",
        "profile": "ToolUseProfile",
        "eventTime": "2020-01-15T10:15:00.000Z",
        "actor": {
          "id": "https://example.edu/users/554433",
          "type": "Person"
        },
        "action": "Used",
        "object": {
          "id": "https://example.edu",
          "type": "SoftwareApplication"
        }
      }
  ]
}'''

curl \
    -H "Content-Type: application/json" \
    -X POST \
    -d "$record" \
    $url
