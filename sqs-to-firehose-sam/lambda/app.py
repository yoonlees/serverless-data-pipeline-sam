import json
import boto3
import os
from datetime import datetime

def lambda_handler(event, context):
    
    now = datetime.now()
    timestamp = now.strftime("%Y-%m-%d.%H:%M:%S")
    
    stats = {}
    positive_uins = []
    handled_messages = []
    
    # gets keys 
   
    sqs = boto3.resource("sqs")
    queue_name = os.environ.get('QUEUE_NAME')
    print (f'queue_name is: {queue_name}')
    queue = sqs.get_queue_by_name(QueueName=queue_name)
    
    s3_obj_prefix = os.environ.get('S3_OBJ_PREFIX')
    s3_bucket = os.environ.get('S3_BUCKET')
    
    found_messages = True
    while len(handled_messages) < 1000 and found_messages:
        found_messages = False
        for message in queue.receive_messages(MaxNumberOfMessages=10, VisibilityTimeout=30):
            found_messages = True
            data = json.loads(message.body)
            payload = json.loads(data.get('Message','{}'))
            positive_uins.append(payload.get('correlation_id'))
            handled_messages.append(message)
        
    print(f'Handled {len(handled_messages)} messages')   
    print(f'Positive UINs: {positive_uins}')
    
    if len(positive_uins):
        file_contents = '\n'.join(positive_uins) + '\n'
        s3 = boto3.resource('s3')
        obj = s3.Object(s3_bucket,f'{s3_obj_prefix}/positives_{timestamp}.csv')
        obj.put(Body=file_contents)
    
    for message in handled_messages:
        message.delete()
    
    stats['positive_uins'] = len(positive_uins)
    stats['lambda_timestamp'] = timestamp
    
    # Poll messages from SQS
    
    print(json.dumps({ 'statusCode': 200, 'body': stats }))
    
    return 0
