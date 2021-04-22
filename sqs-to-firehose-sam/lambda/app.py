import boto3
import traceback
import json
import os
import uuid

firehose_client = boto3.client('firehose')


def extract_data(record):
    '''
    Description:
    This function performs the data extraction from the record
    dictionary
    :param record: [type: dict] corresponds to a message from a list
     of messages read by lambda
    :return data: [type: str] extracted data
    '''
    # body = json.loads(record['body'])
    body = record['body']
    # data = body['Message']
    # return data
    return body


def add_record(record, kinesis_records_all):
    '''
    Description:
    This function extracts the required data from the record
    dictionary and adds it to the list of records to be pushed to
    the respective mapped Kinesis Data Stream based on the
    eventSourceARN.
    If data extraction throws exception then the whole record
    dictionary is dumped as a string to the "non_conformed" key in
    kinesis_records_all; eventually to be dumped in DLQStream.
    :param record: [type: dict] corresponds to a message from a
     list of messages read by lambda

    :param kinesis_records_all: [type: dict] contains sqs queue
     arns as keys mapped to a dict containing list of records
     to be pushed and the name of the destination Kinesis Data Stream
    :return kinesis_records_all: [type: dict] updated dict
     kinesis_records_all
    '''
    # creating record_dict dictionary
    record_dict = {}
    try:
        # source arn
        sqs_source_arn = record['eventSourceARN']

        # Retrieving required data from record for 'Data' key.
        # Value for 'Data' key can be as per your requirement.
        data = extract_data(record)
        record_dict['Data'] = data
        record_dict['PartitionKey'] = str(uuid.uuid1())

        # get sqs name
        sqs_name = sqs_source_arn.split(":")[-1]

        # appending to respective sqs_key in
        # kinesis_records_all dictionary
        kinesis_records_all[sqs_source_arn]['Records']\
            .append(record_dict)

        # get eventid
        # eventId = json.loads(data)["eventId"]
        print(f'type of data: {type(data)}')
        sensor = json.loads(data)["sensor"]
        print(f"Successfully processed sensor : {sensor} "
              f"from SQS : {sqs_name}")
    except Exception as e:
        # dumping the whole record as is to non_conformed list
        record_dict['Data'] = json.dumps(record)
        record_dict['PartitionKey'] = str(uuid.uuid1())

        # log exceptions
        message = '\n'.join([str(e), str(traceback.print_exc())])
        # message = "Something is wrong"
        print(message)
    finally:
        return kinesis_records_all


def push_to_kinesis(source, source_dict):
    '''
    Description:
    This function pushes the list of extracted records from a
    queue / non_conformed records to a particular Kinesis Data Stream

    :param source: [type: str] SQS queue ARN
    :param source_dict: [type: dict] contains a list of records and
     the stream name where the records are to be pushed to
    '''
    if source_dict['Records']:
        for record in source_dict['Records']:
            print(f"recore data type: {type(record['Data'])}")
            response = firehose_client.put_record(
                DeliveryStreamName=source_dict['StreamName'],
                Record={
                    # 'Data': json.dumps(record['Data'])
                    'Data': record['Data']
                }
            )
            records_pushed_message = ''
            if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
                pushed_count = ' '.join(['Pushed',
                                         str(len(source_dict['Records'])), 'records'])
                from_details = ' '.join(['from', source])
                to_details = ' '.join(['to',
                                       source_dict['StreamName'], 'stream'])
                records_pushed_message = ' '.join([pushed_count, to_details]) \
                    if source == 'non_conformed' else ' '.\
                    join([pushed_count, from_details, to_details])
                message = ' '.join(['\nResponse received from',
                                    source_dict['StreamName'],
                                    'with HTTPStatusCode',
                                    str(response["ResponseMetadata"]
                                        ["HTTPStatusCode"]),
                                    ".", records_pushed_message])
                print(message)


def lambda_handler(event, context):
    kinesis_records_all = {}
    print(os.environ['QUEUE_NAME'])
    sqs_to_kinesis_mapping = {
        os.environ['QUEUE_NAME']:
        os.environ['FIREHOSE_DELIVERY_STREAM_NAME']
    }
    for sqs_key in sqs_to_kinesis_mapping.keys():
        kinesis_records_all[sqs_key] = {'Records': [],
                                        'StreamName': sqs_to_kinesis_mapping[sqs_key]}

    for record in event['Records']:
        kinesis_records_all = add_record(record, kinesis_records_all)

    # pushing records retrieved from each sqs source to respective
    # streams & non-conformed records to DLQ Stream
    for source in kinesis_records_all.keys():
        push_to_kinesis(source, kinesis_records_all[source])
