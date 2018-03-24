""" Process a DynamoDB stream Event NEW/OLD

Comfigure the DynamoDB stream to use this Lambda Function using AWS Console.
No additional packages or permissions.
"""
_author__ = "Abhishek Dujari"

import boto3
import logging

#Cloudwatch
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
#Init only, no client required
boto3.resource('dynamodb')
#Used for converting the Event data to Dict
deserialize = boto3.dynamodb.types.TypeDeserializer()


def handler(event, context):
    for record in event['Records']:
        logger.debug(record['eventID'])
        logger.debug(record['eventName'])
        if record['eventName'] in ['INSERT', 'MODIFY']: #Event REMOVE has only 'OldImage'
            row = record['dynamodb']['NewImage']
            logger.debug(row)
            data = {k: deserialize.deserialize(v) for k, v in row.items()}
            logger.debug(data)
    logger.debug('Successfully processed {} records.'.format(str(len(event['Records']))))
