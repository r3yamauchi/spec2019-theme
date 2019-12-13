import json
import os
import uuid
from datetime import datetime

import boto3
import botocore
import requests
import logging
import traceback
from boto3.dynamodb.conditions import Key, Attr

ddb = boto3.resource('dynamodb')
user_table = ddb.Table(os.environ['USER_TABLE'])
history_table = ddb.Table(os.environ['PAYMENT_HISTORY_TABLE'])
sqs = boto3.client('sqs')
client = boto3.client('dynamodb', region_name='ap-northeast-1')

logger = logging.getLogger('spec')
loglevel = logging.DEBUG
logger.setLevel(loglevel)

locations = requests.get(os.environ['LOCATION_ENDPOINT']).json()
logger.debug("locations: {}".format(locations))


def user_create(event, context):
    body = json.loads(event['body'])
    logger.debug("body: {}".format(body))

    user_table.put_item(Item={'id': body['id'], 'name': body['name']})
    return {'statusCode': 200, 'body': json.dumps({'result': 'ok'})}


def wallet_charge(event, context):
    body = json.loads(event['body'])
    logger.debug("body: {}".format(body))

    response = user_table.update_item(
        Key={'id': body['userId']},
        UpdateExpression='ADD amount :chargeAmount',
        ExpressionAttributeValues={':chargeAmount': body['chargeAmount']},
        ReturnValues='ALL_NEW')

    history_table.put_item(
        Item={
            'userId': body['userId'],
            'transactionId': body['transactionId'],
            'chargeAmount': body['chargeAmount'],
            'locationId': body['locationId'],
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

    sqs.send_message(QueueUrl=os.environ['NOTIFICATION_QUEUE'],
                     MessageBody=json.dumps({
                         'transactionId':
                         body['transactionId'],
                         'userId':
                         body['userId'],
                         'chargeAmount':
                         body['chargeAmount'],
                         'totalAmount':
                         int(response['Attributes']['amount'])
                     }))

    return {
        'statusCode':
        202,
        'body':
        json.dumps({'result': 'Assepted. Please wait for the notification.'})
    }


def wallet_use(event, context):
    body = json.loads(event['body'])
    logger.debug("body: {}".format(body))

    try:
        response = user_table.update_item(
            Key={'id': body['userId']},
            UpdateExpression='ADD amount :useAmount',
            ExpressionAttributeValues={':useAmount': body['useAmount'] * -1},
            ConditionExpression=Attr('amount').gte(
                body['useAmount']),
            ReturnValues='ALL_NEW')
    except botocore.exceptions.ClientError as e:
        logger.debug("wallet_use error: {}".format(e.response))
        logger.debug("e.response['Error']['Code']: {}".format(
            e.response['Error']['Code']))
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            return {
                'statusCode': 400,
                'body':
                json.dumps({'errorMessage': 'There was not enough money.'})
            }
        else:
            return {
                'statusCode': 400,
                'body':
                json.dumps({'errorMessage': e.response['Error']['Code']})
            }

    history_table.put_item(
        Item={
            'userId': body['userId'],
            'transactionId': body['transactionId'],
            'useAmount': body['useAmount'],
            'locationId': body['locationId'],
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

    sqs.send_message(QueueUrl=os.environ['NOTIFICATION_QUEUE'],
                     MessageBody=json.dumps({
                         'transactionId':
                         body['transactionId'],
                         'userId':
                         body['userId'],
                         'useAmount':
                         body['useAmount'],
                         'totalAmount':
                         int(response['Attributes']['amount'])
                     }))

    return {
        'statusCode':
        202,
        'body':
        json.dumps({'result': 'Assepted. Please wait for the notification.'})
    }


def wallet_transfer(event, context):
    body = json.loads(event['body'])
    logger.debug("body: {}".format(body))

    try:
        client.transact_write_items(TransactItems=[{
            'Update': {
                'TableName': os.environ['USER_TABLE'],
                'Key': {
                    'id': {
                        'S': body['fromUserId']
                    }
                },
                'ConditionExpression': '#amount >= :tm',
                'UpdateExpression': 'ADD #amount :am',
                'ExpressionAttributeNames': {
                    '#amount': 'amount'
                },
                'ExpressionAttributeValues': {
                    ':tm': {
                        'N': str(body['transferAmount'])
                    },
                    ':am': {
                        'N': str(-body['transferAmount'])
                    },
                }
            }
        }, {
            'Update': {
                'TableName': os.environ['USER_TABLE'],
                'Key': {
                    'id': {
                        'S': body['toUserId']
                    }
                },
                'UpdateExpression': 'ADD #amount :am',
                'ExpressionAttributeNames': {
                    '#amount': 'amount'
                },
                'ExpressionAttributeValues': {
                    ':am': {
                        'N': str(body['transferAmount'])
                    },
                }
            }
        }])

    except botocore.exceptions.ClientError as e:
        logger.debug("transaction error: {}".format(e.response))
        logger.debug("e.response['Error']['Code']: {}".format(
            e.response['Error']['Code']))
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            return {
                'statusCode': 400,
                'body':
                json.dumps({'errorMessage': 'There was not enough money.'})
            }
        else:
            return {
                'statusCode': 400,
                'body':
                json.dumps({'errorMessage': e.response['Error']['Code']})
            }

    history_table.put_item(
        Item={
            'userId': body['fromUserId'],
            'transactionId': body['transactionId'],
            'useAmount': body['transferAmount'],
            'locationId': body['locationId'],
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
    history_table.put_item(
        Item={
            'userId': body['toUserId'],
            'transactionId': body['transactionId'],
            'chargeAmount': body['transferAmount'],
            'locationId': body['locationId'],
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

    sqs.send_message(QueueUrl=os.environ['NOTIFICATION_QUEUE'],
                     MessageBody=json.dumps({
                         'transactionId':
                         body['transactionId'],
                         'userId':
                         body['fromUserId'],
                         'useAmount':
                         body['transferAmount'],
                         'totalAmount':
                         0,
                         'transferTo':
                         body['toUserId']
                     }))

    sqs.send_message(QueueUrl=os.environ['NOTIFICATION_QUEUE'],
                     MessageBody=json.dumps({
                         'transactionId':
                         body['transactionId'],
                         'userId':
                         body['toUserId'],
                         'chargeAmount':
                         body['transferAmount'],
                         'totalAmount':
                         0,
                         'transferFrom':
                         body['fromUserId']
                     }))

    return {
        'statusCode':
        202,
        'body':
        json.dumps({'result': 'Assepted. Please wait for the notification.'})
    }


def get_user_summary(event, context):
    params = event['pathParameters']
    logger.debug("params: {}".format(params))

    user = user_table.get_item(Key={'id': params['userId']})

    payment_history = history_table.query(
        KeyConditions={
            'userId': {
                'AttributeValueList': [params['userId']],
                'ComparisonOperator': 'EQ'
            }
        })

    sum_charge = 0
    sum_payment = 0
    times_per_location = {}
    for item in payment_history['Items']:
        sum_charge += item.get('chargeAmount', 0)
        sum_payment += item.get('useAmount', 0)
        location_name = _get_location_name(item['locationId'])
        if location_name not in times_per_location:
            times_per_location[location_name] = 1
        else:
            times_per_location[location_name] += 1

    return {
        'statusCode':
        200,
        'body':
        json.dumps({
            'userName': user['Item']['name'],
            'currentAmount': int(user['Item']['amount']),
            'totalChargeAmount': int(sum_charge),
            'totalUseAmount': int(sum_payment),
            'timesPerLocation': times_per_location
        })
    }


def get_payment_history(event, context):
    params = event['pathParameters']
    logger.debug("params: {}".format(params))

    payment_history_result = history_table.query(
        KeyConditions={
            'userId': {
                'AttributeValueList': [params['userId']],
                'ComparisonOperator': 'EQ'
            }
        },
        IndexName='timestampIndex',
        ScanIndexForward=False
    )

    payment_history = []
    for p in payment_history_result['Items']:
        if 'chargeAmount' in p:
            p['chargeAmount'] = int(p['chargeAmount'])
        if 'useAmount' in p:
            p['useAmount'] = int(p['useAmount'])
        p['locationName'] = _get_location_name(p['locationId'])
        del p['locationId']
        payment_history.append(p)

    return {'statusCode': 200, 'body': json.dumps(payment_history)}


def send_notification(event, context):
    for record in event['Records']:
        requests.post(os.environ['NOTIFICATION_ENDPOINT'],
                      json=json.loads(record['body']))


def _get_location_name(location_id):
    logger.debug("location_id: {}".format(location_id))
    return locations[str(location_id)] if str(location_id) in locations else 'unknown'
