import json
import os
import uuid
from datetime import datetime

import boto3
import requests
import logging
import traceback

ddb = boto3.resource('dynamodb')
user_table = ddb.Table(os.environ['USER_TABLE'])
wallet_table = ddb.Table(os.environ['WALLET_TABLE'])
history_table = ddb.Table(os.environ['PAYMENT_HISTORY_TABLE'])

logger = logging.getLogger('spec')
loglevel = logging.DEBUG
logger.setLevel(loglevel)


def user_create(event, context):
    body = json.loads(event['body'])
    logger.debug("body: {}".format(body))

    user_table.put_item(
        Item={
            'id': body['id'],
            'name': body['name']
        }
    )
    wallet_table.put_item(
        Item={
            'id': str(uuid.uuid4()),
            'userId': body['id'],
            'amount': 0
        }
    )
    return {
        'statusCode': 200,
        'body': json.dumps({'result': 'ok'})
    }


def wallet_charge(event, context):
    body = json.loads(event['body'])
    logger.debug("body: {}".format(body))

    result = wallet_table.query({
        'KeyConditionExpression': Key('userId').eq(body['fromUserId']),
        'IndexName': 'WalletGSI'
    })
    user_wallet = result['Items'].pop()
    logger.debug("user_wallet: {}".format(user_wallet))

    logger.debug("user_wallet amount: {}, chargeAmount: {}".format(user_wallet['amount'], body['chargeAmount']))
    user_wallet['amount'] = 0 if user_wallet['amount'] is None else user_wallet['amount']
    body['chargeAmount'] = 0 if body['chargeAmount'] is None else body['chargeAmount']

    total_amount = user_wallet['amount'] + body['chargeAmount']
    wallet_table.update_item(
        Key={
            'id': user_wallet['id']
        },
        AttributeUpdates={
            'amount': {
                'Value': total_amount,
                'Action': 'PUT'
            }
        }
    )
    history_table.put_item(
        Item={
            'walletId': user_wallet['id'],
            'transactionId': body['transactionId'],
            'chargeAmount': body['chargeAmount'],
            'locationId': body['locationId'],
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
    )
    requests.post(os.environ['NOTIFICATION_ENDPOINT'], json={
        'transactionId': body['transactionId'],
        'userId': body['userId'],
        'chargeAmount': body['chargeAmount'],
        'totalAmount': int(total_amount)
    })

    return {
        'statusCode': 202,
        'body': json.dumps({'result': 'Assepted. Please wait for the notification.'})
    }


def wallet_use(event, context):
    body = json.loads(event['body'])
    logger.debug("body: {}".format(body))

    result = wallet_table.query({
        'KeyConditionExpression': Key('userId').eq(body['fromUserId']),
        'IndexName': 'WalletGSI'
    })
    user_wallet = result['Items'].pop()
    total_amount = user_wallet['amount'] - body['useAmount']
    if total_amount < 0:
        return {
            'statusCode': 400,
            'body': json.dumps({'errorMessage': 'There was not enough money.'})
        }

    wallet_table.update_item(
        Key={
            'id': user_wallet['id']
        },
        AttributeUpdates={
            'amount': {
                'Value': total_amount,
                'Action': 'PUT'
            }
        }
    )
    history_table.put_item(
        Item={
            'walletId': user_wallet['id'],
            'transactionId': body['transactionId'],
            'useAmount': body['useAmount'],
            'locationId': body['locationId'],
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
    )
    requests.post(os.environ['NOTIFICATION_ENDPOINT'], json={
        'transactionId': body['transactionId'],
        'userId': body['userId'],
        'useAmount': body['useAmount'],
        'totalAmount': int(total_amount)
    })

    return {
        'statusCode': 202,
        'body': json.dumps({'result': 'Assepted. Please wait for the notification.'})
    }


def wallet_transfer(event, context):
    body = json.loads(event['body'])
    logger.debug("body: {}".format(body))
    
    from_wallet = wallet_table.query({
        'KeyConditionExpression': Key('userId').eq(body['fromUserId']),
        'IndexName': 'WalletGSI'
    }).get('Items').pop()
    to_wallet = wallet_table.query({
        'KeyConditionExpression': Key('userId').eq(body['toUserId']),
        'IndexName': 'WalletGSI'
    }).get('Items').pop()

    from_total_amount = from_wallet['amount'] - body['transferAmount']
    to_total_amount = from_wallet['amount'] + body['transferAmount']
    if from_total_amount < 0:
        return {
            'statusCode': 400,
            'body': json.dumps({'errorMessage': 'There was not enough money.'})
        }

    wallet_table.update_item(
        Key={
            'id': from_wallet['id']
        },
        AttributeUpdates={
            'amount': {
                'Value': from_total_amount,
                'Action': 'PUT'
            }
        }
    )
    wallet_table.update_item(
        Key={
            'id': to_wallet['id']
        },
        AttributeUpdates={
            'amount': {
                'Value': to_total_amount,
                'Action': 'PUT'
            }
        }
    )
    history_table.put_item(
        Item={
            'walletId': from_wallet['id'],
            'transactionId': body['transactionId'],
            'useAmount': body['transferAmount'],
            'locationId': body['locationId'],
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
    )
    history_table.put_item(
        Item={
            'walletId': from_wallet['id'],
            'transactionId': body['transactionId'],
            'chargeAmount': body['transferAmount'],
            'locationId': body['locationId'],
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
    )
    requests.post(os.environ['NOTIFICATION_ENDPOINT'], json={
        'transactionId': body['transactionId'],
        'userId': body['fromUserId'],
        'useAmount': body['transferAmount'],
        'totalAmount': int(from_total_amount),
        'transferTo': body['toUserId']
    })
    requests.post(os.environ['NOTIFICATION_ENDPOINT'], json={
        'transactionId': body['transactionId'],
        'userId': body['toUserId'],
        'chargeAmount': body['transferAmount'],
        'totalAmount': int(to_total_amount),
        'transferFrom': body['fromUserId']
    })

    return {
        'statusCode': 202,
        'body': json.dumps({'result': 'Assepted. Please wait for the notification.'})
    }


def get_user_summary(event, context):
    logger.debug("event: {}".format(event))
    params = event['pathParameters']
    logger.debug("params: {}".format(params))

    user = user_table.get_item(
        Key={'id': params['userId']}
    )
    wallet = wallet_table.query({
        'KeyConditionExpression': Key('userId').eq(params['userId']),
        'IndexName': 'WalletGSI'
    }).get('Items').pop()
    payment_history = history_table.query(
        KeyConditionExpression=Key('walletId').eq(wallet['id'])
    )
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
        'statusCode': 200,
        'body': json.dumps({
            'userName': user['Item']['name'],
            'currentAmount': int(wallet['amount']),
            'totalChargeAmount': int(sum_charge),
            'totalUseAmount': int(sum_payment),
            'timesPerLocation': times_per_location
        })
    }


def get_payment_history(event, context):
    logger.debug("event: {}".format(event))
    params = event['pathParameters']
    wallet = wallet_table.query({
        'KeyConditionExpression': Key('userId').eq(params['userId']),
        'IndexName': 'WalletGSI'
    }).get('Items').pop()
    payment_history_result = history_table.get_item(
        Key={'walletId': wallet['id']}
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

    sorted_payment_history = list(sorted(
        payment_history,
        key=lambda x:x['timestamp'],
        reverse=True))

    return {
        'statusCode': 200,
        'body': json.dumps(sorted_payment_history)
    }


def _get_location_name(location_id):
    logger.debug("location_id: {}".format(location_id))
    locations = requests.get(os.environ['LOCATION_ENDPOINT']).json()
    return locations[str(location_id)]
