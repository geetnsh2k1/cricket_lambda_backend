import json
import boto3
from customEncoder import CustomEncoder
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)
service_name = "dynamodb"
table_name = "Players"

def generate_error_response(body):
    response = dict()
    response['statusCode'] = 404
    response['headers'] = {
        'Access-Control-Allow-Headers': 'application/json',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
    }
    response['body'] = str(body)
    return response

def generate_right_response(body):
    response = dict()
    response['statusCode'] = 200
    response['headers'] = {
        'Access-Control-Allow-Headers': 'application/json',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
    }
    try:
        if type(body) != str:
            body = json.dumps(body, indent=4, cls=CustomEncoder)
        response['body'] = body
    except Exception as e:
        response['statusCode'] = 404
        response['body'] = e
    return response

def get_dynamodb(service_name):
    response = dict()
    response['status'] = True
    try:
        response['data'] = boto3.resource(service_name)
    except Exception as e:
        response['status'] = False
        response['data'] = e
    return response

def get_table(dynamodb, table_name):
    response = dict()
    response['status'] = True
    try:
        response['data'] = dynamodb.Table(table_name)
    except Exception as e:
        response['status'] = False
        response['data'] = e
    return response

def get_table_attrs(table):
    response = dict()
    response['status'] = True
    try:
        item = table.scan(Limit=1)['Items'][0]
        attrs = list(item.keys())
        response['data'] = attrs
    except Exception as e:
        response['status'] = False
        response['data'] = e
    return response

def get_player(table, name):
    response = dict()
    response['status'] = True
    try:
        response['data'] = table.get_item(
            Key={
                'name': name
            }
        )['Item']
    except Exception as e:
        response['status'] = False
        response['data'] = e
    return response

def get_players(table):
    response = dict()
    response['status'] = True
    try:
        response['data'] = table.scan()['Items']
    except Exception as e:
        response['status'] = False
        response['data'] = e
    return response

def get_scanner(Filters):
    response = dict()
    response['status'] = True
    try:
        bt_operation = ' AND '
        
        try:
            bt_operation = ' ' + Filters['bt_operation'] + ' '
            Filters.pop('bt_operation')
        except:
            pass
        
        FilterExpression = ""
        ExpressionAttributeNames = dict()
        ExpressionAttributeValues = dict()
        
        flg = False
        for k, v in Filters.items():
            key = k.upper()
            variable = '#'+key
            value = ':'+key
            ExpressionAttributeNames[variable] = k
            ExpressionAttributeValues[value] = v['value']
            
            operation = v['operation']
            
            if flg:
                FilterExpression += bt_operation
            flg = True
            
            if operation == 'gte':
                FilterExpression += f'{variable} >= {value}'
            elif operation == 'gt':
                FilterExpression += f'{variable} > {value}'
            elif operation == 'lte':
                FilterExpression += f'{variable} <= {value}'
            elif operation == 'lt':
                FilterExpression += f'{variable} < {value}'
            elif operation == 'eq':
                FilterExpression += f'{variable} = {value}'
            elif operation == 'ne':
                FilterExpression += f'{variable} <> {value}'
            elif operation == 'contains':
                FilterExpression += f'contains({variable}, {value})'
            elif operation == 'begins_with':
                FilterExpression += f'begins_with({variable}, {value})'
        
        if FilterExpression == "":
            raise Exception("No, valid filter provided!!")
        
        response['FilterExpression'] = FilterExpression
        response['ExpressionAttributeNames'] = ExpressionAttributeNames
        response['ExpressionAttributeValues'] = ExpressionAttributeValues
        
    except Exception as e:
        response['status'] = False
        response['data'] = e
    return response

def scan_players(table, filters):
    response = dict()
    response['status'] = True
    try:
        resp = get_scanner(filters)
        if resp['status'] == False:
            raise Exception(resp['data'])
        
        response['data'] = table.scan(
            FilterExpression=resp['FilterExpression'],
            ExpressionAttributeNames=resp['ExpressionAttributeNames'],
            ExpressionAttributeValues=resp['ExpressionAttributeValues']
        )['Items']
        
    except Exception as e:
        response['status'] = False
        response['data'] = e
    return response

def lambda_handler(event, context):
    logger.info(event)
    body = "Hey there!, It is working fine."
    
    method = event['httpMethod']
    resource = event['resource']
    
    try:
        resp = get_dynamodb(service_name=service_name)
        if resp['status'] == False:
            raise Exception(resp['data'])
        dynamodb = resp['data']
        
        resp = get_table(dynamodb=dynamodb, table_name=table_name)
        if resp['status'] == False:
            raise Exception(resp['data'])
        table = resp['data']
        
        data = None
        body = "None"
        if method == "POST":
            data = json.loads(event['body'])
            
            if resource == "/players":
                resp = scan_players(table=table, filters=data)
                if resp['status'] == False:
                    raise Exception(resp['data'])
                body = resp['data']
            else:
                resp = get_player(table=table, name=data['name'])
                if resp['status'] == False:
                    raise Exception(resp['data'])
                body = resp['data']
            
        else:
            resp = get_players(table=table)
            if resp['status'] == False:
                raise Exception(resp['data'])
            body = resp['data']
            
        return generate_right_response(body)
    
    except Exception as e:
        return generate_error_response(e)