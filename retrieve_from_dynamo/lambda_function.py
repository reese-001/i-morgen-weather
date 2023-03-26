import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timedelta
from boto3.dynamodb.conditions import Attr
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_all_time_min_max_temperature(table):
    try:
        response = table.scan(Select='ALL_ATTRIBUTES')
    except ClientError as e:
        logger.error("Error scanning DynamoDB table: %s", e.response['Error']['Message'])
        return None, None

    items = response.get('Items', [])
    if items:
        all_time_min_temp = min(float(item['temperature']) for item in items)
        all_time_max_temp = max(float(item['temperature']) for item in items)
        return all_time_min_temp, all_time_max_temp
    else:
        logger.warning('No temperature data found in the table')
        return None, None

def fetch_temperature_data():
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('temperature-data')
    
    all_time_min_temp, all_time_max_temp = get_all_time_min_max_temperature(table)

    current_time = datetime.utcnow()
    time_24_hours_ago = current_time - timedelta(hours=24)
    time_24_hours_ago_timestamp = int(time_24_hours_ago.timestamp())

    try:
        response = table.scan(
            FilterExpression=Attr('timestamp').gt(time_24_hours_ago_timestamp)
        )
    except ClientError as e:
        logger.error("Error scanning DynamoDB table: %s", e.response['Error']['Message'])
        return None

    items = response.get('Items', [])
    if items:
        min_temp = float('inf')
        max_temp = float('-inf')
        total_temp = 0
        count = 0
        max_timestamp = 0
        current_temp = None
        curent_dewpoint = None
        currentheatindex = None
        currentwindchill = None
        currentwindspeed = None
        precipitation_6_hr = None

        for item in response['Items']:
            temp        = float(item['temperature'])
            timestamp   = int(item['timestamp'])
            min_temp    = min(min_temp, temp)
            max_temp    = max(max_temp, temp)
            total_temp  += temp
            count += 1
                
            if timestamp > max_timestamp:
                max_timestamp       = timestamp
                current_temp        = float(item.get('temperature', 0)) # Provide a default value, e.g., 0
                curent_dewpoint     = float(item.get('dewpoint', 0)) # Provide a default value, e.g., 0
                currentheatindex    = float(item.get('heatindex', 0)) # Provide a default value, e.g., 0
                currentwindchill    = float(item.get('windchill', 0)) # Provide a default value, e.g., 0
                currentwindspeed    = float(item.get('windspeed', 0)) # Provide a default value, e.g., 0
                precipitation_6_hr  = float(item.get('precipitation_6_hr', 0)) # Provide a default value, e.g., 0

        avg_temp = total_temp / count
        avg_temp = round(avg_temp, 1)
       
        return {
            'min_temperature': min_temp,
            'max_temperature': max_temp,
            'average_temperature': avg_temp,
            'current_temperature': current_temp,
            'all_time_min_temperature': all_time_min_temp,
            'all_time_max_temperature': all_time_max_temp,
            'current_dewpoint': curent_dewpoint,
            'current_heatindex': currentheatindex,
            'current_windchill': currentwindchill,
            'current_windspeed': currentwindspeed,
            'precipitation_6_hr': precipitation_6_hr
        }

    else:
        logger.warning('No temperature data found in the past 24 hours')
        return None

def lambda_handler(event, context):
    temperature_data = fetch_temperature_data()
    return temperature_data

print(lambda_handler(None, None))

