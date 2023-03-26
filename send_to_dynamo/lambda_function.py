import json
import urllib.request
from datetime import datetime
import boto3

dynamodb = boto3.client("dynamodb")


def lambda_handler(event, context):
    url = "https://api.weather.gov/stations/KANE/observations"
    headers = {
        "User-Agent": "get-weather",
        "Accept": "application/geo+json"
    }

    req = urllib.request.Request(url, headers=headers)
    try:
        response = urllib.request.urlopen(req)
    except urllib.error.HTTPError as e:
        return {
            "statusCode": e.code,
            "body": f"Error fetching weather data: {e.reason}"
        }
    data = json.loads(response.read().decode("utf-8"))

    properties = data['features'][0]['properties']
    current_temperature = properties['temperature']['value']
    current_dewpoint = properties['dewpoint']['value']
    current_windspeed = properties['windSpeed']['value']
    current_precipitation_last_6_hrs = properties['precipitationLast6Hours']['value'] or 0
    current_windchill = properties['windChill']['value'] or 0
    current_heatindex = properties['heatIndex']['value'] or 0

    current_timestamp = int(datetime.utcnow().timestamp())

    item = {
        "timestamp": {"N": str(current_timestamp)},
        "city": {"S": "Plymouth, MN"},
        "temperature": {"N": str(current_temperature)},
        "unit": {"S": "C"},
        "dewpoint": {"N": str(current_dewpoint)},
        "windspeed": {"N": str(current_windspeed)},
        "precipitation_last_6_hrs": {"N": str(current_precipitation_last_6_hrs)},
        "windchill": {"N": str(current_windchill)},
        "heatindex": {"N": str(current_heatindex)}
    }

    dynamodb.put_item(TableName="temperature-data", Item=item)
    
    return {
        "statusCode": 200,
        "body": f"Current temperature in Minneapolis, MN: {current_temperature} C Timestamp: {current_timestamp}"
    }

# print(lambda_handler(None, None))
