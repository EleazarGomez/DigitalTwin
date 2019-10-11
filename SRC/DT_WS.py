# WARNING: Do not rename this file "websocket.py".
# NOTE/TODO: If the socket sends more than 3 units of bytes without
#            retrieving events, the code is not working at this time.
#            Try again tomorrow.

import os
import json
import threading

# pip install websocket-client
import websocket

# pip install predix
import predix.security.uaa

#pip install pymysql
import pymysql

# DATABASE VARIABLES (update for your database
#                        -- must update query strings as well, table names)
##connection = pymysql.connect(host="localhost",
##                             user="root",
##                             passwd="password",
##                             db="cityiq_events")
##myCursor = connection.cursor()

def get_auth_token():
    uaa_uri = 'https://auth.aa.cityiq.io'
    os.environ['PREDIX_SECURITY_UAA_URI'] = uaa_uri
    uaa = predix.security.uaa.UserAccountAuthentication()

    client_id = 'PublicAccess'
    client_secret = 'uVeeMuiue4k='
    uaa.authenticate(client_id, client_secret)

    return uaa.get_token()

def on_message(ws, message):
    jsonMessage = json.loads(message)
    evt = jsonMessage["eventType"]

    # Construct query strings
    if (evt == "TFEVT"):
        queryTemplate = """INSERT INTO tfevt(timestamp,
                                             assetUid,
                                             locationUid,
                                             vehicleType,
                                             vehicleCount,
                                             speed,
                                             direction,
                                             counter_direction_vehicleCount,
                                             counter_direction_speed,
                                             counter_direction)
                           VALUES ({}, '{}', '{}', '{}', {}, {}, {}, {}, {}, {});
                        """
        queryString = queryTemplate.format(jsonMessage["timestamp"],
                                           jsonMessage["assetUid"],
                                           jsonMessage["locationUid"],
                                           jsonMessage["properties"]["vehicleType"],
                                           jsonMessage["measures"]["vehicleCount"],
                                           jsonMessage["measures"]["speed"],
                                           jsonMessage["measures"]["direction"],
                                           jsonMessage["measures"]["counter_direction_vehicleCount"],
                                           jsonMessage["measures"]["counter_direction_speed"],
                                           jsonMessage["measures"]["counter_direction"])

        with open('../../../data/TFEVT.txt', 'a+') as f:
            json.dump(jsonMessage, f)
            f.write('\n')
    elif (evt == "PKIN"):
        queryTemplate = """INSERT INTO pkin(timestamp,
                                            assetUid,
                                            locationUid,
                                            pixelCoordinates,
                                            orgPixelCoordinates,
                                            objectUid,
                                            imageAssetUid,
                                            geoCoordinates)
                           VALUES ({}, '{}', '{}', '{}', '{}', '{}', '{}', '{}');
                        """
        queryString = queryTemplate.format(jsonMessage["timestamp"],
                                           jsonMessage["assetUid"],
                                           jsonMessage["locationUid"],
                                           jsonMessage["properties"]["pixelCoordinates"],
                                           jsonMessage["properties"]["orgPixelCoordinates"],
                                           jsonMessage["properties"]["objectUid"],
                                           jsonMessage["properties"]["imageAssetUid"],
                                           jsonMessage["properties"]["geoCoordinates"])
        with open('../../../data/PKIN.txt', 'a+') as f:
            json.dump(jsonMessage, f)
            f.write('\n')
    elif (evt == "PKOUT"):
        queryTemplate = """INSERT INTO pkout(timestamp,
                                             assetUid,
                                             locationUid,
                                             pixelCoordinates,
                                             orgPixelCoordinates,
                                             objectUid,
                                             imageAssetUid,
                                             geoCoordinates)
                           VALUES ({}, '{}', '{}', '{}', '{}', '{}', '{}', '{}');
                        """
        queryString = queryTemplate.format(jsonMessage["timestamp"],
                                           jsonMessage["assetUid"],
                                           jsonMessage["locationUid"],
                                           jsonMessage["properties"]["pixelCoordinates"],
                                           jsonMessage["properties"]["orgPixelCoordinates"],
                                           jsonMessage["properties"]["objectUid"],
                                           jsonMessage["properties"]["imageAssetUid"],
                                           jsonMessage["properties"]["geoCoordinates"]) # occasionally complains about this line
        with open('../../../data/PKOUT.txt', 'a+') as f:
            json.dump(jsonMessage, f)
            f.write('\n')
    elif (evt == "PEDEVT"):
        queryTemplate = """INSERT INTO pedevt(timestamp,
                                              assetUid,
                                              locationUid,
                                              pedestrianCount,
                                              speed,
                                              direction,
                                              counter_direction_pedestrianCount,
                                              counter_direction_speed,
                                              counter_direction)
                           VALUES ({}, '{}', '{}', {}, {}, {}, {}, {}, {});
                        """
        queryString = queryTemplate.format(jsonMessage["timestamp"],
                                           jsonMessage["assetUid"],
                                           jsonMessage["locationUid"],
                                           jsonMessage["measures"]["pedestrianCount"],
                                           jsonMessage["measures"]["speed"],
                                           jsonMessage["measures"]["direction"],
                                           jsonMessage["measures"]["counter_direction_pedestrianCount"],
                                           jsonMessage["measures"]["counter_direction_speed"],
                                           jsonMessage["measures"]["counter_direction"])
        peds = jsonMessage["measures"]["pedestrianCount"] + jsonMessage["measures"]["counter_direction_pedestrianCount"]
        with open('../../../data/PEDEVT.txt', 'a+') as f:
            json.dump(jsonMessage, f)
            f.write('\n')
    elif (evt == "PRESSURE"):
        queryTemplate = """INSERT INTO pressure(timestamp,
                                                assetUid,
                                                locationUid,
                                                powerOf10,
                                                min,
                                                median,
                                                max,
                                                mean)
                           VALUES ({}, '{}', '{}', '{}', {}, {}, {}, {});
                        """
        queryString = queryTemplate.format(jsonMessage["timestamp"],
                                           jsonMessage["assetUid"],
                                           jsonMessage["locationUid"],
                                           jsonMessage["properties"]["powerOf10"],
                                           jsonMessage["measures"]["min"],
                                           jsonMessage["measures"]["median"],
                                           jsonMessage["measures"]["max"],
                                           jsonMessage["measures"]["mean"])
        with open('../../../data/PRESSURE.txt', 'a+') as f:
            json.dump(jsonMessage, f)
            f.write('\n')
    elif (evt == "TEMPERATURE"):
        queryTemplate = """INSERT INTO temperature(timestamp,
                                                   assetUid,
                                                   locationUid,
                                                   powerOf10,
                                                   min,
                                                   median,
                                                   max,
                                                   mean)
                           VALUES ({}, '{}', '{}', '{}', {}, {}, {}, {});
                        """
        queryString = queryTemplate.format(jsonMessage["timestamp"],
                                           jsonMessage["assetUid"],
                                           jsonMessage["locationUid"],
                                           jsonMessage["properties"]["powerOf10"],
                                           jsonMessage["measures"]["min"],
                                           jsonMessage["measures"]["median"],
                                           jsonMessage["measures"]["max"],
                                           jsonMessage["measures"]["mean"])
        with open('../../../data/TEMPERATURE.txt', 'a+') as f:
            json.dump(jsonMessage, f)
            f.write('\n')
    elif (evt == "HUMIDITY"):
        queryTemplate = """INSERT INTO humidity(timestamp,
                                                assetUid,
                                                locationUid,
                                                powerOf10,
                                                min,
                                                median,
                                                max,
                                                mean)
                           VALUES ({}, '{}', '{}', '{}', {}, {}, {}, {});
                        """
        queryString = queryTemplate.format(jsonMessage["timestamp"],
                                           jsonMessage["assetUid"],
                                           jsonMessage["locationUid"],
                                           jsonMessage["properties"]["powerOf10"],
                                           jsonMessage["measures"]["min"],
                                           jsonMessage["measures"]["median"],
                                           jsonMessage["measures"]["max"],
                                           jsonMessage["measures"]["mean"])
        with open('../../../data/HUMIDITY.txt', 'a+') as f:
            json.dump(jsonMessage, f)
            f.write('\n')
    elif (evt == "ORIENTATION"):
        queryTemplate = """INSERT INTO orientation(timestamp,
                                                   assetUid,
                                                   locationUid,
                                                   powerOf10,
                                                   minX,
                                                   medianX,
                                                   maxX,
                                                   meanX,
                                                   minY,
                                                   medianY,
                                                   maxY,
                                                   meanY,
                                                   minZ,
                                                   medianZ,
                                                   maxZ,
                                                   meanZ)
                           VALUES ({}, '{}', '{}', '{}', {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {});
                        """
        queryString = queryTemplate.format(jsonMessage["timestamp"],
                                           jsonMessage["assetUid"],
                                           jsonMessage["locationUid"],
                                           jsonMessage["properties"]["powerOf10"],
                                           jsonMessage["measures"]["minX"],
                                           jsonMessage["measures"]["medianX"],
                                           jsonMessage["measures"]["maxX"],
                                           jsonMessage["measures"]["meanX"],
                                           jsonMessage["measures"]["minY"],
                                           jsonMessage["measures"]["medianY"],
                                           jsonMessage["measures"]["maxY"],
                                           jsonMessage["measures"]["meanY"],
                                           jsonMessage["measures"]["minZ"],
                                           jsonMessage["measures"]["medianZ"],
                                           jsonMessage["measures"]["maxZ"],
                                           jsonMessage["measures"]["meanZ"])
        with open('../../../data/ORIENTATION.txt', 'a+') as f:
            json.dump(jsonMessage, f)
            f.write('\n')

    # Execute query and commit to database
    #myCursor.execute(queryString)
    #connection.commit()

def on_error(ws, ex):
    print(ex)

def on_close(ws):
    print('### closed ###')
    connection.close()

def traffic_on_open(ws):
    print('### connected ###')
    ws.send('{"bbox":"-90:-180,90:180","eventTypes":["TFEVT"]}')

def parking_on_open(ws):
    print('### connected ###')
    ws.send('{"bbox":"-90:-180,90:180","eventTypes":["PKIN","PKOUT"]}')

def pedestrian_on_open(ws):
    print('### connected ###')
    ws.send('{"bbox":"-90:-180,90:180","eventTypes":["PEDEVT"]}')

def environmental_on_open(ws):
    print('### connected ###')
    ws.send('{"bbox":"-90:-180,90:180","eventTypes":["PRESSURE","TEMPERATURE","ORIENTATION","HUMIDITY"]}')

def headers(zone):
    token = get_auth_token()
    
    headers = {
        'Authorization': 'Bearer ' + token,
        'Predix-Zone-Id': zone,
        'Cache-Control': 'no-cache',
    }

    return headers

if __name__ == '__main__':
    websocket.enableTrace(True)
    
    traffic_zone = 'SD-IE-TRAFFIC'
    parking_zone = 'SD-IE-PARKING'
    pedestrian_zone = 'SD-IE-PEDESTRIAN'
    environmental_zone = 'SD-IE-ENVIRONMENTAL'

    cityiq = 'wss://sandiego.cityiq.io/api/v2/websocket/events'

    # Creating Web Sockets
    traffic_ws = websocket.WebSocketApp(cityiq,
                                        header = headers(traffic_zone),
                                        on_message = on_message,
                                        on_error = on_error,
                                        on_close = on_close)
    traffic_ws.on_open = traffic_on_open

    parking_ws = websocket.WebSocketApp(cityiq,
                                        header = headers(parking_zone),
                                        on_message = on_message,
                                        on_error = on_error,
                                        on_close = on_close)
    parking_ws.on_open = parking_on_open

    pedestrian_ws = websocket.WebSocketApp(cityiq,
                                           header = headers(pedestrian_zone),
                                           on_message = on_message,
                                           on_error = on_error,
                                           on_close = on_close)
    pedestrian_ws.on_open = pedestrian_on_open

    environmental_ws = websocket.WebSocketApp(cityiq,
                                              header = headers(environmental_zone),
                                              on_message = on_message,
                                              on_error = on_error,
                                              on_close = on_close)
    environmental_ws.on_open = environmental_on_open

    # Creating Threads
    traffic_thread = threading.Thread(target = traffic_ws.run_forever, args=())
    parking_thread = threading.Thread(target = parking_ws.run_forever, args=())
    pedestrian_thread = threading.Thread(target = pedestrian_ws.run_forever, args=())
    environmental_thread = threading.Thread(target = environmental_ws.run_forever, args=())

    # Traffic is commented out for now because of the amount of data it produces
    #traffic_thread.start()
    parking_thread.start()
    pedestrian_thread.start()
    environmental_thread.start()
