# WARNING: Do not rename this file "websocket.py".
# NOTE/TODO: Currently BICYCLE and METROLOGY events don't work.
# NOTE/TODO: If the socket sends more than 3 units of bytes without
#            retrieving events, the code is not working at this time.
#            Try again tomorrow.

import os

# pip install websocket-client
import websocket

# pip install predix
import predix.security.uaa

def get_auth_token():
    uaa_uri = 'https://auth.aa.cityiq.io'
    os.environ['PREDIX_SECURITY_UAA_URI'] = uaa_uri
    uaa = predix.security.uaa.UserAccountAuthentication()

    client_id = 'PublicAccess'
    client_secret = 'uVeeMuiue4k='
    uaa.authenticate(client_id, client_secret)

    return uaa.get_token()

def on_message(ws, message):
    print(message)

def on_error(ws, ex):
    print(ex)

def on_close(ws):
    print('### closed ###')

def on_open(ws):
    print('### connected ###')

    # The events are listed in this order:
    #   TRAFFIC
    #   PARKING
    #   BICYCLE
    #   PEDESTRIAN
    #   METROLOGY
    #   ENVIRONMENTAL

    #ws.send('{"bbox":"-90:-180,90:180","eventTypes":["TFEVT"]}')
    #ws.send('{"bbox":"-90:-180,90:180","eventTypes":["PKIN","PKOUT"]}')
    #ws.send('{"bbox":"-90:-180,90:180","eventTypes":["BICYCLE"]}')
    #ws.send('{"bbox":"-90:-180,90:180","eventTypes":["PEDEVT"]}')
    #ws.send('{"bbox":"-90:-180,90:180","eventTypes":["METROLOGY"]}')
    ws.send('{"bbox":"-90:-180,90:180","eventTypes":["PRESSURE","TEMPERATURE","ORIENTATION","HUMIDITY"]}')

if __name__ == '__main__':
    websocket.enableTrace(True)

    # DIRECTIONS:
    #
    # Uncomment/Comment corresponding lines here and in the "on_open"
    # function to try a different web socket.
    
    #cityiq_zone = 'SD-IE-TRAFFIC'
    #cityiq_zone = 'SD-IE-PARKING'
    #cityiq_zone = 'SD-IE-BICYCLE'
    #cityiq_zone = 'SD-IE-PEDESTRIAN'
    #cityiq_zone = 'SD-IE-METROLOGY'
    cityiq_zone = 'SD-IE-ENVIRONMENTAL'

    cityiq = 'wss://sandiego.cityiq.io/api/v2/websocket/events'
    token = get_auth_token()

    headers = {
        'Authorization': 'Bearer ' + token,
        'Predix-Zone-Id': cityiq_zone,
        'Cache-Control': 'no-cache',
    }
    
    ws = websocket.WebSocketApp(cityiq,
                                header = headers,
                                on_message = on_message,
                                on_error = on_error,
                                on_close = on_close)
    ws.on_open = on_open
    ws.run_forever()
