# WARNING: Do not rename this file "websocket.py"

import os
import time
import _thread

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

    # Example for Traffic Predix Zone
    ws.send('{"bbox":"-90:-180,90:180","eventTypes":["TFEVT"]}')

    # Example for Parking Predix Zone
    # ws.send('{"bbox":"-90:-180,90:180","eventTypes":["PKIN","PKOUT"]}')

if __name__ == '__main__':
    websocket.enableTrace(True)

    # Use Predix-Zone-ID to match events of interest
    # cityiq_zone = '---Parking Predix Zone HERE ---'
    cityiq_zone = 'SD-IE-TRAFFIC'

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
