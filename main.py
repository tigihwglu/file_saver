import os.path


import paho.mqtt.client as mqtt
import yaml
import random
import logging
import time
import json
from datetime import datetime
from yaml.loader import SafeLoader

with open('config.yaml') as f:
    config = yaml.load(f, Loader=SafeLoader)

if os.path.isdir('error') == False:
    error_path = os.path.join(os.getcwd(),"error")
    os.mkdir(error_path)
if os.path.isdir('files') == False:
    file_path = os.path.join(os.getcwd(),"files")
    os.mkdir(file_path)


errorfilepath = "./error/error_{date}.txt"
flatfilepath  = "./files/file_{date}.txt"
broker = config['broker']
port = config['port']
topic = config['topic']
client_id = f'python-mqtt-{random.randint(0, 1000)}'

FIRST_RECONNECT_DELAY = 1
RECONNECT_RATE = 2
MAX_RECONNECT_COUNT = 12
MAX_RECONNECT_DELAY = 60


def on_connect(client,userdata, flags, reason_code, properties):
    if reason_code == 0:
        print("Connected to MQTT Broker!")
    else:
        print("Failed to connect, return code %d\n", reason_code)
    client.subscribe(topic)


def on_disconnect(client, userdata, flags, rc, properties):
    logging.info("Disconnected with result code: %s", rc)
    reconnect_count, reconnect_delay = 0, FIRST_RECONNECT_DELAY
    while reconnect_count < MAX_RECONNECT_COUNT:
        logging.info("Reconnecting in %d seconds...", reconnect_delay)
        time.sleep(reconnect_delay)

        try:
            client.reconnect()
            logging.info("Reconnected successfully!")
            return
        except Exception as err:
            logging.error("%s. Reconnect failed. Retrying...", err)

        reconnect_delay *= RECONNECT_RATE
        reconnect_delay = min(reconnect_delay, MAX_RECONNECT_DELAY)
        reconnect_count += 1
    logging.info("Reconnect failed after %s attempts. Exiting...", reconnect_count)

def on_message(client, userdata, msg):
    date_str = datetime.now().strftime("%Y-%m-%d")
    try:
        if not msg.payload:
            raise ValueError("Empty message received")
        payload = json.loads(msg.payload)

        with open(flatfilepath.format(date=date_str), "a") as flatfile:
            flatfile.write(f"{payload}\n")

    except ValueError as e:
        with open(errorfilepath.format(date=date_str), "a") as errorfile:
            errorfile.write(f"{str(e)}: 'file is empty',\n")
    except Exception as e:
        with open(errorfilepath.format(date=date_str), "a") as errorfile:
            errorfile.write(f"{str(e)}: {msg.payload},\n")

if __name__ == '__main__':
    mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    mqttc.on_connect = on_connect
    mqttc.on_disconnect = on_disconnect
    mqttc.on_message = on_message
    mqttc.connect(broker, port,60)


    mqttc.loop_forever()

