'OpenWeatherMap to MQTT bridge'

import click
import logging
import paho.mqtt.client
import schedule
import json

__version__ = '1.0.0'

logging.basicConfig(format='%(asctime)s <%(levelname)s> %(message)s',
                    level=logging.DEBUG, datefmt='%Y-%m-%dT%H:%M:%S')

display_temperature = False
temperature = None
humidity = None


def job_tick(mqtt):
    global display_temperature
    global temperature
    global humidity
    logging.debug('Starting scheduled job')
    if not display_temperature:
        display_temperature = True
        put_temperature(mqtt, temperature)
    else:
        display_temperature = False
        put_humidity(mqtt, humidity)


def put_text(mqtt, text):
    if text is None:
        mqtt.publish('bigsegment/0/set', qos=1, payload='"-"')
        mqtt.publish('bigsegment/1/set', qos=1, payload='"-"')
        mqtt.publish('bigsegment/2/set', qos=1, payload='"-"')
        mqtt.publish('bigsegment/3/set', qos=1, payload='"-"')
        return
    if len(text) > 4:
        mqtt.publish('bigsegment/0/set', qos=1, payload='"-"')
        mqtt.publish('bigsegment/1/set', qos=1, payload='"-"')
        mqtt.publish('bigsegment/2/set', qos=1, payload='"-"')
        mqtt.publish('bigsegment/3/set', qos=1, payload='"-"')
        return
    if len(text) >= 1:
        mqtt.publish('bigsegment/3/set', qos=1, payload='"%s"' % text[-1])
    else:
        mqtt.publish('bigsegment/3/set', qos=1, payload='" "')
    if len(text) >= 2:
        mqtt.publish('bigsegment/2/set', qos=1, payload='"%s"' % text[-2])
    else:
        mqtt.publish('bigsegment/2/set', qos=1, payload='" "')
    if len(text) >= 3:
        mqtt.publish('bigsegment/1/set', qos=1, payload='"%s"' % text[-3])
    else:
        mqtt.publish('bigsegment/1/set', qos=1, payload='" "')
    if len(text) >= 4:
        mqtt.publish('bigsegment/0/set', qos=1, payload='"%s"' % text[-4])
    else:
        mqtt.publish('bigsegment/0/set', qos=1, payload='" "')


def put_temperature(mqtt, temperature):
    try:
        if temperature is None:
            put_text(mqtt, None)
        else:
            temperature = '%.0fC' % temperature
            put_text(mqtt, temperature)
    except:
        logging.error('Temperature processing failed', exc_info=True)


def put_humidity(mqtt, humidity):
    try:
        if humidity is None:
            put_text(mqtt, None)
        else:
            humidity = '%.0fRH' % humidity
            put_text(mqtt, humidity)
    except:
        logging.error('Humidity processing failed', exc_info=True)


def on_connect(client, userdata, flags, rc):
    logging.info('MQTT connected (code: %d)' % rc)
    client.subscribe('weather')


def on_message(client, userdata, msg):
    logging.debug('Received topic: %s' % msg.topic)
    if msg.topic == 'weather':
        try:
            data = json.loads(msg.payload.decode('utf-8'))
        except:
            logging.warning('Incorrect payload')
        global temperature
        global humidity
        temperature = data['main']['temp']
        humidity = data['main']['humidity']
    else:
        logging.warning('Unknown topic')


@click.command()
@click.option('--host', '-h', default='127.0.0.1', help='MQTT broker host.')
@click.option('--port', '-p', default='1883', help='MQTT broker port.')
@click.version_option(version=__version__)
def main(host, port):
    try:
        logging.info('Program started')
        logging.getLogger('schedule').propagate = False
        mqtt = paho.mqtt.client.Client()
        mqtt.on_connect = on_connect
        mqtt.on_message = on_message
        mqtt.connect(host, int(port))
        schedule.every(5).seconds.do(job_tick, mqtt)
        while True:
            schedule.run_pending()
            mqtt.loop()
    except KeyboardInterrupt:
        pass
