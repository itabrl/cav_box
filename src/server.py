import socketserver
from binascii import hexlify, unhexlify
import J2735
import json
import sys
import csv
import logging
# from kafka import KafkaProducer
# from kafka.errors import KafkaError

from confluent_kafka import Producer
import socket

BOOTSTRAP_SERVER_IP = '127.0.0.1:9092'

# producer = KafkaProducer(bootstrap_servers=[BOOTSTRAP_SERVER_IP], value_serializer=lambda m: json.dumps(m).encode('ascii'))

conf = {'bootstrap.servers': BOOTSTRAP_SERVER_IP,
        'client.id': socket.gethostname()}

producer = Producer(conf)

class TCPHandler(socketserver.BaseRequestHandler):
    """
    The request handler class for our server.

    It is instantiated once per connection to the server, and must
    override the handle() method to implement communication to the
    client.
    """        

    def handle(self):
        # self.request is the TCP socket connected to the client
        self.data = self.request.recv(1024).strip()
        logging.info("{} Wrote:".format(self.client_address[0]))

        msg = J2735.DSRC.MessageFrame

        logging.info('Unhexlify Data')
        try:
            unhex_data = unhexlify(unhexlify(self.data).decode('utf-8'))
        except Exception as e:
            logging.exception(e)

        logging.info('Loading message from uper')
        try:
            msg.from_uper(unhex_data)
            msg()['value'][1]['coreData']['id'] = str(msg()['value'][1]['coreData']['id'])
        except Exception as e:
            logging.exception(e)

        def acked(err, msg):
            if err is not None:
                logging.error("Failed to deliver message: %s: %s" % (str(msg), str(err)))
            else:
                logging.info("Message produced: %s" % (msg))

        topic = 'received-dsrc-message'
        record_key = "J2735.DSRC.MessageFrame"
        record_value = json.dumps(msg())

        try:
            logging.info("Producing record: {}\t{}".format(record_key, record_value))
            producer.produce(topic, key=record_key, value=record_value, on_delivery=acked)
            producer.poll(0)
        except Exception as e:
            logging.exception(e)

        # just send back the same data, but upper-cased
        self.request.sendall(self.data.upper())


if __name__ == "__main__":
    HOST, PORT = "127.0.0.1", 8882

    # Create the server, binding to localhost on port 9999
    with socketserver.TCPServer((HOST, PORT), TCPHandler) as server:
        # Activate the server; this will keep running until you
        # interrupt the program with Ctrl-C
        server.serve_forever()
