import socketserver
from binascii import hexlify, unhexlify
import J2735
import json
import sys
import csv
import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError

BOOTSTRAP_SERVER_IP = '127.0.0.1:9092'

producer = KafkaProducer(bootstrap_servers=[BOOTSTRAP_SERVER_IP], value_serializer=lambda m: json.dumps(m).encode('ascii'))

class TCPHandler(socketserver.BaseRequestHandler):
    """
    The request handler class for our server.

    It is instantiated once per connection to the server, and must
    override the handle() method to implement communication to the
    client.
    """        

    def handle(self):
        def on_send_success(record_metadata):
            logging.info('Publish data to ' + record_metadata.topic)

        def on_send_error(excp):
            logging.error('Error sending data', exc_info=excp)

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

        try:
            # rdm Received DSRC Messages
            future = producer.send('received-dsrc-messages', value=msg()).add_callback(on_send_success).add_errback(on_send_error)
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
