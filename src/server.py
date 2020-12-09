import socketserver
from binascii import hexlify, unhexlify
import J2735
import json
import sys
import csv

from kafka import KafkaProducer
from kafka.errors import KafkaError

BOOTSTRAP_SERVER_IP = '127.0.0.1:9092'

# producer = KafkaProducer(bootstrap_servers=[BOOTSTRAP_SERVER_IP])

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
        print("{} wrote:".format(self.client_address[0]))

        msge = unhexlify(self.data).decode('utf-8')
        msg = J2735.DSRC.MessageFrame
        msg.from_uper(unhexlify(msge))

        print("msg", msg.__dict__)

        try:
            json_object = json.dumps(msg(), indent = 4) 

            # dict2str=json.dumps(msg(),indent=4,sort_keys=True,ensure_ascii=False)
            # parsed = json.loads(dict2str.decode("utf-8","ignore"))
            # parsed = json.loads(dict2str.decode("utf-8","ignore"))
            # dict2str=json.dumps(msg())
        except Exception as e:
            print(e)

        print("trying kafka")
        try:
            # Asynchronous by default
            future = producer.send('my-topic', b'raw_bytes')
        except Exception as e:
            print(e)

        # txt=b"0014251d59d162dad7de266e9a7d1ea6d4220974ffffffff8ffff080fdfa1fa1007fff0000640fa0"
        # hexed = hexlify(txt)

        # just send back the same data, but upper-cased
        self.request.sendall(self.data.upper())

if __name__ == "__main__":
    HOST, PORT = "127.0.0.1", 8882

    # Create the server, binding to localhost on port 9999
    with socketserver.TCPServer((HOST, PORT), TCPHandler) as server:
        # Activate the server; this will keep running until you
        # interrupt the program with Ctrl-C
        server.serve_forever()
