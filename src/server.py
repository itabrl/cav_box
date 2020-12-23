import os
import socket
import threading
import socketserver
from tcp_handler.handler import TCPHandler


if __name__ == "__main__":
    HOST = os.getenv('SERVER_HOST', "127.0.0.1")
    PORT = os.getenv('SERVER_PORT', 8882)

    # Create the server, binding to localhost on port 9999
    with socketserver.TCPServer((HOST, PORT), TCPHandler) as server:
        # Activate the server; this will keep running until you
        # interrupt the program with Ctrl-C
        server.serve_forever()

if __name__ == "__main__":
    HOST = os.getenv('SERVER_HOST', "127.0.0.1")
    PORT = os.getenv('SERVER_PORT', 8882)

    server = ThreadedTCPServer((HOST, PORT), ThreadedTCPRequestHandler)
    with server:
        ip, port = server.server_address

        # Start a thread with the server -- that thread will then start one
        # more thread for each request
        server_thread = threading.Thread(target=server.serve_forever)
        # Exit the server thread when the main thread terminates
        server_thread.daemon = True
        server_thread.start()
        print("Server loop running in thread:", server_thread.name)


        
        server.shutdown()
