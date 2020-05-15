import socket
from configparser import ConfigParser
from collections import defaultdict
from metadata import metadata
from _thread import start_new_thread


class Server:
    def __init__(self):
        self.record = defaultdict(list)
        cfg = ConfigParser()
        cfg.read("../config/Server_config.ini")
        self.port = cfg.getint("network", "port")

    def new_thread(self, conn):
        request = conn.recv(4096).decode()
        print(f"server: {request}")
        command, content = request.split(' -> ')
        if command == 'input':
            sample = metadata(content)
            self.record[sample.path].append(command)
        conn.close()
    
    def response(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(('', self.port))
        s.listen(10)

        while True:
            conn, addr = s.accept()
            start_new_thread(new_thread, (conn))

        s.close()

    def run(self):
        print("A MDS is starting...")
        print("The MDS is listening...")
        self.response()


if __name__ == "__main__":
    server = Server()
    server.run()
