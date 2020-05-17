import socket
from configparser import ConfigParser
from metadata import metadata
from _thread import start_new_thread


class Server:
    def __init__(self):
        self.record = dict()
        cfg = ConfigParser()
        cfg.read("../config/Server_config.ini")
        self.port = cfg.getint("network", "port")

    def new_thread(self, conn):
        while True:
            request = conn.recv(4096).decode()
            if request == "#finished#":
                break
            print(f"server: {request}")
            command, content = request.split(' -> ')
            if command == 'insert':
                sample = metadata(content)
                if sample.path not in self.record:
                    self.record[sample.path] = [content, set()]
                else:
                    self.record[sample.path][0] = content

            elif command == 'query_path':
                if content in self.record:
                    resp = 'Y'
                else:
                    resp = 'N'
                conn.sendall(resp.encode())

            elif command == 'add_dir':
                dir_path, filename = content.split(':')
                self.record[dir_path][1].add(filename)

        conn.close()

    def response(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(('', self.port))
        s.listen(10)

        while True:
            conn, addr = s.accept()
            start_new_thread(self.new_thread, (conn, ))

        s.close()

    def run(self):
        print("A MDS is starting...")
        print("The MDS is listening...")
        self.response()


if __name__ == "__main__":
    server = Server()
    server.run()
