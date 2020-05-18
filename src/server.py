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
                    resp = self.record[content][0].split(', ')[2]
                else:
                    resp = 'N'
                conn.sendall(resp.encode())

            elif command == 'add_dir':
                dir_path, filename = content.split(':')
                self.record[dir_path][1].add(filename)

            elif command == 'query':
                if content in self.record:
                    resp = ';;'.join(list(self.record[content][1]))
                else:
                    resp = ''
                conn.sendall(resp.encode())
            elif command == 'remove':
                if content in self.record:
                    del self.record[content]

            elif command == 'query_metadata':
                resp = self.record[content][0]
                conn.sendall(resp.encode())

            elif command == 'distribution':
                resp = sorted(self.record.keys())
                resp = "\n".join(list(map(lambda x: "\t" + x, resp)))
                conn.sendall(resp.encode())

            else:
                print("No corresponding response.")

            # print(self.record)
        conn.close()

    def response(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(('', self.port))
        s.listen(10)

        while True:
            conn, addr = s.accept()
            self.new_thread(conn)
            # start_new_thread(self.new_thread, (conn, ))

        s.close()

    def run(self):
        print("A MDS is starting...")
        print("The MDS is listening...")
        self.response()


if __name__ == "__main__":
    server = Server()
    server.run()
