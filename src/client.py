# A POSIX API based client

import os
from metadata import metadata
from configparser import ConfigParser
import socket
from Hash import BKDRHash


class Client:
    def __init__(self):
        cfg = ConfigParser()
        cfg.read("../config/Client_config.ini")
        self.mds_num = cfg.getint('MDS', 'NUM')
        self.mds = [None for i in range(self.mds_num)]
        for i in range(self.mds_num):
            self.mds[i] = cfg.get('MDS', f"MDS{i + 1}")
        self.seed = cfg.getint('hash', 'seed')
        self.port = cfg.getint('network', 'port')

    # Run the client. Get some basic commands and parse them. Then
    # execute and interact with MDSs.
    def run(self):
        print("Start the POSIX API based client...")
        print(
            "Welcome to use the simple client. It supports a basic POSIX command set: mkdir, touch, readdir, rm, stat, input\n"
        )

        command = None

        while command != "q":
            command = input("command[quit: q]: ")
            formated_command = self.parse_command(command)
            if formated_command is not None:
                self.execute(formated_command)

        print("The client has been stopped. Bye!")

    # Parse basic commands
    def parse_command(self, command):
        formated_command = command.split()
        if formated_command[0] == "input":
            if len(formated_command) != 2 or not os.path.exists(
                    formated_command[1]) or not os.path.isfile(
                        formated_command[1]):
                print(
                    "Invalid input command: the input must is a proper file.")
                formated_command = None

        return formated_command

    # Execute a specific command
    def execute(self, formated_command):
        if formated_command[0] == "input":
            self._input(formated_command[1])

    # Execute the input command
    def _input(self, input_file):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        with open(input_file, "r") as f:
            while True:
                line = f.readline().strip()
                print(f"client: {line}")
                sample = metadata(line)
                to_MDS = BKDRHash(sample.path, self.seed, self.mds_num)
                mds_ip = self.mds[to_MDS]
                s.connect((mds_ip, self.port))
                
                s.sendall(f"input:{line}".encode())
        s.close()


if __name__ == "__main__":
    client = Client()
    client.run()
