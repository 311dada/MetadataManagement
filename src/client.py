# A POSIX API based client

import os
from metadata import metadata
from configparser import ConfigParser
import socket
from Hash import BKDRHash
import datetime
import time


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
        with open(input_file, "r") as f:
            while True:
                line = f.readline().strip()
                if not line:
                    break
                # print(f"client: {line}")
                # sample = metadata(line)
                # to_MDS = BKDRHash(sample.path, self.seed, self.mds_num)

                # sockets[to_MDS].sendall(f"input -> {line}".encode())
                # time.sleep(0.1)
                print(line)
                self._initialize_insert(line)


    def _initialize_insert(self, line):
        sample = metadata(line)
        temp = sample.path.split("/")
        pre_path = None
        cur_index = -1

        self._insert(sample)

        while pre_path != '/':
            print(pre_path)
            pre_path = '/'.join(temp[:cur_index])
            if pre_path == '':
                pre_path = '/'
            filename = temp[cur_index]
            cur_index -= 1
            if not self._query_path(pre_path):
                self._create(pre_path, "yes")
            self._add_to_dir(filename, pre_path)
        

    # Query the MDS if a path exists
    def _query_path(self, path):
        to_MDS = BKDRHash(path, self.seed, self.mds_num)
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((self.mds[to_MDS], self.port))
        s.sendall(f"query_path -> {path}".encode())
        result = s.recv(4096).decode()
        s.sendall("#finished#".encode())
        s.close()

        if result == "Y":
            return True
        else:
            return False

    def _create(self, path, isdir):
        filename = path.split('/')[-1]
        if '.' in filename:
            ftype = filename.split('.')[-1]
        else:
            ftype = 'txt'

        time_stamp = datetime.datetime.now()
        ctime = '"' + time_stamp.strftime('%Y-%m-%d %H:%M:%S') + '"'
        line_sample = ', '.join([path, '10', isdir, ftype, ctime])
        print(line_sample)
        to_MDS = BKDRHash(path, self.seed, self.mds_num)
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((self.mds[to_MDS], self.port))
        s.sendall(f"insert -> {line_sample}".encode())
        time.sleep(0.1)
        s.sendall("#finished#".encode())
        s.close()

    def _insert(self, sample):
        to_MDS = BKDRHash(sample.path, self.seed, self.mds_num)
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((self.mds[to_MDS], self.port))
        s.sendall(f"insert -> {sample.to_string()}".encode())
        time.sleep(0.1)
        s.sendall("#finished#".encode())
        s.close()

    def _add_to_dir(self, filename, dir_path):
        to_MDS = BKDRHash(dir_path, self.seed, self.mds_num)
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((self.mds[to_MDS], self.port))
        msg = dir_path + ':' + filename
        s.sendall(f"add_dir -> {msg}".encode())
        s.sendall("#finished#".encode())
        s.close()

    # Remove a path
    def _remove(self, path):
        pass


if __name__ == "__main__":
    client = Client()
    client.run()
