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
            "Welcome to use the simple client. It supports a basic POSIX command set: mkdir, touch, readdir, rm, stat, input, distribute\n"
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

        # input
        if formated_command[0] == "input":
            if len(formated_command) != 2 or not os.path.exists(
                    formated_command[1]) or not os.path.isfile(
                        formated_command[1]):
                print(
                    "Invalid input command: the input must be a proper file.")
                formated_command = None

        # mkdir
        elif formated_command[0] == "mkdir":
            if len(formated_command) != 2:
                print(
                    "Invalid mkdir command: an absolute directory path is required."
                )
                formated_command = None

        # touch
        elif formated_command[0] == "touch":
            if len(formated_command) != 2:
                print(
                    "Invalid mkdir command: an absolute directory path is required."
                )
                formated_command = None

        elif formated_command[0] == "rm":
            if len(formated_command) > 3 or len(
                    formated_command) == 3 and formated_command[1] != '-r':
                print("Invalid rm command: rm <filename> or rm -r <directory>")
                formated_command = None

        elif formated_command[0] == "stat":
            if len(formated_command) != 2:
                print(
                    "Invalid stat command: an absolute directory path is required."
                )
                formated_command = None

        elif formated_command[0] == "readdir":
            if len(formated_command) != 2:
                print(
                    "Invalid readdir command: an absolute directory path is required."
                )
                formated_command = None

        elif formated_command[0] == "distribute":
            if len(formated_command) != 1:
                print(
                    "Invalid distribute command: distribute is the only legal format."
                )
                formated_command = None

        else:
            print("Not supported command!")
            formated_command = None

        return formated_command

    # Execute a specific command
    def execute(self, formated_command):
        if formated_command[0] == "input":
            self._input(formated_command[1])

        elif formated_command[0] == "mkdir":
            self._mkdir(formated_command[1])

        elif formated_command[0] == "touch":
            self._touch(formated_command[1])

        elif formated_command[0] == "rm":
            self._remove(formated_command[-1])

        elif formated_command[0] == "stat":
            self._stat(formated_command[1])

        elif formated_command[0] == "readdir":
            path = formated_command[1]
            if '/' != path[0]:
                path = '/' + path
            resp = self._query_path(path)
            print(resp)
            if not resp:
                print(
                    "Invalid readdir command: the directory path does not exist.")

            elif resp == 'no':
                print("Invalid readdir command: the path is not a directory.")
                self._readdir(formated_command[1])

        elif formated_command[0] == "distribute":
            self._get_distribution()

    # Execute the input command
    def _input(self, input_file):
        with open(input_file, "r") as f:
            while True:
                line = f.readline().strip()
                if not line:
                    break
                self._initialize_insert(line)

    def _initialize_insert(self, line):
        sample = metadata(line)
        temp = sample.path.split("/")
        pre_path = None
        cur_index = -1

        self._insert(sample)

        while pre_path != '/':
            pre_path = '/'.join(temp[:cur_index])
            if pre_path == '':
                pre_path = '/'
            filename = temp[cur_index]
            cur_index -= 1
            if not self._query_path(pre_path):
                self._create(pre_path, "yes")
            self._add_to_dir(filename, pre_path)

    def _mkdir(self, path):
        if '/' != path[0]:
            path = '/' + path
        
        if path == "/":
            self._create(path, "yes")
            return
        temp = path.split("/")
        pre_path = '/'.join(temp[:-1])
        filename = temp[-1]
        if pre_path == '':
            pre_path = '/'
        resp = self._query_path(pre_path)
        if not resp:
            print(
                "Invalid mkdir command: the parent directory does not exist.")
        elif resp == 'no':
            print("Invalid mkdir command: the parent path is not a directory.")
        else:
            self._create(path, "yes")
            self._add_to_dir(filename, pre_path)

    def _touch(self, path):
        if '/' != path[0]:
            path = '/' + path
        temp = path.split("/")
        pre_path = '/'.join(temp[:-1])
        filename = temp[-1]
        if pre_path == '':
            pre_path = '/'
        resp = self._query_path(pre_path)
        if not resp:
            print(
                "Invalid touch command: the parent directory does not exist.")
        elif resp == 'no':
            print("Invalid touch command: the parent path is not a directory.")
        else:
            self._create(path, "no")
            self._add_to_dir(filename, pre_path)

    def _readdir(self, path):
        result = self._query(path)
        dir_or_file_list = []
        if result != "##none##":
            dir_or_file_list = result.split(";;")
            for next_path in dir_or_file_list:
                new_path = path + "/" + next_path
                self._readdir(new_path)
        if not dir_or_file_list:
            print(
                f"{path}: there are no files or directories in this directory."
            )
        else:
            print(f"{path}: {', '.join(dir_or_file_list)}")

    def _stat(self, path):
        if '/' != path[0]:
            path = '/' + path
        if not self._query_path(path):
            print(
                "Invalid stat command: the file or directory does not exist.")
        else:
            info = self._query_metadata(path)
            sample = metadata(info)
            sample.display()

    # Query the MDS if a path exists
    def _query_path(self, path):
        to_MDS = BKDRHash(path, self.seed, self.mds_num)
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((self.mds[to_MDS], self.port))
        s.sendall(f"query_path -> {path}".encode())
        result = s.recv(4096).decode()
        s.sendall("#finished#".encode())
        s.close()

        if result == "N":
            return False
        else:
            return result

    def _query(self, path):
        to_MDS = BKDRHash(path, self.seed, self.mds_num)
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((self.mds[to_MDS], self.port))
        s.sendall(f"query -> {path}".encode())
        result = s.recv(4096).decode()
        s.sendall("#finished#".encode())
        s.close()
        return result

    def _query_metadata(self, path):
        to_MDS = BKDRHash(path, self.seed, self.mds_num)
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((self.mds[to_MDS], self.port))
        s.sendall(f"query_metadata -> {path}".encode())
        result = s.recv(4096).decode()
        s.sendall("#finished#".encode())
        s.close()
        return result

    def _create(self, path, isdir):
        if isdir == "no":
            filename = path.split('/')[-1]
            if '.' in filename:
                ftype = filename.split('.')[-1]
            else:
                ftype = 'txt'
        else:
            ftype = "none"

        time_stamp = datetime.datetime.now()
        ctime = '"' + time_stamp.strftime('%Y-%m-%d %H:%M:%S') + '"'
        line_sample = ', '.join([path, '10', isdir, ftype, ctime])
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
        time.sleep(0.1)
        s.sendall("#finished#".encode())
        s.close()

    def _rm_dir_or_filename(self, path):
        to_MDS = BKDRHash(path, self.seed, self.mds_num)
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((self.mds[to_MDS], self.port))
        s.sendall(f"remove -> {path}".encode())
        time.sleep(0.1)
        s.sendall("#finished#".encode())
        s.close()

    # Remove a path
    def _remove(self, path):
        if '/' != path[0]:
            path = '/' + path
        result = self._query(path)
        print(result)
        if result != "##none##":
            dir_or_file_list = result.split(";;")
            for next_path in dir_or_file_list:
                new_path = path + "/" + next_path
                print(new_path)
                self._remove(new_path)
        self._rm_dir_or_filename(path)

    def _get_distribution(self):
        for to_MDS in range(self.mds_num):
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((self.mds[to_MDS], self.port))
            s.sendall("distribution -> ...".encode())
            distribution = s.recv(4096).decode()
            print(f"MDS {to_MDS + 1}:\n{distribution}")
            s.sendall("#finished#".encode())
            s.close()


if __name__ == "__main__":
    client = Client()
    client.run()
