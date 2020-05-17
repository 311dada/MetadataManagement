class metadata:
    def __init__(self, sample):
        temp = sample.split(', ')
        self.path, self.size, self.isdir, self.type, self.time = temp
        if self.path != '/':
            self.path = self.path.rstrip("/")

    def to_string(self):
        return ", ".join(
            [self.path, self.size, self.isdir, self.type, self.time])

    def display(self):
        print(
            f"============== THE INFO ==============\nPath: {self.path}\nSize: {self.size}\nDirectory: {self.isdir}\nFile type: {self.type}\nLast change time: {self.time}\n"
        )
