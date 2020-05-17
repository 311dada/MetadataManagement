class metadata:
    def __init__(self, sample):
        temp = sample.split(', ')
        self.path, self.size, self.isdir, self.type, self.time = temp
        self.path = self.path.rstrip("/")

    def to_string(self):
        return ", ".join(
            [self.path, self.size, self.isdir, self.type, self.time])
