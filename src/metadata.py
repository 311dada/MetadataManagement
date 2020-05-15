class metadata:
    def __init__(self, sample):
        temp = sample.split(', ')
        self.path, self.size, self.isdir, self.type, self.time = temp

    def to_string(self):
        return " ".join(
            [self.path, self.size, self.isdir, self.type, self.time])
