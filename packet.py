class Packet:

    def __init__(self, path, file_type, table):
        self.path = path
        self.file_type = file_type
        self.table = table

    def __len__(self):
        return len(self.path) + len(self.file_type) + len(self.table)

    def __setitem__(self, path, file_type, table):
        self.path = path
        self.file_type = file_type
        self.table = table

    def __getitem__(self):
        return self
