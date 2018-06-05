import re
import os
import pandas


class Download:

    def __init__(self, directory='.'):
        self.directory = directory

    def to_frame(self, file):
        file_name = re.split('[/\\\\]', file.name)[-1]
        file_path = os.path.join(self.directory, file_name)
        open(file_path, 'wb').write(file.read())
        return pandas.DataFrame(index=pandas.Index([0]))
