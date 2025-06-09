import json
from utils.utils import log

class JSONFileReader:
    def __init__(self, filename):
        self.filename = filename

    def read(self):
        with open(self.filename) as f:
            log(f"Loading the configuration file {self.filename}")
            return json.load(f)