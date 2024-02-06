class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(
                *args, **kwargs
            )
        return cls._instances[cls]


class Bitcask(metaclass=Singleton):
    def __init__(self):
        self.keydir = {}
        self.data = {}

    def open(self, dataDir):
        return True

    def get(self, key):
        return 'value'

    def put(self, key, value):
        return True

    def delete(self, key):
        return True

    def list_keys(self):
        return []

    def fold(self, fun, acc):
        return []

    def merge(self):
        return True

    def sync(self):
        return True

    def close(self):
        return True
