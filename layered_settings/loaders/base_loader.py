import logging


try:
    import boto3
except ImportError:
    boto3 = None

try:
    import aioboto3
except ImportError:
    aioboto3 = None

logger = logging.getLogger(__name__)


class BaseLoader:
    def __init__(self):
        pass

    def get_setting(self, section, key):
        raise NotImplementedError("must implement in subclass.")
