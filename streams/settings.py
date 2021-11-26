from distutils.util import strtobool
import os

LOGGING = {
    "disable_existing_loggers": False,
    "merge": True,
    "formatters": {
        'colored': {
            '()': 'mode.utils.logging.DefaultFormatter',
            'format': "%(asctime)s | %(module)s | %(levelname)s | %(funcName)s | %(message)s",
        },
        'default': {
            '()': 'mode.utils.logging.DefaultFormatter',
            'format': "%(asctime)s | %(filename)s | %(levelname)s | %(funcName)s | %(message)s",
        }
    }
}

DEBUG = strtobool(os.getenv("DEBUG", "False"))

TRANSACTION_CARD_APP_ID = os.getenv("STREAM_APP_ID",
                                    f"stream_v1")

STORE_URI = os.getenv("STORE_URI", "memory://")

SSL_CONTEXT = None

TOPIC_ALLOW_DECLARE = strtobool(os.getenv("TOPIC_ALLOW_DECLARE", "True"))

TOPIC_DISABLE_LEADER = strtobool(os.getenv("TOPIC_DISABLE_LEADER", "False"))

OFFSET_ACK_ON_KAFKA = strtobool(os.getenv("OFFSET_ACK_ON_KAFKA", "False"))

KAFKA_BOOTSTRAP_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVER", "kafka://kafka:29092")

CARD_TOPIC_TRANSACTIONS = os.getenv("KAFKA_TOPIC_TRANSACTIONS", "card_transaction")
CORE_TOPIC_TRANSACTIONS = os.getenv("KAFKA_TOPIC_TRANSACTIONS", "core_transaction")
