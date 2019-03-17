import logging

########################################################################################################################
# Connection/Auth
########################################################################################################################

# API URL.
# BASE_URL = "https://testnet.bitmex.com/api/v1/"
BASE_URL = "https://www.bitmex.com/api/v1/"

INSTANCE_NAME = "WATCHER_NODE"

MONGO_DB_URI = "mongodb://mongo:27017/"

BITMEX_DB = "bitmex_data"
TRADES_COLLECTION = "trades"
TRADES_CURSOR_COLLECTION = "trades_cursor"
ORDER_BOOK_SNAPSHOTS_COLLECTION = "order_book_snapshots"

MAX_TRADES_COLLECTION_BYTES = 100000000
MAX_ORDER_BOOK_COLLECTION_BYTES = 100000000

REDIS_HOST = "redis"
REDIS_PORT = 6379
REDIS_DB = 0

########################################################################################################################
# Target
########################################################################################################################

# Instrument to market make on BitMEX.
SYMBOL = "XBTUSD"

########################################################################################################################
# Misc Behavior, Technicals
########################################################################################################################

# Subscribing "orderBookL2_25" would be sufficient.
MAX_ORDERS_OF_EACH_SIDE = 25

ENABLE_SAMPLE_SUBSCRIBER = False

LOOP_INTERVAL = 1.5
MAX_IDLE_COUNT = 10

# Available levels: logging.(DEBUG|INFO|WARN|ERROR)
LOG_LEVEL = logging.INFO

# Logging to files is not recommended when you run this on Docker.
# By leaving the name empty the program avoids to create log files.
LOG_FILE_NAME = ''
