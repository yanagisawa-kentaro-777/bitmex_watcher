import logging
import os

########################################################################################################################
# Connection/Auth
########################################################################################################################

# API URL.
BASE_URL = "https://www.bitmex.com/api/v1/"

MARKET_ORDER_BOOK_DATA_NAME = os.environ['MARKET_ORDER_BOOK_DATA_NAME']

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

REDIS_ORDER_BOOK_SNAPSHOT_ID_CHANNEL_NAME = 'from-watcher:order-book-snapshot-id'
REDIS_ORDER_BOOK_SNAPSHOT_ID_KEY_NAME = 'from-watcher:latest-order-book-snapshot-id'
SHOULD_PUBLISH_TO_CHANNEL = False

GRAPHITE_HOST = "graphite"
GRAPHITE_PORT = 2004


########################################################################################################################
# Target
########################################################################################################################

# Instrument to market make on BitMEX.
SYMBOL = "XBTUSD"

########################################################################################################################
# Misc Behavior, Technicals
########################################################################################################################

TARGET_ORDER_BOOK_PRICE_RATIO = 0.005

ENABLE_SAMPLE_SUBSCRIBER = False

LOOP_INTERVAL = float(os.environ['LOOP_INTERVAL'])
MARKET_WAIT_SECONDS = 25
MAX_ORDERS_IDLE_COUNT = 5
MAX_TRADES_IDLE_COUNT = 25

WS_REFRESH_INTERVAL = 600
REFRESH_WAIT_SECONDS = 10

# Available levels: logging.(DEBUG|INFO|WARN|ERROR)
LOG_LEVEL = logging.INFO

# Logging to files is not recommended when you run this on Docker.
# By leaving the name empty the program avoids to create log files.
LOG_FILE_NAME = ''
