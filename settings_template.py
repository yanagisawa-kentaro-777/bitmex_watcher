
# BASE_URL = "https://testnet.bitmex.com/api/v1/"
BASE_URL = "https://www.bitmex.com/api/v1/"

INSTANCE_NAME = "WATCHER_NODE"

MONGO_DB_URI = "mongodb://mongo:27017/"

REDIS_HOST = "redis"
REDIS_PORT = 6379
REDIS_DB = 0

# If this flag is set True, sample_subscriber.py (it does nothing meaningful.) is executed in another thread.
ENABLE_SAMPLE_SUBSCRIBER = False

# Logging to files is not recommended when you run this on Docker.
# By leaving the name empty the program avoids to create log files.
LOG_FILE_NAME = ''
