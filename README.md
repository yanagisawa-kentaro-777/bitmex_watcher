# BitMEX Market Watcher

Store and utilize [BitMEX](https://www.bitmex.com) public data using MongoDB.

## Features

This is a market watcher program for use with [BitMEX](https://www.bitmex.com).
It provides the following functions:

* Fetches snapshot of current order books and recent trade records
from [BitMEX WebSocket API](https://www.bitmex.com/app/wsAPI).
* Saves the data to MongoDB.
* Publishes the update by Redis.

This program does not require authentication. No API keys or secrets needed.

> The author is not responsible for any losses incurred by using this code.

## Getting started

### Docker
```bash
docker-compose up -d
```

### Hosted
1. Install Python 3.3+.
2. Install MongoDB.
3. Install Redis.
4. Rename or copy /settings_template.py to /settings.py.
5. Edit host names and port numbers of MongoDB and Redis in /settings.py.
6. Run /startup.
