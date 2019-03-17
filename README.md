# BitMEX Market Watcher

Store and utilize [BitMEX](https://www.bitmex.com) market data.

[![GPL-3.0](https://img.shields.io/github/license/yanagisawa-kentaro-777/bitmex_watcher.svg)](LICENSE)

## Features

This is a market watcher program for use with BitMEX.
It is intended to be a data feeder for trading bots.
(At least I myself am using this as such for months.)

It provides the following functions:

* Fetches snapshot of current order books and recent trade records
from [BitMEX WebSocket API](https://www.bitmex.com/app/wsAPI).
* Saves the data to [MongoDB](https://www.mongodb.com/).
* Notifies the update to subscribing programs (e.g. bots) using [Redis](https://redis.io/).

This program does not require authentication. No BitMEX API keys or secrets needed.

*The author is not responsible for any losses incurred by using this code.*

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
