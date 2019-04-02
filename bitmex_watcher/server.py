from __future__ import absolute_import

import sys
import atexit
import signal

from time import sleep
from datetime import datetime
from dateutil import parser

import logging

import pymongo
import redis

from bitmexclient import BitMEXClient

from bitmex_watcher.models import *
from bitmex_watcher.settings import settings
from bitmex_watcher.utils import log, constants, errors


logger = log.setup_custom_logger('root')


class MarketWatcher:

    def __init__(self):
        self.instance_name = settings.INSTANCE_NAME

        # Client to the BitMex exchange.
        logger.info("Connecting to BitMEX exchange: %s %s" % (settings.BASE_URL, settings.SYMBOL))
        self.bitmex_client = BitMEXClient(
            settings.BASE_URL, settings.SYMBOL,
            api_key=None, api_secret=None,
            use_websocket=True, use_rest=False,
            subscriptions=["instrument", "orderBookL2_25", "trade"]
        )

        # MongoDB client.
        logger.info("Connecting to %s" % settings.MONGO_DB_URI)
        self.mongo_client = pymongo.MongoClient(settings.MONGO_DB_URI)
        self.bitmex_db = self.mongo_client[settings.BITMEX_DB]
        # Create indices and set caps to collections.
        self._initialize_db_scheme()
        # Collections to save data in.
        self.trades_collection = self.bitmex_db[settings.TRADES_COLLECTION]
        self.order_book_snapshot_collection = self.bitmex_db[settings.ORDER_BOOK_SNAPSHOTS_COLLECTION]
        self.trades_cursor_collection = self.bitmex_db[settings.TRADES_CURSOR_COLLECTION]

        # Redis client.
        self.redis = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, db=settings.REDIS_DB)

        # Now the clients are all up.
        self.is_running = True

        """
        Once db, redis and exchange clients are created,
        register exit handler that will always release resources on any error.
        """
        atexit.register(self.exit)
        signal.signal(signal.SIGTERM, self.exit)

        self.sanity_check()

    def _initialize_db_scheme(self):
        collections = self.bitmex_db.list_collection_names()
        if (settings.TRADES_COLLECTION in collections) and (settings.ORDER_BOOK_SNAPSHOTS_COLLECTION in collections):
            logger.info("MongoDB scheme is already initialized. Do nothing.")
        else:
            logger.info("INITIALIZING MongoDB scheme.")
            self.bitmex_db.create_collection(settings.TRADES_COLLECTION,
                                             capped=True, size=settings.MAX_TRADES_COLLECTION_BYTES)
            self.bitmex_db[settings.TRADES_COLLECTION].create_index([("timestamp", pymongo.ASCENDING)])

            self.bitmex_db.create_collection(settings.ORDER_BOOK_SNAPSHOTS_COLLECTION,
                                             capped=True, size=settings.MAX_ORDER_BOOK_COLLECTION_BYTES)
            self.bitmex_db[settings.ORDER_BOOK_SNAPSHOTS_COLLECTION].create_index([("timestamp", pymongo.ASCENDING)])
            logger.info("INITIALIZED MongoDB scheme.")

    def sanity_check(self):
        # Ensure market is open.
        if not self.bitmex_client.is_market_in_normal_state():
            logger.error("Market is NOT in normal state: %s" % self.bitmex_client.get_instrument()["state"])
            raise errors.MarketClosedError()

    def exit(self, p1=None, p2=None, p3=None):
        if not self.is_running:
            return

        logger.info('SHUTTING DOWN BitMEX Watcher. Version %s' % constants.VERSION)

        try:
            self.mongo_client.close()
        except Exception as e:
            logger.info("Unable to close MongoDB client: %s" % e)
        try:
            self.bitmex_client.close()
        except Exception as e:
            logger.info("Unable to close Bitmex client: %s" % e)

        # Now the clients are all down.
        self.is_running = False

        sleep(1)
        sys.exit()

    @staticmethod
    def create_order_book_snapshot(timestamp, depth):
        bids = sorted([b for b in depth if b["side"] == "Buy"], key=lambda b: b["price"], reverse=True)
        asks = sorted([b for b in depth if b["side"] == "Sell"], key=lambda b: b["price"], reverse=False)

        def prune(order_books):
            return [{"price": float(each["price"]), "size": int(each["size"])} for each in order_books]

        return OrderBookSnapshot(timestamp, prune(bids), prune(asks), settings.MAX_ORDERS_OF_EACH_SIDE)

    @staticmethod
    def launder_trades(trades):
        result = [Trade(t["trdMatchID"], parser.parse(t["timestamp"]).astimezone(constants.TIMEZONE),
                        t["side"], float(t["price"]), int(t["size"])) for t in trades]
        return sorted([t for t in result], key=lambda t: (t.timestamp, t.trd_match_id), reverse=False)

    @staticmethod
    def filter_new_trades(cursor, all_trades):
        if cursor is None:
            new_trades = all_trades
        else:
            num_trades = len(all_trades)
            i = 0
            while (i < num_trades) and (not cursor.is_behind_of(all_trades[i])):
                i += 1
            new_trades = all_trades[i:]
        return new_trades

    @staticmethod
    def is_healthy(order_book_snapshot):
        if order_book_snapshot.lowest_ask <= order_book_snapshot.highest_bid:
            return False
        if settings.MAX_ORDERS_OF_EACH_SIDE < len(order_book_snapshot.bids):
            return False
        if settings.MAX_ORDERS_OF_EACH_SIDE < len(order_book_snapshot.asks):
            return False
        return True

    def load_trades_cursor(self):
        data = self.trades_cursor_collection.find_one()
        if data is None:
            logger.info("Trades cursor is NOT loaded.")
            return None
        else:
            result = TradesCursor(data['timestamp'], data['trdMatchID'])
            logger.info("Trades cursor is loaded: %s", str(result))
            return result

    def save_trades_cursor(self, cursor):
        if cursor is None:
            return
        self.trades_cursor_collection.remove()
        self.trades_cursor_collection.insert(cursor.to_dict())
        if logger.isEnabledFor(logging.ERROR):
            logger.debug("Trades cursor is saved: %s", str(cursor))

    def run_loop(self):
        try:
            order_book_digest = ""
            idle_count = 0

            trades_cursor = self.load_trades_cursor()
            while True:
                logger.info("LOOP[%s] (%s)" % (self.instance_name, constants.VERSION))
                self.sanity_check()

                # Fetch recent trade data from the market.
                raw_trades = self.bitmex_client.recent_trades()
                trades = MarketWatcher.launder_trades(raw_trades)
                if 0 < len(trades):
                    logger.info("%d trades are fetched [%s - %s].",
                                len(trades),
                                trades[0].timestamp.strftime(constants.DATE_FORMAT),
                                trades[-1].timestamp.strftime(constants.DATE_FORMAT))
                else:
                    logger.info("NO trades are fetched from the market.")

                new_trades = MarketWatcher.filter_new_trades(trades_cursor, trades)
                if 0 < len(new_trades):
                    logger.info("%d new trades. [%s - %s]",
                                len(new_trades),
                                new_trades[0].timestamp.strftime(constants.DATE_FORMAT),
                                new_trades[-1].timestamp.strftime(constants.DATE_FORMAT))
                    insert_result = self.trades_collection.insert_many([t.to_dict() for t in new_trades])
                    trades_cursor = TradesCursor(new_trades[-1].timestamp, new_trades[-1].trd_match_id)
                    logger.info("%d trades inserted. The last: %s",
                                len(insert_result.inserted_ids), str(trades_cursor))
                    self.save_trades_cursor(trades_cursor)
                else:
                    logger.info("NO new trades.")

                # Fetch order books.
                timestamp = datetime.now().astimezone(constants.TIMEZONE)
                order_books = self.bitmex_client.order_books()
                order_book_snapshot = MarketWatcher.create_order_book_snapshot(timestamp, order_books)
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug("OrderBookSnapshot: %s" % str(order_book_snapshot))
                if not MarketWatcher.is_healthy(order_book_snapshot):
                    logger.error("OrderBookSnapshot corrupted: %s" % str(order_book_snapshot))
                    break

                prev_digest = order_book_digest
                order_book_digest = order_book_snapshot.digest_string()

                if prev_digest == order_book_digest:
                    order_book_snapshot_id = "*"
                    logger.info("Order book digest has NOT changed.")
                else:
                    # Save the order book snapshot to MongoDB.
                    insert_result = self.order_book_snapshot_collection.insert_one(order_book_snapshot.to_dict())
                    order_book_snapshot_id = str(insert_result.inserted_id)
                    logger.info("A new order book snapshot is inserted: %s" % order_book_snapshot_id)

                if (0 < len(new_trades)) and (prev_digest != order_book_digest):
                    # At least either trades or order books were updated.
                    idle_count = 0
                    self.redis.publish("orderBookSnapshotID", order_book_snapshot_id)
                    logger.info("Published to redis: %s" % order_book_snapshot_id)
                else:
                    # No new data has arrived for LOOP_INTERVAL seconds.
                    idle_count += 1
                    logger.info("Nothing to publish. Count=%d" % idle_count)
                    if settings.MAX_IDLE_COUNT < idle_count:
                        logger.error("Communication trouble? Aborting. IdleCount=%d" % idle_count)
                        break

                # Sleep in the main loop.
                sleep(settings.LOOP_INTERVAL)
        except Exception as e:
            logger.error("Error: %s" % str(e))
            logger.error(sys.exc_info())
            raise e
        finally:
            self.exit()


def start():
    logger.info('STARTING BitMEX Watcher. Version %s' % constants.VERSION)
    # Try/except just keeps ctrl-c from printing an ugly stacktrace
    try:
        watcher = MarketWatcher()
        watcher.run_loop()
    except (KeyboardInterrupt, SystemExit):
        sys.exit()
