from __future__ import absolute_import

import sys
import atexit
import signal

from time import sleep
from datetime import datetime

import logging

import pymongo
import redis

from pybitmex import *

from bitmex_watcher.models import *
from bitmex_watcher.settings import settings
from bitmex_watcher.utils import log, constants, errors


logger = log.setup_custom_logger('root')


class MarketWatcher:

    def __init__(self):
        self.instance_name = settings.INSTANCE_NAME

        # Client to the BitMex exchange.
        logger.info("Connecting to BitMEX exchange: %s %s %s",
                    settings.BASE_URL, settings.SYMBOL, settings.MARKET_ORDER_BOOK_DATA_NAME)
        self.bitmex_client = BitMEXClient(
            settings.BASE_URL, settings.SYMBOL,
            api_key=None, api_secret=None,
            use_websocket=True, use_rest=False,
            subscriptions=["instrument", settings.MARKET_ORDER_BOOK_DATA_NAME, "trade"]
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
            logger.error("Market is NOT in normal state: %s" % self.bitmex_client.ws_market_state())
            raise errors.MarketClosedError()

    def is_ws_idle(self, table_name, max_idle_count):
        last_update = self.bitmex_client.get_last_ws_update(table_name)
        if last_update is None:
            return True
        elapsed_seconds = (datetime.now() - last_update).total_seconds()
        logger.warning("WS elapsed seconds: %d", elapsed_seconds)
        return ((max_idle_count / 2.0) * settings.LOOP_INTERVAL) < elapsed_seconds

    def exit(self, p1=None, p2=None, p3=None):
        if not self.is_running:
            return
        if p1 and p2 and p3:
            logger.debug("")

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
    def create_order_book_snapshot(timestamp, bids, asks):
        return OrderBookSnapshot(timestamp, bids, asks, settings.TARGET_ORDER_BOOK_PRICE_RATIO)

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

    def _wait_while_market_is_closed(self):
        count = 0
        while (self.bitmex_client.ws_market_state() == "Closed") and (count < settings.MARKET_WAIT_SECONDS):
            logger.info("The market is closed. Waiting for a while.")
            count += 1
            sleep(1.0)

    def run_loop(self):
        try:
            order_book_digest = ""
            orders_idle_count = 0
            trades_idle_count = 0
            loop_count = 0
            trades_cursor = self.load_trades_cursor()
            while True:
                loop_start_time = datetime.now().astimezone(constants.TIMEZONE)
                loop_id = loop_start_time.strftime("%Y%m%d%H%M%S") + "_" + str(loop_count)
                logger.info("LOOP_HEAD[%s](%s)" % (loop_id, constants.VERSION))

                self._wait_while_market_is_closed()
                self.sanity_check()

                # Fetch recent trade data from the market.
                trades = self.bitmex_client.ws_sorted_recent_trade_objects_of_market()
                if 0 < len(trades):
                    logger.info("%d trades are fetched [%s - %s].",
                                len(trades),
                                trades[0].timestamp.strftime(constants.DATE_FORMAT),
                                trades[-1].timestamp.strftime(constants.DATE_FORMAT))
                else:
                    logger.info("NO trades are fetched from the market.")

                new_trades = MarketWatcher.filter_new_trades(trades_cursor, trades)
                if 0 < len(new_trades):
                    trades_idle_count = 0
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
                    trades_idle_count += 1
                    logger.info("NO new trades.")

                # Fetch order books.
                timestamp = datetime.now().astimezone(constants.TIMEZONE)
                bids, asks = self.bitmex_client.ws_sorted_bids_and_asks_of_market()
                order_book_snapshot = self.create_order_book_snapshot(timestamp, bids, asks)
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug("OrderBookSnapshot: %s" % str(order_book_snapshot))
                if not MarketWatcher.is_healthy(order_book_snapshot):
                    logger.error("OrderBookSnapshot corrupted: %s" % str(order_book_snapshot))
                    break

                prev_digest = order_book_digest
                order_book_digest = order_book_snapshot.digest_string()

                if prev_digest == order_book_digest:
                    orders_idle_count += 1
                    logger.info("Order book digest has NOT changed.")
                    self.redis.publish(settings.REDIS_ORDER_BOOK_SNAPSHOT_ID_CHANNEL_NAME, '*')
                else:
                    orders_idle_count = 0
                    # Save the order book snapshot to MongoDB.
                    insert_result = self.order_book_snapshot_collection.insert_one(order_book_snapshot.to_dict())
                    order_book_snapshot_id = str(insert_result.inserted_id)
                    logger.info("A new order book snapshot is inserted: %s" % order_book_snapshot_id)
                    # We publish the updated order book snapshot.
                    self.redis.publish(settings.REDIS_ORDER_BOOK_SNAPSHOT_ID_CHANNEL_NAME, order_book_snapshot_id)
                    logger.info("Published to redis [%s]: %s",
                                settings.REDIS_ORDER_BOOK_SNAPSHOT_ID_CHANNEL_NAME, order_book_snapshot_id)

                if settings.MAX_ORDERS_IDLE_COUNT < orders_idle_count:
                    logger.error("Order book NOT updated. Aborting. IdleCount=%d" % orders_idle_count)
                    break
                if settings.MAX_TRADES_IDLE_COUNT < trades_idle_count:
                    logger.error("Trades NOT updated. Aborting. IdleCount=%d; WS Idle: %s",
                                 trades_idle_count, self.is_ws_idle('trade', settings.MAX_TRADES_IDLE_COUNT))
                    break

                loop_end_time = datetime.now().astimezone(constants.TIMEZONE)
                elapsed_seconds = (loop_end_time - loop_start_time).total_seconds()
                logger.info("LOOP[%s] (SUMMARY) ElapsedSeconds: %.2f; OrderBookIdleCount: %d; TradesIdleCount: %d;",
                            loop_id, elapsed_seconds, orders_idle_count, trades_idle_count)

                # Sleep in the main loop.
                sleep(settings.LOOP_INTERVAL)
        except Exception as e:
            import traceback
            traceback.print_exc(file=sys.stdout)

            logger.info("Error: %s" % str(e))
            logger.info(sys.exc_info())
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
