import concurrent.futures

from datetime import datetime, timedelta

import pymongo
from bson.objectid import ObjectId
import redis

from bitmex_watcher.settings import settings
from bitmex_watcher.utils import log, constants


logger = log.setup_custom_logger('root')


class SampleSubscriber:

    def __init__(self):
        # MongoDB client.
        self.mongo_client = pymongo.MongoClient(settings.MONGO_DB_URI)
        self.bitmex_db = self.mongo_client[settings.BITMEX_DB]
        # Collections to save data in.
        self.trades_collection = self.bitmex_db[settings.TRADES_COLLECTION]
        self.order_book_snapshot_collection = self.bitmex_db[settings.ORDER_BOOK_SNAPSHOTS_COLLECTION]

        # Redis client.
        self.redis = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, db=settings.REDIS_DB)

    def wait_and_load_market_data(self):
        pubsub = self.redis.pubsub()
        pubsub.subscribe("orderBookSnapshotID")
        for message in pubsub.listen():
            try:
                logger.debug("[SUB] Message arrived from Redis: %s" % str(message))

                raw_order_book_snapshot_id = message.get("data")
                if raw_order_book_snapshot_id is None or raw_order_book_snapshot_id == 1:
                    continue

                order_book_snapshot_id = raw_order_book_snapshot_id.decode(encoding='utf-8')
                logger.info("[SUB] Received OrderBookSnapshotID: %s" % order_book_snapshot_id)
                self.order_book_snapshot_collection.find_one()
                loaded_snapshot = self.order_book_snapshot_collection.find_one(
                    {"_id": ObjectId(order_book_snapshot_id)})
                if loaded_snapshot is None:
                    logger.info("[SUB] Cannot load snapshot for %s" % order_book_snapshot_id)
                    continue
                logger.info("[SUB] Loaded from MongoDB: %s", str(loaded_snapshot))

                std_datetime = datetime.now().astimezone(constants.TIMEZONE) - timedelta(minutes=30)
                logger.info("[SUB] StdDateTime: %s", std_datetime.strftime(constants.DATE_FORMAT))

                trades_pipeline = [
                    {'$match':
                         {'timestamp': {'$gte': std_datetime}}
                     },
                    {'$group':
                         {'_id': 'null',
                          'total_volume': {'$sum': '$size'},
                          'bought_volume': {'$sum': '$boughtSize'},
                          'sold_volume': {'$sum': '$soldSize'},
                          'market_momentum': {'$sum': '$momentum'},
                          'average_price': {'$avg': '$price'},
                          'sd_of_price': {'$stdDevPop': '$price'},
                          'min_price': {'$min': '$price'},
                          'max_price': {'$max': '$price'}
                          }
                     }
                ]
                rows = self.trades_collection.aggregate(pipeline=trades_pipeline)
                for row in rows:
                    logger.info(
                        "[SUB] %d trade vol. MarketMomentum: %d (%d - %d). Avg price: %.2f, SD: %.2f, [%.1f - %.1f]",
                        row['total_volume'], row['market_momentum'], row['bought_volume'], row['sold_volume'],
                        row['average_price'], row['sd_of_price'], row['min_price'], row['max_price'])
            except Exception as e:
                logger.error(e)


def start():
    from time import sleep
    sleep(10)

    subscriber = SampleSubscriber()
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    executor.submit(subscriber.wait_and_load_market_data)
