import unittest
from datetime import datetime


class TestOrderBookSnapshot(unittest.TestCase):

    def test_order_books_attributes_calculation1(self):

        from bitmex_watcher.utils import constants
        from bitmex_watcher.models import OrderBookSnapshot

        bids = [{"price": 100.0, "size": 100}, {"price": 99.5, "size": 200}]
        asks = [{"price": 100.5, "size": 10}, {"price": 101.0, "size": 50}]
        depth = OrderBookSnapshot(datetime.now().astimezone(constants.TIMEZONE), bids, asks, 0.0075)

        self.assertEqual(100.25, depth.mid_price)
        self.assertEqual(100.5417, depth.price_from_depth)
        self.assertEqual(0.2917, depth.depth_bias)
        self.assertEqual(0.8333, depth.bids_ratio)
        self.assertEqual(360, depth.total_volume)

        d = depth.to_dict()
        self.assertEqual(depth.mid_price, d["midPrice"])
        self.assertEqual(depth.depth_bias, d["depthBias"])
        self.assertEqual(depth.bids_ratio, d["bidsRatio"])
        self.assertEqual(depth.total_volume, d["totalVolume"])

        dt20000101000000 = datetime.strptime("2000-01-01 00:00:00", '%Y-%m-%d %H:%M:%S').astimezone(constants.TIMEZONE)
        depth2 = OrderBookSnapshot(dt20000101000000, bids, asks, 25)
        self.assertEqual(depth.digest_string(), depth2.digest_string())

        self.assertTrue(0 < len(str(depth)))

    def test_order_books_attributes_calculation2(self):

        from bitmex_watcher.utils import constants
        from bitmex_watcher.models import OrderBookSnapshot

        bids = [{"price": 99.0, "size": 200}, {"price": 98.5, "size": 10},
                {"price": 98.0, "size": 250}, {"price": 97.0, "size": 100}]
        asks = [{"price": 100.5, "size": 150}, {"price": 101.0, "size": 100},
                {"price": 102.0, "size": 200}, {"price": 102.5, "size": 150}]
        depth = OrderBookSnapshot(datetime.now().astimezone(constants.TIMEZONE), bids, asks, 0.025)

        self.assertEqual(99.75, depth.mid_price)
        self.assertEqual(3, len(depth.bids))
        self.assertEqual(3, len(depth.asks))
        self.assertEqual(99.8736, depth.price_from_depth)
        self.assertEqual(0.1236, depth.depth_bias)
        self.assertEqual(0.5055, depth.bids_ratio)
        self.assertEqual(910, depth.total_volume)

        d = depth.to_dict()
        self.assertEqual(depth.mid_price, d["midPrice"])
        self.assertEqual(depth.depth_bias, d["depthBias"])
        self.assertEqual(depth.bids_ratio, d["bidsRatio"])
        self.assertEqual(depth.total_volume, d["totalVolume"])

    def test_order_books_attributes_calculation3(self):

        from bitmex_watcher.utils import constants
        from bitmex_watcher.models import OrderBookSnapshot

        bids = [{"price": 99.5, "size": 50}, {"price": 99.0, "size": 200}, {"price": 98.5, "size": 10},
                {"price": 98.0, "size": 250}, {"price": 97.0, "size": 100}]
        asks = [{"price": 100.5, "size": 150}, {"price": 101.0, "size": 100},
                {"price": 102.0, "size": 200}, {"price": 102.5, "size": 150}]
        depth = OrderBookSnapshot(datetime.now().astimezone(constants.TIMEZONE), bids, asks, 0.025)

        self.assertEqual(100.0, depth.mid_price)
        self.assertEqual(4, len(depth.bids))
        self.assertEqual(4, len(depth.asks))
        self.assertEqual(100.0068, depth.price_from_depth)
        self.assertEqual(0.0068, depth.depth_bias)
        self.assertEqual(0.4595, depth.bids_ratio)
        self.assertEqual(1110, depth.total_volume)

        d = depth.to_dict()
        self.assertEqual(depth.mid_price, d["midPrice"])
        self.assertEqual(depth.depth_bias, d["depthBias"])
        self.assertEqual(depth.bids_ratio, d["bidsRatio"])
        self.assertEqual(depth.total_volume, d["totalVolume"])


class TestTrade(unittest.TestCase):

    def test_attributes_calculation1(self):
        from bitmex_watcher.utils import constants
        from bitmex_watcher.models import Trade

        trade = Trade("test_id", datetime.now().astimezone(constants.TIMEZONE), "Buy", 10000.5, 500000)

        self.assertEqual("test_id", trade.trd_match_id)
        self.assertEqual("Buy", trade.side)
        self.assertEqual(10000.5, trade.price)
        self.assertEqual(500000, trade.size)
        self.assertEqual(500000, trade.bought_size)
        self.assertEqual(0, trade.sold_size)

        d = trade.to_dict()
        self.assertEqual(trade.trd_match_id, d["trdMatchID"])
        self.assertEqual(trade.side, d["side"])
        self.assertEqual(trade.price, d["price"])
        self.assertEqual(trade.size, d["size"])
        self.assertEqual(trade.bought_size, d["boughtSize"])
        self.assertEqual(trade.sold_size, d["soldSize"])

        trade2 = Trade("test_id", datetime.now().astimezone(constants.TIMEZONE), "Sell", 11000.5, 100)
        self.assertEqual(100, trade2.size)
        self.assertEqual(0, trade2.bought_size)
        self.assertEqual(100, trade2.sold_size)

        self.assertTrue(0 < len(str(trade)))


class TestTradesCursor(unittest.TestCase):

    def test1(self):
        from bitmex_watcher.utils import constants
        from bitmex_watcher.models import Trade, TradesCursor

        dt20190317115959 = datetime.strptime("2019-03-17 11:59:59", '%Y-%m-%d %H:%M:%S').astimezone(constants.TIMEZONE)
        dt20190317120000 = datetime.strptime("2019-03-17 12:00:00", '%Y-%m-%d %H:%M:%S').astimezone(constants.TIMEZONE)
        dt20190317120001 = datetime.strptime("2019-03-17 12:00:01", '%Y-%m-%d %H:%M:%S').astimezone(constants.TIMEZONE)
        cursor = TradesCursor(dt20190317120000, "x")
        self.assertEqual(dt20190317120000, cursor.to_dict()["timestamp"])
        self.assertEqual("x", cursor.to_dict()["trdMatchID"])

        trade0 = Trade("z", dt20190317115959, "Buy", 3000000, 100)
        self.assertFalse(cursor.is_behind_of(trade0))

        trade1 = Trade("x", dt20190317120000, "Buy", 3000000, 100)
        self.assertFalse(cursor.is_behind_of(trade1))

        trade2 = Trade("y", dt20190317120000, "Buy", 3000000, 100)
        self.assertTrue(cursor.is_behind_of(trade2))

        trade3 = Trade("a", dt20190317120001, "Buy", 3000000, 100)
        self.assertTrue(cursor.is_behind_of(trade3))

        self.assertTrue(0 < len(str(cursor)))
