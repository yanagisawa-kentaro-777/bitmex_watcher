import unittest
from datetime import datetime


class TestOrderBookSnapshot(unittest.TestCase):

    def test_order_books_attributes_calculation1(self):

        from bitmex_watcher.utils import constants
        from bitmex_watcher.models import OrderBookSnapshot

        bids = [{"price": 99.0, "size": 100}, {"price": 98.0, "size": 200}]
        asks = [{"price": 101.0, "size": 10}, {"price": 102.0, "size": 50}]
        depth = OrderBookSnapshot(datetime.now().astimezone(constants.TIMEZONE), bids, asks, 25)

        self.assertEqual(100, depth.mid_price)
        self.assertEqual(0.9167, depth.depth_bias)
        self.assertEqual(0.8333, depth.bids_ratio)
        self.assertEqual(360, depth.total_volume)

        d = depth.to_dict()
        self.assertEqual(depth.mid_price, d["midPrice"])
        self.assertEqual(depth.depth_bias, d["depthBias"])
        self.assertEqual(depth.bids_ratio, d["bidsRatio"])
        self.assertEqual(depth.total_volume, d["totalVolume"])

    def test_2(self):
        self.assertEqual(1, 1)


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

