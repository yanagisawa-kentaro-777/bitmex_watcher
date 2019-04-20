from bitmex_watcher.utils import constants
import math
import hashlib


# Rounding float numbers.
def _round_float(v):
    _num_digits = 4
    return round(v, _num_digits)


###
# A snapshot of order books.
##
class OrderBookSnapshot:

    BOARD_PRICE_INTERVAL = 0.5

    def __init__(self, _timestamp, _bids, _asks, accept_price_range_ratio: float):
        self.timestamp = _timestamp

        self.mid_price = _round_float(float(_bids[0]["price"] + _asks[0]["price"]) / 2)

        self.bids = self.filter_order_books(self.mid_price, _bids, accept_price_range_ratio)
        self.asks = self.filter_order_books(self.mid_price, _asks, accept_price_range_ratio)

        self.highest_bid = self.bids[0]["price"]
        self.lowest_bid = self.bids[-1]["price"]
        self.lowest_ask = self.asks[0]["price"]
        self.highest_ask = self.asks[-1]["price"]

        num_bids = len(self.bids)
        num_asks = len(self.asks)

        def get_total_volume(orders, count):
            return sum([int(orders[i]["size"]) for i in range(count)])

        self.bids_volume = get_total_volume(self.bids, num_bids)
        self.asks_volume = get_total_volume(self.asks, num_asks)
        self.total_volume = self.bids_volume + self.asks_volume

        if 0 < self.total_volume:
            self.bids_ratio = _round_float(float(self.bids_volume) / self.total_volume)
        else:
            self.bids_ratio = -1

    @staticmethod
    def filter_order_books(std_price, order_books, accept_range_ratio):
        allowable_price_diff = std_price * accept_range_ratio
        for i in range(len(order_books)):
            each_order_book = order_books[i]
            if allowable_price_diff < math.fabs(each_order_book["price"] - std_price):
                return order_books[:i]
        return order_books

    def __str__(self):
        return str(self._to_summary_dict())

    def _to_summary_dict(self):
        return {
            'timestamp': self.timestamp,
            'midPrice': self.mid_price,
            'bidsRatio': self.bids_ratio,
            'totalVolume': self.total_volume,

            'highestBid': self.highest_bid,
            'lowestBid': self.lowest_bid,
            'bidsVolume': self.bids_volume,

            'lowestAsk': self.lowest_ask,
            'highestAsk': self.highest_ask,
            'asksVolume': self.asks_volume,

            'bids': self.bids,
            'asks': self.asks
        }

    def to_dict(self):
        result = self._to_summary_dict()
        result.update({'bids': self.bids, 'asks': self.asks})
        return result

    def digest_string(self):
        document = str(self.bids) + str(self.asks)
        return hashlib.sha256(document.encode('utf-8')).hexdigest()


class TradesCursor:

    def __init__(self, _timestamp, _trd_match_id):
        self.timestamp = _timestamp.astimezone(constants.TIMEZONE)
        self.trd_match_id = _trd_match_id

    def is_behind_of(self, trade):
        # The primary sort key is timestamp.
        if self.timestamp < trade.timestamp:
            return True
        if trade.timestamp < self.timestamp:
            return False
        # The secondary sort key is trd_match_id.
        return self.trd_match_id < trade.trd_match_id

    def __str__(self):
        return "({} : {})".format(self.timestamp.strftime(constants.DATE_FORMAT), self.trd_match_id)

    def to_dict(self):
        return {
            'timestamp': self.timestamp,
            'trdMatchID': self.trd_match_id
        }
