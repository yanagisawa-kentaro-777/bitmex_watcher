from bitmex_watcher.utils import constants
import hashlib


# Rounding float numbers.
def _round_float(v):
    _num_digits = 4
    return round(v, _num_digits)


###
# A snapshot of order books.
##
class OrderBookSnapshot:

    def __init__(self, _timestamp, _bids, _asks, max_orders_of_side):
        self.timestamp = _timestamp

        self.bids = _bids
        self.asks = _asks

        num_bids = min(max_orders_of_side, len(self.bids))
        num_asks = min(max_orders_of_side, len(self.asks))

        def get_total_volume(orders, count):
            return sum([int(orders[i]["size"]) for i in range(count)])

        def get_highest_price(orders, count):
            return max([float(orders[i]["price"]) for i in range(count)])

        def get_lowest_price(orders, count):
            return min([float(orders[i]["price"]) for i in range(count)])

        self.highest_bid = get_highest_price(self.bids, num_bids)
        self.lowest_bid = get_lowest_price(self.bids, num_bids)
        self.lowest_ask = get_lowest_price(self.asks, num_asks)
        self.highest_ask = get_highest_price(self.asks, num_asks)
        self.mid_price = _round_float(float(self.highest_bid + self.lowest_ask) / 2)

        self.bids_volume = get_total_volume(self.bids, num_bids)
        self.asks_volume = get_total_volume(self.asks, num_asks)
        self.total_volume = self.bids_volume + self.asks_volume

        self.price_from_depth = self.calculate_weighed_average_price_from_orders(num_bids, num_asks, self.mid_price)
        self.depth_bias = _round_float(self.price_from_depth - self.mid_price)
        if 0 < self.total_volume:
            self.bids_ratio = _round_float(float(self.bids_volume) / self.total_volume)
        else:
            self.bids_ratio = -1

    def calculate_weighed_average_price_from_orders(self, _num_bids, _num_asks, _mid_price):
        accum = 0.0
        accum_vol = 0
        end_idx = min(_num_bids, _num_asks)
        for i in range(end_idx):
            # Price is biased high by high and large bids.
            accum += float(self.asks[end_idx - 1 - i]["price"]) * int(self.bids[i]["size"])
            # Price is biased low by low and large asks.
            accum += float(self.bids[end_idx - 1 - i]["price"]) * int(self.asks[i]["size"])
            # Divisor.
            accum_vol += int(self.bids[i]["size"])
            accum_vol += int(self.asks[i]["size"])
        if 0 < accum_vol:
            return _round_float(accum / accum_vol)
        else:
            return _mid_price

    def __str__(self):
        return str(self._to_summary_dict())

    def _to_summary_dict(self):
        return {
            'timestamp': self.timestamp,
            'midPrice': self.mid_price,
            'priceFromDepth': self.price_from_depth,
            'depthBias': self.depth_bias,
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


class Trade:

    def __init__(self, _trd_match_id, _timestamp, _side, _price, _size):
        self.trd_match_id = _trd_match_id
        self.timestamp = _timestamp
        self.side = _side
        self.price = _price
        self.size = _size
        # Redundant fields for the convenience of aggregation.
        if self.side == "Buy":
            self.bought_size = self.size
            self.sold_size = 0
        else:
            self.sold_size = self.size
            self.bought_size = 0

    def __str__(self):
        return str(self.to_dict())

    def to_dict(self):
        return {
            'trdMatchID': self.trd_match_id,
            'timestamp': self.timestamp,
            'side': self.side,
            'price': self.price,
            'size': self.size,
            "boughtSize": self.bought_size,
            "soldSize": self.sold_size
        }


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
        return "(%s : %s)".format(self.timestamp.strftime(constants.DATE_FORMAT), self.trd_match_id)

    def to_dict(self):
        return {
            'timestamp': self.timestamp,
            'trdMatchID': self.trd_match_id
        }
