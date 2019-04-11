from threading import Lock
from concurrent.futures import ThreadPoolExecutor

import logging
from datetime import timezone
from dateutil.parser import parse

import schedule

from bitmexclient import ws, rest, models


class BitMEXClient:

    def __init__(
            self,
            uri="https://testnet.bitmex.com/api/v1/",
            symbol="XBTUSD",
            api_key=None,
            api_secret=None,
            use_websocket=True,
            use_rest=True,
            subscriptions=None,
            order_id_prefix="",
            agent_name="trading_bot",
            http_timeout=7,
            expiration_seconds=3600,
            ws_refresh_interval_seconds=600
    ):
        self.logger = logging.getLogger(__name__)

        self.uri = uri
        self.symbol = symbol
        self.is_running = True
        if use_websocket:
            self.ws_client0 = ws.BitMEXWebSocketClient(
                endpoint=uri,
                symbol=symbol,
                api_key=api_key,
                api_secret=api_secret,
                subscriptions=subscriptions,
                expiration_seconds=expiration_seconds
            )
            self.ws_client1 = ws.BitMEXWebSocketClient(
                endpoint=uri,
                symbol=symbol,
                api_key=api_key,
                api_secret=api_secret,
                subscriptions=subscriptions,
                expiration_seconds=expiration_seconds
            )
            self.ws_refresh_interval_seconds = ws_refresh_interval_seconds
            self.executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="ws-refresher")
            self.executor.submit(self._schedule_ws_refresh)
        else:
            self.ws_client0 = None
            self.ws_client1 = None
            self.executor = None
        self.ws_lock0 = Lock()
        self.ws_lock1 = Lock()
        self.last_refreshed_ws = 0

        if use_rest:
            self.rest_client = rest.RestClient(
                uri=uri,
                api_key=api_key,
                api_secret=api_secret,
                symbol=symbol,
                order_id_prefix=order_id_prefix,
                agent_name=agent_name,
                timeout=http_timeout,
                expiration_seconds=expiration_seconds
            )
        else:
            self.rest_client = None
        self.order_id_prefix = order_id_prefix

    def _create_ws_client(self):
        return ws.BitMEXWebSocketClient(
            endpoint=self.uri,
            symbol=self.symbol,
            api_key=self.ws_client0.api_key,
            api_secret=self.ws_client0.api_secret,
            subscriptions=self.ws_client0.subscription_list,
            expiration_seconds=self.ws_client0.expiration_seconds
        )

    @staticmethod
    def _close_ws_client(ws_client, ws_lock):
        if ws_client:
            ws_lock.acquire()
            try:
                ws_client.exit()
            finally:
                ws_lock.release()

    def _schedule_ws_refresh(self):
        import time
        schedule.every(self.ws_refresh_interval_seconds).seconds.do(self.refresh_ws_client)
        self.logger.info("WS Refresh task is registered for every %d seconds.", self.ws_refresh_interval_seconds)
        while self.is_running:
            schedule.run_pending()
            time.sleep(1)

    def refresh_ws_client(self):
        try:
            self.logger.info("Executing WS Refresh task.")
            if self.last_refreshed_ws == 0:
                self.ws_lock1.acquire()
                try:
                    self.logger.info("Refreshing ws client 1.")
                    self.ws_client1 = self._create_ws_client()
                    self.last_refreshed_ws = 1
                finally:
                    self.ws_lock1.release()
            else:
                self.ws_lock0.acquire()
                try:
                    self.logger.info("Refreshing ws client 0.")
                    self.ws_client0 = self._create_ws_client()
                    self.last_refreshed_ws = 0
                finally:
                    self.ws_lock0.release()
        except Exception as e:
            import traceback
            import sys
            traceback.print_exc(file=sys.stdout)

            self.logger.error("Error: %s" % str(e))
            self.logger.error(sys.exc_info())

    def close(self):
        self.is_running = False
        self._close_ws_client(self.ws_client0, self.ws_lock0)
        self._close_ws_client(self.ws_client1, self.ws_lock1)
        if self.executor:
            self.executor.shutdown(wait=False)

        if self.rest_client is not None:
            self.rest_client.close()

    def is_market_in_normal_state(self):
        instrument = self.get_instrument()
        state = instrument["state"]
        return state == "Open" or state == "Closed"

    def _get_latest_ws_client(self, table_name):
        locked0 = self.ws_lock0.acquire(blocking=False)
        if not locked0:
            return self.ws_client1
        try:
            locked1 = self.ws_lock1.acquire(blocking=False)
            if not locked1:
                return self.ws_client0
            try:
                time0 = self.ws_client0.updates.get(table_name)
                time1 = self.ws_client1.updates.get(table_name)
                if time0 is not None and time1 is not None:
                    # Both are active. So we compare the latest update time.
                    if time0 < time1:
                        return self.ws_client1
                    else:
                        return self.ws_client0
                elif time0 is None:
                    return self.ws_client1
                elif time1 is None:
                    return self.ws_client0
                else:
                    # Both are inactive. We cannot help.
                    return self.ws_client0
            finally:
                self.ws_lock1.release()
        finally:
            self.ws_lock0.release()

    def get_instrument(self):
        return self._get_latest_ws_client('instrument').get_instrument()

    def order_books(self):
        table_name = self.ws_client0.get_order_book_table_name()
        return self._get_latest_ws_client(table_name).market_depth()

    def recent_trades(self):
        return self._get_latest_ws_client('trade').recent_trades()

    def current_position(self):
        """
        [{'account': XXXXX, 'symbol': 'XBTUSD', 'currency': 'XBt', 'underlying': 'XBT',
         'quoteCurrency': 'USD', 'commission': 0.00075, 'initMarginReq': 0.01,
         'maintMarginReq': 0.005, 'riskLimit': 20000000000, 'leverage': 100, 'crossMargin': True,
        'deleveragePercentile': None, 'rebalancedPnl': 0, 'prevRealisedPnl': 0,
        'prevUnrealisedPnl': 0, 'prevClosePrice': 3972.24,
        'openingTimestamp': '2019-03-25T07:00:00.000Z', 'openingQty': 0, 'openingCost': 0, 'openingComm': 0,
        'openOrderBuyQty': 0, 'openOrderBuyCost': 0, 'openOrderBuyPremium': 0, 'openOrderSellQty': 0,
        'openOrderSellCost': 0, 'openOrderSellPremium': 0, 'execBuyQty': 0,
        'execBuyCost':0, 'execSellQty': 30, 'execSellCost': 756060, 'execQty': -30, 'execCost': 756060,
        'execComm': -189, 'currentTimestamp': '2019-03-25T07:27:06.107Z',
        'currentQty': -30, 'currentCost': 756060, 'currentComm': -189, 'realisedCost': 0,
        'unrealisedCost': 756060, 'grossOpenCost': 0, 'grossOpenPremium': 0,
        'grossExecCost': 756060, 'isOpen': True, 'markPrice': 3964.82, 'markValue': 756660,
        'riskValue': 756660, 'homeNotional': -0.0075666, 'foreignNotional': 30, 'posState': '',
        'posCost': 756060, 'posCost2': 756060, 'posCross': 0, 'posInit': 7561, 'posComm': 573,
        'posLoss': 0, 'posMargin': 8134, 'posMaint': 4712, 'posAllowance': 0,
        'taxableMargin': 0, 'initMargin': 0, 'maintMargin': 8734, 'sessionMargin': 0,
        'targetExcessMargin': 0, 'varMargin': 0, 'realisedGrossPnl': 0, 'realisedTax': 0,
        'realisedPnl': 189, 'unrealisedGrossPnl': 600, 'longBankrupt': 0, 'shortBankrupt': 0,
        'taxBase': 0, 'indicativeTaxRate': None, 'indicativeTax': 0, 'unrealisedTax': 0,
        'unrealisedPnl': 600, 'unrealisedPnlPcnt': 0.0008, 'unrealisedRoePcnt': 0.0794,
        'simpleQty': None, 'simpleCost': None, 'simpleValue': None, 'simplePnl': None,
        'simplePnlPcnt': None, 'avgCostPrice': 3968, 'avgEntryPrice': 3968,
        'breakEvenPrice':3968.5, 'marginCallPrice': 100000000, 'liquidationPrice': 100000000,
        'bankruptPrice': 100000000, 'timestamp': '2019-03-25T07:27:06.107Z', 'lastPrice': 3964.82,
        'lastValue': 756660}]
        """

        json_array = self._get_latest_ws_client('position').positions()
        for each in json_array:
            if each["symbol"] == self.symbol:
                return int(each["currentQty"])
        return 0

    def open_orders(self):
        """
        [{'orderID': '57180f5f-d16a-62d6-ff8d-d1430637a8d9',
        'clOrdID': '', 'clOrdLinkID': '',
        'account': XXXXX, 'symbol': 'XBTUSD', 'side': 'Sell',
        'simpleOrderQty': None,
        'orderQty': 30, 'price': 3968,
        'displayQty': None, 'stopPx': None, 'pegOffsetValue': None,
        'pegPriceType': '', 'currency': 'USD', 'settlCurrency': 'XBt',
        'ordType': 'Limit', 'timeInForce': 'GoodTillCancel',
        'execInst': 'ParticipateDoNotInitiate', 'contingencyType': '',
        'exDestination': 'XBME', 'ordStatus': 'New', 'triggered': '',
        'workingIndicator': True, 'ordRejReason': '', 'simpleLeavesQty': None,
        'leavesQty': 30, 'simpleCumQty': None, 'cumQty': 0, 'avgPx': None,
        'multiLegReportingType': 'SingleSecurity', 'text': 'Submission from www.bitmex.com',
        'transactTime': '2019-03-25T07:10:34.290Z', 'timestamp': '2019-03-25T07:10:34.290Z'}]
        """
        # clOrdID, orderID, side, orderQty, price
        def order_obj_from_json(json):
            return models.OpenOrder(
                json["orderID"], json["clOrdID"],
                json["side"], json["orderQty"], json["price"],
                parse(json["timestamp"]).astimezone(timezone.utc)
            )

        json_array = self._get_latest_ws_client('order').open_orders(self.order_id_prefix)
        bids = [order_obj_from_json(each) for each in json_array if each["side"] == "Buy"]
        asks = [order_obj_from_json(each) for each in json_array if each["side"] == "Sell"]
        return models.OpenOrders(
            bids=sorted(bids, key=lambda o: o.price, reverse=True),
            asks=sorted(asks, key=lambda o: o.price, reverse=False)
        )

    def recent_executions(self):
        """
        [{'execID': '0e14ddd0-702d-7338-82d8-fd4c1a419d03',
        'orderID': '57180f5f-d16a-62d6-ff8d-d1430637a8d9',
        'clOrdID': '', 'clOrdLinkID': '', 'account': XXXXX,
        'symbol':'XBTUSD', 'side': 'Sell', 'lastQty': 30, 'lastPx': 3968,
        'underlyingLastPx': None, 'lastMkt': 'XBME', 'lastLiquidityInd': 'AddedLiquidity',
        'simpleOrderQty': None, 'orderQty': 30, 'price': 3968,
        'displayQty': None, 'stopPx': None, 'pegOffsetValue': None,
        'pegPriceType': '', 'currency': 'USD', 'settlCurrency': 'XBt', 'execType': 'Trade',
        'ordType': 'Limit', 'timeInForce': 'GoodTillCancel', 'execInst': 'ParticipateDoNotInitiate',
        'contingencyType': '', 'exDestination': 'XBME',
        'ordStatus': 'Filled', 'triggered': '', 'workingIndicator': False,
        'ordRejReason': '', 'simpleLeavesQty': None, 'leavesQty': 0, 'simpleCumQty': None, 'cumQty':30,
        'avgPx': 3968, 'commission': -0.00025, 'tradePublishIndicator': 'PublishTrade',
        'multiLegReportingType': 'SingleSecurity', 'text': 'Submission from www.bitmex.com',
        'trdMatchID': '34137715-0068-a923-4685-6dbc70e6d2ac', 'execCost': 756060,
        'execComm': -189, 'homeNotional': -0.0075606, 'foreignNotional': 30,
        'transactTime': '2019-03-25T07:26:06.334Z', 'timestamp': '2019-03-25T07:26:06.334Z'}]
         """
        return self._get_latest_ws_client('execution').executions()

    def balances(self):
        """
        {'account': XXXXX, 'currency': 'XBt', 'riskLimit': 1000000000000, 'prevState': '',
        'state': '', 'action': '', 'amount': 377084143, 'pendingCredit': 0, 'pendingDebit': 0,
        'confirmedDebit': 0, 'prevRealisedPnl': 1038, 'prevUnrealisedPnl': 0, 'grossComm': -567,
        'grossOpenCost': 0, 'grossOpenPremium': 0, 'grossExecCost': 756345, 'grossMarkValue': 756090,
        'riskValue': 756090, 'taxableMargin': 0, 'initMargin': 0, 'maintMargin': 8142,
        'sessionMargin': 0, 'targetExcessMargin': 0, 'varMargin': 0, 'realisedPnl': 1227,
        'unrealisedPnl': -540, 'indicativeTax': 0, 'unrealisedProfit': 0, 'syntheticMargin': None,
        'walletBalance': 377085370, 'marginBalance': 377084830, 'marginBalancePcnt': 1,
        'marginLeverage': 0.0020050925941518254, 'marginUsedPcnt': 0, 'excessMargin': 377076688,
        'excessMarginPcnt': 1, 'availableMargin': 377076688, 'withdrawableMargin': 377076688,
        'timestamp': '2019-03-25T07:56:25.462Z', 'grossLastValue': 756090, 'commission': None}
        """
        satoshis_for_btc = 100000000
        data = self._get_latest_ws_client('margin').funds()
        withdrawable_balance = float(data['withdrawableMargin']) / satoshis_for_btc
        wallet_balance = float(data['walletBalance']) / satoshis_for_btc
        return withdrawable_balance, wallet_balance

    def place_orders(self, new_order_list, post_only=True, max_retries=None):
        if len(new_order_list) == 0:
            return
        self.rest_client.place_orders([o for o in new_order_list], post_only=post_only, max_retries=max_retries)

    def cancel_orders(self, order_id_list, max_retries=None):
        if len(order_id_list) == 0:
            return
        self.rest_client.cancel_orders(order_id_list, max_retries=max_retries)

    def cancel_all_orders(self):
        open_orders = self.open_orders()
        self.cancel_orders([o.order_id for o in open_orders.to_list()])

    def get_trade_history(self, start_time_str, end_time_str, count=500):
        trades = self.rest_client.get_trade_history(start_time_str, end_time_str, count)
        return [t for t in trades if t['symbol'] == self.symbol and t['execType'] == 'Trade']

    def get_trade_history_with_filter_json(self, filter_json_obj, count=500):
        trades = self.rest_client.get_trade_history_with_filter_json(filter_json_obj, count)
        return [t for t in trades if t['symbol'] == self.symbol and t['execType'] == 'Trade']

    def get_user_margin(self):
        return self.rest_client.get_user_margin()