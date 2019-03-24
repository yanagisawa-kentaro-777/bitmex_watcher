from bitmexclient import ws, rest


class BitMEXClient:

    def __init__(
            self,
            uri="https://testnet.bitmex.com/api/v1/",
            symbol= "XBTUSD",
            api_key=None,
            api_secret=None,
            use_websocket=True,
            use_rest=True,
            subscriptions=None
    ):
        self.uri = uri
        self.symbol = symbol
        if use_websocket:
            self.ws_client = ws.BitMEXWebSocketClient(
                "https://www.bitmex.com/api/v1/",
                symbol=symbol,
                api_key=api_key,
                api_secret=api_secret,
                subscriptions=subscriptions
            )
        else:
            self.ws_client = None

        if use_rest:
            self.rest_client = rest.create_rest_client(
                test=(0 <= uri.find("test")),
                api_key=api_key,
                api_secret=api_secret
            )
        else:
            self.rest_client = None

    def close(self):
        if self.ws_client is not None:
            self.ws_client.exit()

    def is_market_in_normal_state(self):
        instrument = self.get_instrument()
        state = instrument["state"]
        return state == "Open" or state == "Closed"

    def get_instrument(self):
        return self.ws_client.get_instrument()

    def order_books(self):
        return self.ws_client.market_depth()

    def recent_trades(self):
        return self.ws_client.recent_trades()
