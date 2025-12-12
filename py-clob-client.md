================================================
FILE: py_clob_client/**init**.py
================================================
from .client import ClobClient
from .clob_types import (
ApiCreds,
OrderArgs,
MarketOrderArgs,
OrderType,
TickSize,
BookParams,
TradeParams,
OpenOrderParams,
BalanceAllowanceParams,
AssetType,
PartialCreateOrderOptions,
CreateOrderOptions,
)

# RFQ exports

from .rfq import (
RfqClient,
RfqUserRequest,
RfqUserQuote,
CreateRfqRequestParams,
CreateRfqQuoteParams,
CancelRfqRequestParams,
CancelRfqQuoteParams,
AcceptQuoteParams,
ApproveOrderParams,
GetRfqRequestsParams,
GetRfqQuotesParams,
GetRfqBestQuoteParams,
RfqRequest,
RfqQuote,
RfqRequestResponse,
RfqQuoteResponse,
RfqPaginatedResponse,
)

**all** = [
# Main client
"ClobClient",
# Core types
"ApiCreds",
"OrderArgs",
"MarketOrderArgs",
"OrderType",
"TickSize",
"BookParams",
"TradeParams",
"OpenOrderParams",
"BalanceAllowanceParams",
"AssetType",
"PartialCreateOrderOptions",
"CreateOrderOptions",
# RFQ client
"RfqClient",
# RFQ input types
"RfqUserRequest",
"RfqUserQuote",
"CreateRfqRequestParams",
"CreateRfqQuoteParams",
"CancelRfqRequestParams",
"CancelRfqQuoteParams",
"AcceptQuoteParams",
"ApproveOrderParams",
"GetRfqRequestsParams",
"GetRfqQuotesParams",
"GetRfqBestQuoteParams",
# RFQ response types
"RfqRequest",
"RfqQuote",
"RfqRequestResponse",
"RfqQuoteResponse",
"RfqPaginatedResponse",
]

================================================
FILE: py_clob_client/client.py
================================================
import logging
import json
from typing import Optional

from py_builder_signing_sdk.config import BuilderConfig

from .order_builder.builder import OrderBuilder
from .headers.headers import (
create_level_1_headers,
create_level_2_headers,
enrich_l2_headers_with_builder_headers,
)
from .signer import Signer
from .config import get_contract_config

from .endpoints import (
CANCEL,
CANCEL_ORDERS,
CANCEL_MARKET_ORDERS,
CANCEL_ALL,
CREATE_API_KEY,
DELETE_API_KEY,
DERIVE_API_KEY,
GET_API_KEYS,
CLOSED_ONLY,
CREATE_READONLY_API_KEY,
GET_READONLY_API_KEYS,
DELETE_READONLY_API_KEY,
VALIDATE_READONLY_API_KEY,
GET_LAST_TRADE_PRICE,
GET_ORDER,
GET_ORDER_BOOK,
MID_POINT,
ORDERS,
POST_ORDER,
POST_ORDERS,
PRICE,
TIME,
TRADES,
GET_NOTIFICATIONS,
DROP_NOTIFICATIONS,
GET_BALANCE_ALLOWANCE,
UPDATE_BALANCE_ALLOWANCE,
IS_ORDER_SCORING,
GET_TICK_SIZE,
GET_NEG_RISK,
GET_FEE_RATE,
ARE_ORDERS_SCORING,
GET_SIMPLIFIED_MARKETS,
GET_MARKETS,
GET_MARKET,
GET_SAMPLING_SIMPLIFIED_MARKETS,
GET_SAMPLING_MARKETS,
GET_MARKET_TRADES_EVENTS,
GET_LAST_TRADES_PRICES,
MID_POINTS,
GET_ORDER_BOOKS,
GET_PRICES,
GET_SPREAD,
GET_SPREADS,
GET_BUILDER_TRADES,
)
from .clob_types import (
ApiCreds,
ReadonlyApiKeyResponse,
TradeParams,
OpenOrderParams,
OrderArgs,
RequestArgs,
DropNotificationParams,
OrderBookSummary,
BalanceAllowanceParams,
OrderScoringParams,
TickSize,
CreateOrderOptions,
OrdersScoringParams,
OrderType,
PartialCreateOrderOptions,
BookParams,
MarketOrderArgs,
PostOrdersArgs,
)
from .exceptions import PolyException
from .http_helpers.helpers import (
add_query_trade_params,
add_query_open_orders_params,
delete,
get,
post,
drop_notifications_query_params,
add_balance_allowance_params_to_url,
add_order_scoring_params_to_url,
)

from .constants import (
L0,
L1,
L1_AUTH_UNAVAILABLE,
L2,
L2_AUTH_UNAVAILABLE,
END_CURSOR,
BUILDER_AUTH_UNAVAILABLE,
)
from .utilities import (
parse_raw_orderbook_summary,
generate_orderbook_summary_hash,
order_to_json,
is_tick_size_smaller,
price_valid,
)
from .rfq import RfqClient

class ClobClient:
def **init**(
self,
host,
chain_id: int = None,
key: str = None,
creds: ApiCreds = None,
signature_type: int = None,
funder: str = None,
builder_config: BuilderConfig = None,
):
"""
Initializes the clob client
The client can be started in 3 modes: 1) Level 0: Requires only the clob host url
Allows access to open CLOB endpoints

        2) Level 1: Requires the host, chain_id and a private key.
                    Allows access to L1 authenticated endpoints + all unauthenticated endpoints

        3) Level 2: Requires the host, chain_id, a private key, and Credentials.
                    Allows access to all endpoints
        """
        self.host = host[0:-1] if host.endswith("/") else host
        self.chain_id = chain_id
        self.signer = Signer(key, chain_id) if key else None
        self.creds = creds
        self.mode = self._get_client_mode()

        if self.signer:
            self.builder = OrderBuilder(
                self.signer, sig_type=signature_type, funder=funder
            )

        self.builder_config = None
        if builder_config:
            self.builder_config = builder_config

        # local cache
        self.__tick_sizes = {}
        self.__neg_risk = {}
        self.__fee_rates = {}

        # RFQ client
        self.rfq = RfqClient(self)

        self.logger = logging.getLogger(self.__class__.__name__)

    def get_address(self):
        """
        Returns the public address of the signer
        """
        return self.signer.address() if self.signer else None

    def get_collateral_address(self):
        """
        Returns the collateral token address
        """
        contract_config = get_contract_config(self.chain_id)
        if contract_config:
            return contract_config.collateral

    def get_conditional_address(self):
        """
        Returns the conditional token address
        """
        contract_config = get_contract_config(self.chain_id)
        if contract_config:
            return contract_config.conditional_tokens

    def get_exchange_address(self, neg_risk=False):
        """
        Returns the exchange address
        """
        contract_config = get_contract_config(self.chain_id, neg_risk)
        if contract_config:
            return contract_config.exchange

    def get_ok(self):
        """
        Health check: Confirms that the server is up
        Does not need authentication
        """
        return get("{}/".format(self.host))

    def get_server_time(self):
        """
        Returns the current timestamp on the server
        Does not need authentication
        """
        return get("{}{}".format(self.host, TIME))

    def create_api_key(self, nonce: int = None) -> ApiCreds:
        """
        Creates a new CLOB API key for the given
        """
        self.assert_level_1_auth()

        endpoint = "{}{}".format(self.host, CREATE_API_KEY)
        headers = create_level_1_headers(self.signer, nonce)

        creds_raw = post(endpoint, headers=headers)
        try:
            creds = ApiCreds(
                api_key=creds_raw["apiKey"],
                api_secret=creds_raw["secret"],
                api_passphrase=creds_raw["passphrase"],
            )
        except:
            self.logger.error("Couldn't parse created CLOB creds")
            return None
        return creds

    def derive_api_key(self, nonce: int = None) -> ApiCreds:
        """
        Derives an already existing CLOB API key for the given address and nonce
        """
        self.assert_level_1_auth()

        endpoint = "{}{}".format(self.host, DERIVE_API_KEY)
        headers = create_level_1_headers(self.signer, nonce)

        creds_raw = get(endpoint, headers=headers)
        try:
            creds = ApiCreds(
                api_key=creds_raw["apiKey"],
                api_secret=creds_raw["secret"],
                api_passphrase=creds_raw["passphrase"],
            )
        except:
            self.logger.error("Couldn't parse derived CLOB creds")
            return None
        return creds

    def create_or_derive_api_creds(self, nonce: int = None) -> ApiCreds:
        """
        Creates API creds if not already created for nonce, otherwise derives them
        """
        try:
            return self.create_api_key(nonce)
        except:
            return self.derive_api_key(nonce)

    def set_api_creds(self, creds: ApiCreds):
        """
        Sets client api creds
        """
        self.creds = creds
        self.mode = self._get_client_mode()

    def get_api_keys(self):
        """
        Gets the available API keys for this address
        Level 2 Auth required
        """
        self.assert_level_2_auth()

        request_args = RequestArgs(method="GET", request_path=GET_API_KEYS)
        headers = create_level_2_headers(self.signer, self.creds, request_args)
        return get("{}{}".format(self.host, GET_API_KEYS), headers=headers)

    def get_closed_only_mode(self):
        """
        Gets the closed only mode flag for thsi address
        Level 2 Auth required
        """
        self.assert_level_2_auth()

        request_args = RequestArgs(method="GET", request_path=CLOSED_ONLY)
        headers = create_level_2_headers(self.signer, self.creds, request_args)
        return get("{}{}".format(self.host, CLOSED_ONLY), headers=headers)

    def delete_api_key(self):
        """
        Deletes an API key
        Level 2 Auth required
        """
        self.assert_level_2_auth()

        request_args = RequestArgs(method="DELETE", request_path=DELETE_API_KEY)
        headers = create_level_2_headers(self.signer, self.creds, request_args)
        return delete("{}{}".format(self.host, DELETE_API_KEY), headers=headers)

    def create_readonly_api_key(self) -> ReadonlyApiKeyResponse:
        """
        Creates a new readonly API key for a user
        Level 2 Auth required
        """
        self.assert_level_2_auth()

        request_args = RequestArgs(method="POST", request_path=CREATE_READONLY_API_KEY)
        headers = create_level_2_headers(self.signer, self.creds, request_args)

        response = post("{}{}".format(self.host, CREATE_READONLY_API_KEY), headers=headers)
        try:
            return ReadonlyApiKeyResponse(api_key=response["apiKey"])
        except:
            self.logger.error("Couldn't parse readonly API key response")
            return None

    def get_readonly_api_keys(self) -> list[str]:
        """
        Gets the available readonly API keys for this address
        Level 2 Auth required
        """
        self.assert_level_2_auth()

        request_args = RequestArgs(method="GET", request_path=GET_READONLY_API_KEYS)
        headers = create_level_2_headers(self.signer, self.creds, request_args)
        return get("{}{}".format(self.host, GET_READONLY_API_KEYS), headers=headers)

    def delete_readonly_api_key(self, key: str) -> bool:
        """
        Deletes a readonly API key for a user
        Level 2 Auth required
        """
        self.assert_level_2_auth()

        body = {"key": key}
        serialized = json.dumps(body, separators=(",", ":"), ensure_ascii=False)
        request_args = RequestArgs(
            method="DELETE",
            request_path=DELETE_READONLY_API_KEY,
            body=body,
            serialized_body=serialized,
        )
        headers = create_level_2_headers(self.signer, self.creds, request_args)
        return delete(
            "{}{}".format(self.host, DELETE_READONLY_API_KEY),
            headers=headers,
            data=serialized,
        )

    def validate_readonly_api_key(self, address: str, key: str) -> str:
        """
        Validates a readonly API key for a given address
        This is a public endpoint, no authentication required
        """
        return get(
            "{}{}?address={}&key={}".format(
                self.host, VALIDATE_READONLY_API_KEY, address, key
            )
        )

    def get_midpoint(self, token_id):
        """
        Get the mid market price for the given market
        """
        return get("{}{}?token_id={}".format(self.host, MID_POINT, token_id))

    def get_midpoints(self, params: list[BookParams]):
        """
        Get the mid market prices for a set of token ids
        """
        body = [{"token_id": param.token_id} for param in params]
        return post("{}{}".format(self.host, MID_POINTS), data=body)

    def get_price(self, token_id, side):
        """
        Get the market price for the given market
        """
        return get("{}{}?token_id={}&side={}".format(self.host, PRICE, token_id, side))

    def get_prices(self, params: list[BookParams]):
        """
        Get the market prices for a set
        """
        body = [{"token_id": param.token_id, "side": param.side} for param in params]
        return post("{}{}".format(self.host, GET_PRICES), data=body)

    def get_spread(self, token_id):
        """
        Get the spread for the given market
        """
        return get("{}{}?token_id={}".format(self.host, GET_SPREAD, token_id))

    def get_spreads(self, params: list[BookParams]):
        """
        Get the spreads for a set of token ids
        """
        body = [{"token_id": param.token_id} for param in params]
        return post("{}{}".format(self.host, GET_SPREADS), data=body)

    def get_tick_size(self, token_id: str) -> TickSize:
        if token_id in self.__tick_sizes:
            return self.__tick_sizes[token_id]

        result = get("{}{}?token_id={}".format(self.host, GET_TICK_SIZE, token_id))
        self.__tick_sizes[token_id] = str(result["minimum_tick_size"])

        return self.__tick_sizes[token_id]

    def get_neg_risk(self, token_id: str) -> bool:
        if token_id in self.__neg_risk:
            return self.__neg_risk[token_id]

        result = get("{}{}?token_id={}".format(self.host, GET_NEG_RISK, token_id))
        self.__neg_risk[token_id] = result["neg_risk"]

        return result["neg_risk"]

    def get_fee_rate_bps(self, token_id: str) -> int:
        if token_id in self.__fee_rates:
            return self.__fee_rates[token_id]

        result = get("{}{}?token_id={}".format(self.host, GET_FEE_RATE, token_id))
        fee_rate = result.get("base_fee") or 0
        self.__fee_rates[token_id] = fee_rate

        return fee_rate

    def __resolve_tick_size(
        self, token_id: str, tick_size: TickSize = None
    ) -> TickSize:
        min_tick_size = self.get_tick_size(token_id)
        if tick_size is not None:
            if is_tick_size_smaller(tick_size, min_tick_size):
                raise Exception(
                    "invalid tick size ("
                    + str(tick_size)
                    + "), minimum for the market is "
                    + str(min_tick_size),
                )
        else:
            tick_size = min_tick_size
        return tick_size

    def __resolve_fee_rate(self, token_id: str, user_fee_rate: int = None) -> int:
        market_fee_rate_bps = self.get_fee_rate_bps(token_id)
        # If both fee rate on the market and the user supplied fee rate are non-zero, validate that they match
        # else return the market fee rate
        if (
            market_fee_rate_bps is not None
            and market_fee_rate_bps > 0
            and user_fee_rate is not None
            and user_fee_rate > 0
            and user_fee_rate != market_fee_rate_bps
        ):
            raise Exception(
                f"invalid user provided fee rate: ({user_fee_rate}), fee rate for the market must be {market_fee_rate_bps}"
            )
        return market_fee_rate_bps

    def create_order(
        self, order_args: OrderArgs, options: Optional[PartialCreateOrderOptions] = None
    ):
        """
        Creates and signs an order
        Level 1 Auth required
        """
        self.assert_level_1_auth()

        # add resolve_order_options, or similar
        tick_size = self.__resolve_tick_size(
            order_args.token_id,
            options.tick_size if options else None,
        )

        if not price_valid(order_args.price, tick_size):
            raise Exception(
                "price ("
                + str(order_args.price)
                + "), min: "
                + str(tick_size)
                + " - max: "
                + str(1 - float(tick_size))
            )

        neg_risk = (
            options.neg_risk
            if options and options.neg_risk
            else self.get_neg_risk(order_args.token_id)
        )

        # fee rate
        fee_rate_bps = self.__resolve_fee_rate(
            order_args.token_id, order_args.fee_rate_bps
        )
        order_args.fee_rate_bps = fee_rate_bps

        return self.builder.create_order(
            order_args,
            CreateOrderOptions(
                tick_size=tick_size,
                neg_risk=neg_risk,
            ),
        )

    def create_market_order(
        self,
        order_args: MarketOrderArgs,
        options: Optional[PartialCreateOrderOptions] = None,
    ):
        """
        Creates and signs an order
        Level 1 Auth required
        """
        self.assert_level_1_auth()

        # add resolve_order_options, or similar
        tick_size = self.__resolve_tick_size(
            order_args.token_id,
            options.tick_size if options else None,
        )

        if order_args.price is None or order_args.price <= 0:
            order_args.price = self.calculate_market_price(
                order_args.token_id,
                order_args.side,
                order_args.amount,
                order_args.order_type,
            )

        if not price_valid(order_args.price, tick_size):
            raise Exception(
                "price ("
                + str(order_args.price)
                + "), min: "
                + str(tick_size)
                + " - max: "
                + str(1 - float(tick_size))
            )

        neg_risk = (
            options.neg_risk
            if options and options.neg_risk
            else self.get_neg_risk(order_args.token_id)
        )

        # fee rate
        fee_rate_bps = self.__resolve_fee_rate(
            order_args.token_id, order_args.fee_rate_bps
        )
        order_args.fee_rate_bps = fee_rate_bps

        return self.builder.create_market_order(
            order_args,
            CreateOrderOptions(
                tick_size=tick_size,
                neg_risk=neg_risk,
            ),
        )

    def post_orders(self, args: list[PostOrdersArgs]):
        """
        Posts orders
        """
        self.assert_level_2_auth()
        body = [
            order_to_json(arg.order, self.creds.api_key, arg.orderType) for arg in args
        ]
        request_args = RequestArgs(
            method="POST",
            request_path=POST_ORDERS,
            body=body,
            serialized_body=json.dumps(body, separators=(",", ":"), ensure_ascii=False),
        )
        headers = create_level_2_headers(self.signer, self.creds, request_args)
        # Builder flow
        if self.can_builder_auth():
            builder_headers = self._generate_builder_headers(request_args, headers)
            if builder_headers is not None:
                return post(
                    "{}{}".format(self.host, POST_ORDERS),
                    headers=builder_headers,
                    data=request_args.serialized_body,
                )
        # send exact serialized bytes
        return post(
            "{}{}".format(self.host, POST_ORDERS),
            headers=headers,
            data=request_args.serialized_body,
        )

    def post_order(self, order, orderType: OrderType = OrderType.GTC):
        """
        Posts the order
        """
        self.assert_level_2_auth()
        body = order_to_json(order, self.creds.api_key, orderType)
        request_args = RequestArgs(
            method="POST",
            request_path=POST_ORDER,
            body=body,
            serialized_body=json.dumps(body, separators=(",", ":"), ensure_ascii=False),
        )
        headers = create_level_2_headers(self.signer, self.creds, request_args)
        # Builder flow
        if self.can_builder_auth():
            builder_headers = self._generate_builder_headers(request_args, headers)
            if builder_headers is not None:
                return post(
                    "{}{}".format(self.host, POST_ORDER),
                    headers=builder_headers,
                    data=request_args.serialized_body,
                )
        return post(
            "{}{}".format(self.host, POST_ORDER),
            headers=headers,
            data=request_args.serialized_body,
        )

    def create_and_post_order(
        self, order_args: OrderArgs, options: PartialCreateOrderOptions = None
    ):
        """
        Utility function to create and publish an order
        """
        ord = self.create_order(order_args, options)
        return self.post_order(ord)

    def cancel(self, order_id):
        """
        Cancels an order
        Level 2 Auth required
        """
        self.assert_level_2_auth()
        body = {"orderID": order_id}

        request_args = RequestArgs(
            method="DELETE",
            request_path=CANCEL,
            body=body,
            serialized_body=json.dumps(body, separators=(",", ":"), ensure_ascii=False),
        )
        headers = create_level_2_headers(self.signer, self.creds, request_args)
        return delete(
            "{}{}".format(self.host, CANCEL),
            headers=headers,
            data=request_args.serialized_body,
        )

    def cancel_orders(self, order_ids):
        """
        Cancels orders
        Level 2 Auth required
        """
        self.assert_level_2_auth()
        body = order_ids
        serialized = json.dumps(body, separators=(",", ":"), ensure_ascii=False)
        request_args = RequestArgs(
            method="DELETE",
            request_path=CANCEL_ORDERS,
            body=body,
            serialized_body=serialized,
        )
        headers = create_level_2_headers(self.signer, self.creds, request_args)
        return delete(
            "{}{}".format(self.host, CANCEL_ORDERS), headers=headers, data=serialized
        )

    def cancel_all(self):
        """
        Cancels all available orders for the user
        Level 2 Auth required
        """
        self.assert_level_2_auth()
        request_args = RequestArgs(method="DELETE", request_path=CANCEL_ALL)
        headers = create_level_2_headers(self.signer, self.creds, request_args)
        return delete("{}{}".format(self.host, CANCEL_ALL), headers=headers)

    def cancel_market_orders(self, market: str = "", asset_id: str = ""):
        """
        Cancels orders
        Level 2 Auth required
        """
        self.assert_level_2_auth()
        body = {"market": market, "asset_id": asset_id}
        serialized = json.dumps(body, separators=(",", ":"), ensure_ascii=False)
        request_args = RequestArgs(
            method="DELETE",
            request_path=CANCEL_MARKET_ORDERS,
            body=body,
            serialized_body=serialized,
        )
        headers = create_level_2_headers(self.signer, self.creds, request_args)
        return delete(
            "{}{}".format(self.host, CANCEL_MARKET_ORDERS),
            headers=headers,
            data=serialized,
        )

    def get_orders(self, params: OpenOrderParams = None, next_cursor="MA=="):
        """
        Gets orders for the API key
        Requires Level 2 authentication
        """
        self.assert_level_2_auth()
        request_args = RequestArgs(method="GET", request_path=ORDERS)
        headers = create_level_2_headers(self.signer, self.creds, request_args)

        results = []
        next_cursor = next_cursor if next_cursor is not None else "MA=="
        while next_cursor != END_CURSOR:
            url = add_query_open_orders_params(
                "{}{}".format(self.host, ORDERS), params, next_cursor
            )
            response = get(url, headers=headers)
            next_cursor = response["next_cursor"]
            results += response["data"]

        return results

    def get_order_book(self, token_id) -> OrderBookSummary:
        """
        Fetches the orderbook for the token_id
        """
        raw_obs = get("{}{}?token_id={}".format(self.host, GET_ORDER_BOOK, token_id))
        return parse_raw_orderbook_summary(raw_obs)

    def get_order_books(self, params: list[BookParams]) -> list[OrderBookSummary]:
        """
        Fetches the orderbook for a set of token ids
        """
        body = [{"token_id": param.token_id} for param in params]
        raw_obs = post("{}{}".format(self.host, GET_ORDER_BOOKS), data=body)
        return [parse_raw_orderbook_summary(r) for r in raw_obs]

    def get_order_book_hash(self, orderbook: OrderBookSummary) -> str:
        """
        Calculates the hash for the given orderbook
        """
        return generate_orderbook_summary_hash(orderbook)

    def get_order(self, order_id):
        """
        Fetches the order corresponding to the order_id
        Requires Level 2 authentication
        """
        self.assert_level_2_auth()
        endpoint = "{}{}".format(GET_ORDER, order_id)
        request_args = RequestArgs(method="GET", request_path=endpoint)
        headers = create_level_2_headers(self.signer, self.creds, request_args)
        return get("{}{}".format(self.host, endpoint), headers=headers)

    def get_trades(self, params: TradeParams = None, next_cursor="MA=="):
        """
        Fetches the trade history for a user
        Requires Level 2 authentication
        """
        self.assert_level_2_auth()
        request_args = RequestArgs(method="GET", request_path=TRADES)
        headers = create_level_2_headers(self.signer, self.creds, request_args)

        results = []
        next_cursor = next_cursor if next_cursor is not None else "MA=="
        while next_cursor != END_CURSOR:
            url = add_query_trade_params(
                "{}{}".format(self.host, TRADES), params, next_cursor
            )
            response = get(url, headers=headers)
            next_cursor = response["next_cursor"]
            results += response["data"]

        return results

    def get_last_trade_price(self, token_id):
        """
        Fetches the last trade price token_id
        """
        return get("{}{}?token_id={}".format(self.host, GET_LAST_TRADE_PRICE, token_id))

    def get_last_trades_prices(self, params: list[BookParams]):
        """
        Fetches the last trades prices for a set of token ids
        """
        body = [{"token_id": param.token_id} for param in params]
        return post("{}{}".format(self.host, GET_LAST_TRADES_PRICES), data=body)

    def assert_level_1_auth(self):
        """
        Level 1 Poly Auth
        """
        if self.mode < L1:
            raise PolyException(L1_AUTH_UNAVAILABLE)

    def assert_level_2_auth(self):
        """
        Level 2 Poly Auth
        """
        if self.mode < L2:
            raise PolyException(L2_AUTH_UNAVAILABLE)

    def assert_builder_auth(self):
        """
        Builder Auth
        """
        if not self.can_builder_auth():
            raise PolyException(BUILDER_AUTH_UNAVAILABLE)

    def can_builder_auth(self) -> bool:
        return self.builder_config is not None and self.builder_config.is_valid()

    def _get_client_mode(self):
        if self.signer is not None and self.creds is not None:
            return L2
        if self.signer is not None:
            return L1
        return L0

    def _generate_builder_headers(self, request_args: RequestArgs, headers: dict):
        """
        Generates builder headers and attaches them to the L2 Header
        """
        if self.builder_config is not None:
            builder_headers = self._get_builder_headers(
                request_args.method,
                request_args.request_path,
                request_args.serialized_body,
            )
            if builder_headers is None:
                return None
            return enrich_l2_headers_with_builder_headers(headers, builder_headers)
        return None

    def _get_builder_headers(self, method: str, path: str, body: Optional[str] = None):
        """
        Generates builder headers for the given method, path, and body.

        Args:
            method (str): HTTP method.
            path (str): Request path.
            body (Optional[str]): Pre-serialized JSON string or None.

        Returns:
            dict or None: Builder headers as a dictionary, or None if not available.
        """
        headers = self.builder_config.generate_builder_headers(method, path, body)
        if headers:
            return headers.to_dict()
        return None

    def get_notifications(self):
        """
        Fetches the notifications for a user
        Requires Level 2 authentication
        """
        self.assert_level_2_auth()
        request_args = RequestArgs(method="GET", request_path=GET_NOTIFICATIONS)
        headers = create_level_2_headers(self.signer, self.creds, request_args)
        url = "{}{}?signature_type={}".format(
            self.host, GET_NOTIFICATIONS, self.builder.sig_type
        )
        return get(url, headers=headers)

    def drop_notifications(self, params: DropNotificationParams = None):
        """
        Drops the notifications for a user
        Requires Level 2 authentication
        """
        self.assert_level_2_auth()
        request_args = RequestArgs(method="DELETE", request_path=DROP_NOTIFICATIONS)
        headers = create_level_2_headers(self.signer, self.creds, request_args)
        url = drop_notifications_query_params(
            "{}{}".format(self.host, DROP_NOTIFICATIONS), params
        )
        return delete(url, headers=headers)

    def get_balance_allowance(self, params: BalanceAllowanceParams = None):
        """
        Fetches the balance & allowance for a user
        Requires Level 2 authentication
        """
        self.assert_level_2_auth()
        request_args = RequestArgs(method="GET", request_path=GET_BALANCE_ALLOWANCE)
        headers = create_level_2_headers(self.signer, self.creds, request_args)
        if params.signature_type == -1:
            params.signature_type = self.builder.sig_type
        url = add_balance_allowance_params_to_url(
            "{}{}".format(self.host, GET_BALANCE_ALLOWANCE), params
        )
        return get(url, headers=headers)

    def update_balance_allowance(self, params: BalanceAllowanceParams = None):
        """
        Updates the balance & allowance for a user
        Requires Level 2 authentication
        """
        self.assert_level_2_auth()
        request_args = RequestArgs(method="GET", request_path=UPDATE_BALANCE_ALLOWANCE)
        headers = create_level_2_headers(self.signer, self.creds, request_args)
        if params.signature_type == -1:
            params.signature_type = self.builder.sig_type
        url = add_balance_allowance_params_to_url(
            "{}{}".format(self.host, UPDATE_BALANCE_ALLOWANCE), params
        )
        return get(url, headers=headers)

    def is_order_scoring(self, params: OrderScoringParams):
        """
        Check if the order is currently scoring
        Requires Level 2 authentication
        """
        self.assert_level_2_auth()
        request_args = RequestArgs(method="GET", request_path=IS_ORDER_SCORING)
        headers = create_level_2_headers(self.signer, self.creds, request_args)
        url = add_order_scoring_params_to_url(
            "{}{}".format(self.host, IS_ORDER_SCORING), params
        )
        return get(url, headers=headers)

    def are_orders_scoring(self, params: OrdersScoringParams):
        """
        Check if the orders are currently scoring
        Requires Level 2 authentication
        """
        self.assert_level_2_auth()
        body = params.orderIds
        serialized = json.dumps(body, separators=(",", ":"), ensure_ascii=False)
        request_args = RequestArgs(
            method="POST",
            request_path=ARE_ORDERS_SCORING,
            body=body,
            serialized_body=serialized,
        )
        headers = create_level_2_headers(self.signer, self.creds, request_args)
        return post(
            "{}{}".format(self.host, ARE_ORDERS_SCORING),
            headers=headers,
            data=serialized,
        )

    def get_sampling_markets(self, next_cursor="MA=="):
        """
        Get the current sampling markets
        """
        return get(
            "{}{}?next_cursor={}".format(self.host, GET_SAMPLING_MARKETS, next_cursor)
        )

    def get_sampling_simplified_markets(self, next_cursor="MA=="):
        """
        Get the current sampling simplified markets
        """
        return get(
            "{}{}?next_cursor={}".format(
                self.host, GET_SAMPLING_SIMPLIFIED_MARKETS, next_cursor
            )
        )

    def get_markets(self, next_cursor="MA=="):
        """
        Get the current markets
        """
        return get("{}{}?next_cursor={}".format(self.host, GET_MARKETS, next_cursor))

    def get_simplified_markets(self, next_cursor="MA=="):
        """
        Get the current simplified markets
        """
        return get(
            "{}{}?next_cursor={}".format(self.host, GET_SIMPLIFIED_MARKETS, next_cursor)
        )

    def get_market(self, condition_id):
        """
        Get a market by condition_id
        """
        return get("{}{}{}".format(self.host, GET_MARKET, condition_id))

    def get_market_trades_events(self, condition_id):
        """
        Get the market's trades events by condition id
        """
        return get("{}{}{}".format(self.host, GET_MARKET_TRADES_EVENTS, condition_id))

    def get_builder_trades(self, params: TradeParams = None, next_cursor="MA=="):
        """
        Get trades originated by the builder
        """
        self.assert_builder_auth()

        request_args = RequestArgs(method="GET", request_path=GET_BUILDER_TRADES)
        headers = self._get_builder_headers(
            request_args.method, request_args.request_path, request_args.body
        )

        results = []
        next_cursor = next_cursor if next_cursor is not None else "MA=="
        while next_cursor != END_CURSOR:
            url = add_query_trade_params(
                "{}{}".format(self.host, GET_BUILDER_TRADES), params, next_cursor
            )
            response = get(url, headers=headers)
            next_cursor = response["next_cursor"]
            results += response["data"]

        return results

    def calculate_market_price(
        self, token_id: str, side: str, amount: float, order_type: OrderType
    ) -> float:
        """
        Calculates the matching price considering an amount and the current orderbook
        """
        book = self.get_order_book(token_id)
        if book is None:
            raise Exception("no orderbook")
        if side == "BUY":
            if book.asks is None:
                raise Exception("no match")
            return self.builder.calculate_buy_market_price(
                book.asks, amount, order_type
            )
        else:
            if book.bids is None:
                raise Exception("no match")
            return self.builder.calculate_sell_market_price(
                book.bids, amount, order_type
            )

================================================
FILE: py_clob_client/clob_types.py
================================================
from typing import Any
from dataclasses import dataclass, asdict
from json import dumps
from typing import Literal, Optional
from py_order_utils.model import (
SignedOrder,
)

from .constants import ZERO_ADDRESS

class OrderType(enumerate):
GTC = "GTC"
FOK = "FOK"
GTD = "GTD"
FAK = "FAK"

@dataclass
class ApiCreds:
api_key: str
api_secret: str
api_passphrase: str

@dataclass
class ReadonlyApiKeyResponse:
api_key: str

@dataclass
class RequestArgs:
method: str
request_path: str
body: Any = None
serialized_body: Optional[str] = None

@dataclass
class BookParams:
token_id: str
side: str = ""

@dataclass
class OrderArgs:
token_id: str
"""
TokenID of the Conditional token asset being traded
"""

    price: float
    """
    Price used to create the order
    """

    size: float
    """
    Size in terms of the ConditionalToken
    """

    side: str
    """
    Side of the order
    """

    fee_rate_bps: int = 0
    """
    Fee rate, in basis points, charged to the order maker, charged on proceeds
    """

    nonce: int = 0
    """
    Nonce used for onchain cancellations
    """

    expiration: int = 0
    """
    Timestamp after which the order is expired.
    """

    taker: str = ZERO_ADDRESS
    """
    Address of the order taker. The zero address is used to indicate a public order
    """

@dataclass
class MarketOrderArgs:
token_id: str
"""
TokenID of the Conditional token asset being traded
"""

    amount: float
    """
    BUY orders: $$$ Amount to buy
    SELL orders: Shares to sell
    """

    side: str
    """
    Side of the order
    """

    price: float = 0
    """
    Price used to create the order
    """

    fee_rate_bps: int = 0
    """
    Fee rate, in basis points, charged to the order maker, charged on proceeds
    """

    nonce: int = 0
    """
    Nonce used for onchain cancellations
    """

    taker: str = ZERO_ADDRESS
    """
    Address of the order taker. The zero address is used to indicate a public order
    """

    order_type: OrderType = OrderType.FOK

@dataclass
class TradeParams:
id: str = None
maker_address: str = None
market: str = None
asset_id: str = None
before: int = None
after: int = None

@dataclass
class OpenOrderParams:
id: str = None
market: str = None
asset_id: str = None

@dataclass
class DropNotificationParams:
ids: list[str] = None

@dataclass
class OrderSummary:
price: str = None
size: str = None

    @property
    def __dict__(self):
        return asdict(self)

    @property
    def json(self):
        return dumps(self.__dict__)

@dataclass
class OrderBookSummary:
market: str = None
asset_id: str = None
timestamp: str = None
bids: list[OrderSummary] = None
asks: list[OrderSummary] = None
min_order_size: str = None
neg_risk: bool = None
tick_size: str = None
hash: str = None

    @property
    def __dict__(self):
        return asdict(self)

    @property
    def json(self):
        return dumps(self.__dict__, separators=(",", ":"))

class AssetType(enumerate):
COLLATERAL = "COLLATERAL"
CONDITIONAL = "CONDITIONAL"

@dataclass
class BalanceAllowanceParams:
asset_type: AssetType = None
token_id: str = None
signature_type: int = -1

@dataclass
class OrderScoringParams:
orderId: str

@dataclass
class OrdersScoringParams:
orderIds: list[str]

TickSize = Literal["0.1", "0.01", "0.001", "0.0001"]

@dataclass
class CreateOrderOptions:
tick_size: TickSize
neg_risk: bool

@dataclass
class PartialCreateOrderOptions:
tick_size: Optional[TickSize] = None
neg_risk: Optional[bool] = None

@dataclass
class RoundConfig:
price: float
size: float
amount: float

@dataclass
class ContractConfig:
"""
Contract Configuration
"""

    exchange: str
    """
    The exchange contract responsible for matching orders
    """

    collateral: str
    """
    The ERC20 token used as collateral for the exchange's markets
    """

    conditional_tokens: str
    """
    The ERC1155 conditional tokens contract
    """

@dataclass
class PostOrdersArgs:
order: SignedOrder
orderType: OrderType = OrderType.GTC

================================================
FILE: py_clob_client/config.py
================================================
from .clob_types import ContractConfig

def get_contract_config(chainID: int, neg_risk: bool = False) -> ContractConfig:
"""
Get the contract configuration for the chain
"""

    CONFIG = {
        137: ContractConfig(
            exchange="0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E",
            collateral="0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",
            conditional_tokens="0x4D97DCd97eC945f40cF65F87097ACe5EA0476045",
        ),
        80002: ContractConfig(
            exchange="0xdFE02Eb6733538f8Ea35D585af8DE5958AD99E40",
            collateral="0x9c4e1703476e875070ee25b56a58b008cfb8fa78",
            conditional_tokens="0x69308FB512518e39F9b16112fA8d994F4e2Bf8bB",
        ),
    }

    NEG_RISK_CONFIG = {
        137: ContractConfig(
            exchange="0xC5d563A36AE78145C45a50134d48A1215220f80a",
            collateral="0x2791bca1f2de4661ed88a30c99a7a9449aa84174",
            conditional_tokens="0x4D97DCd97eC945f40cF65F87097ACe5EA0476045",
        ),
        80002: ContractConfig(
            exchange="0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296",
            collateral="0x9c4e1703476e875070ee25b56a58b008cfb8fa78",
            conditional_tokens="0x69308FB512518e39F9b16112fA8d994F4e2Bf8bB",
        ),
    }

    if neg_risk:
        config = NEG_RISK_CONFIG.get(chainID)
    else:
        config = CONFIG.get(chainID)
    if config is None:
        raise Exception("Invalid chainID: ${}".format(chainID))

    return config

================================================
FILE: py_clob_client/constants.py
================================================

# Access levels

L0 = 0
L1 = 1
L2 = 2

CREDENTIAL_CREATION_WARNING = """🚨🚨🚨
Your credentials CANNOT be recovered after they've been created.
Be sure to store them safely!
🚨🚨🚨"""

L1_AUTH_UNAVAILABLE = "A private key is needed to interact with this endpoint!"

L2_AUTH_UNAVAILABLE = "API Credentials are needed to interact with this endpoint!"

BUILDER_AUTH_UNAVAILABLE = (
"Builder API Credentials needed to interact with this endpoint!"
)

ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"

AMOY = 80002
POLYGON = 137

END_CURSOR = "LTE="

================================================
FILE: py_clob_client/endpoints.py
================================================
TIME = "/time"
CREATE_API_KEY = "/auth/api-key"
GET_API_KEYS = "/auth/api-keys"
DELETE_API_KEY = "/auth/api-key"
DERIVE_API_KEY = "/auth/derive-api-key"
CLOSED_ONLY = "/auth/ban-status/closed-only"

# Readonly API Key endpoints

CREATE_READONLY_API_KEY = "/auth/readonly-api-key"
GET_READONLY_API_KEYS = "/auth/readonly-api-keys"
DELETE_READONLY_API_KEY = "/auth/readonly-api-key"
VALIDATE_READONLY_API_KEY = "/auth/validate-readonly-api-key"

TRADES = "/data/trades"
GET_ORDER_BOOK = "/book"
GET_ORDER_BOOKS = "/books"
GET_ORDER = "/data/order/"
ORDERS = "/data/orders"
POST_ORDER = "/order"
POST_ORDERS = "/orders"
CANCEL = "/order"
CANCEL_ORDERS = "/orders"
CANCEL_ALL = "/cancel-all"
CANCEL_MARKET_ORDERS = "/cancel-market-orders"
MID_POINT = "/midpoint"
MID_POINTS = "/midpoints"
PRICE = "/price"
GET_PRICES = "/prices"
GET_SPREAD = "/spread"
GET_SPREADS = "/spreads"
GET_LAST_TRADE_PRICE = "/last-trade-price"
GET_LAST_TRADES_PRICES = "/last-trades-prices"
GET_NOTIFICATIONS = "/notifications"
DROP_NOTIFICATIONS = "/notifications"
GET_BALANCE_ALLOWANCE = "/balance-allowance"
UPDATE_BALANCE_ALLOWANCE = "/balance-allowance/update"
IS_ORDER_SCORING = "/order-scoring"
ARE_ORDERS_SCORING = "/orders-scoring"
GET_TICK_SIZE = "/tick-size"
GET_NEG_RISK = "/neg-risk"
GET_FEE_RATE = "/fee-rate"
GET_SAMPLING_SIMPLIFIED_MARKETS = "/sampling-simplified-markets"
GET_SAMPLING_MARKETS = "/sampling-markets"
GET_SIMPLIFIED_MARKETS = "/simplified-markets"
GET_MARKETS = "/markets"
GET_MARKET = "/markets/"
GET_MARKET_TRADES_EVENTS = "/live-activity/events/"

GET_BUILDER_TRADES = "/builder/trades"

# RFQ Endpoints

CREATE_RFQ_REQUEST = "/rfq/request"
CANCEL_RFQ_REQUEST = "/rfq/request"
GET_RFQ_REQUESTS = "/rfq/data/requests"
CREATE_RFQ_QUOTE = "/rfq/quote"
CANCEL_RFQ_QUOTE = "/rfq/quote"
GET_RFQ_QUOTES = "/rfq/data/quotes"
GET_RFQ_BEST_QUOTE = "/rfq/data/best-quote"
RFQ_REQUESTS_ACCEPT = "/rfq/request/accept"
RFQ_QUOTE_APPROVE = "/rfq/quote/approve"
RFQ_CONFIG = "/rfq/config"

================================================
FILE: py_clob_client/exceptions.py
================================================
from typing import Optional

import httpx

class PolyException(Exception):
def **init**(self, msg):
self.msg = msg

class PolyApiException(PolyException):
def **init**(self, resp: Optional[httpx.Response] = None, error_msg=None):
assert resp is not None or error_msg is not None

        if resp is not None:
            self.status_code = resp.status_code
            self.error_msg = self._get_message(resp)
        else:
            self.status_code = None
            self.error_msg = error_msg

    def _get_message(self, resp: httpx.Response):
        try:
            return resp.json()
        except Exception:
            return resp.text

    def __repr__(self):
        return f"PolyApiException[status_code={self.status_code}, error_message={self.error_msg}]"

    def __str__(self):
        return self.__repr__()

================================================
FILE: py_clob_client/signer.py
================================================
from eth_account import Account

class Signer:
def **init**(self, private_key: str, chain_id: int):
assert private_key is not None and chain_id is not None

        self.private_key = private_key
        self.account = Account.from_key(private_key)
        self.chain_id = chain_id

    def address(self):
        return self.account.address

    def get_chain_id(self):
        return self.chain_id

    def sign(self, message_hash):
        """
        Signs a message hash
        """
        return Account._sign_hash(message_hash, self.private_key).signature.hex()

================================================
FILE: py_clob_client/utilities.py
================================================
import hashlib

from .clob_types import OrderBookSummary, OrderSummary, TickSize

def parse_raw_orderbook_summary(raw_obs: any) -> OrderBookSummary:
bids = []
for bid in raw_obs["bids"]:
bids.append(OrderSummary(size=bid["size"], price=bid["price"]))

    asks = []
    for ask in raw_obs["asks"]:
        asks.append(OrderSummary(size=ask["size"], price=ask["price"]))

    orderbookSummary = OrderBookSummary(
        market=raw_obs["market"],
        asset_id=raw_obs["asset_id"],
        timestamp=raw_obs["timestamp"],
        min_order_size=raw_obs["min_order_size"],
        neg_risk=raw_obs["neg_risk"],
        tick_size=raw_obs["tick_size"],
        bids=bids,
        asks=asks,
        hash=raw_obs["hash"],
    )

    return orderbookSummary

def generate_orderbook_summary_hash(orderbook: OrderBookSummary) -> str:
orderbook.hash = ""
hash = hashlib.sha1(str(orderbook.json).encode("utf-8")).hexdigest()
orderbook.hash = hash
return hash

def order_to_json(order, owner, orderType) -> dict:
return {"order": order.dict(), "owner": owner, "orderType": orderType}

def is_tick_size_smaller(a: TickSize, b: TickSize) -> bool:
return float(a) < float(b)

def price_valid(price: float, tick_size: TickSize) -> bool:
return price >= float(tick_size) and price <= 1 - float(tick_size)

================================================
FILE: py_clob_client/headers/**init**.py
================================================
[Empty file]

================================================
FILE: py_clob_client/headers/headers.py
================================================
from ..clob_types import ApiCreds, RequestArgs
from ..signing.hmac import build_hmac_signature
from ..signer import Signer
from ..signing.eip712 import sign_clob_auth_message

from datetime import datetime

POLY_ADDRESS = "POLY_ADDRESS"
POLY_SIGNATURE = "POLY_SIGNATURE"
POLY_TIMESTAMP = "POLY_TIMESTAMP"
POLY_NONCE = "POLY_NONCE"
POLY_API_KEY = "POLY_API_KEY"
POLY_PASSPHRASE = "POLY_PASSPHRASE"

def create_level_1_headers(signer: Signer, nonce: int = None):
"""
Creates Level 1 Poly headers for a request
"""
timestamp = int(datetime.now().timestamp())

    n = 0
    if nonce is not None:
        n = nonce

    signature = sign_clob_auth_message(signer, timestamp, n)
    headers = {
        POLY_ADDRESS: signer.address(),
        POLY_SIGNATURE: signature,
        POLY_TIMESTAMP: str(timestamp),
        POLY_NONCE: str(n),
    }

    return headers

def create_level_2_headers(signer: Signer, creds: ApiCreds, request_args: RequestArgs):
"""Creates Level 2 Poly headers for a request using pre-serialized body if provided"""
timestamp = int(datetime.now().timestamp())

    # Prefer the pre-serialized body string for deterministic signing if available
    body_for_sig = (
        request_args.serialized_body
        if request_args.serialized_body is not None
        else request_args.body
    )

    hmac_sig = build_hmac_signature(
        creds.api_secret,
        timestamp,
        request_args.method,
        request_args.request_path,
        body_for_sig,
    )

    return {
        POLY_ADDRESS: signer.address(),
        POLY_SIGNATURE: hmac_sig,
        POLY_TIMESTAMP: str(timestamp),
        POLY_API_KEY: creds.api_key,
        POLY_PASSPHRASE: creds.api_passphrase,
    }

def enrich_l2_headers_with_builder_headers(
headers: dict, builder_headers: dict
) -> dict:
return {**headers, **builder_headers}

================================================
FILE: py_clob_client/http_helpers/**init**.py
================================================
[Empty file]

================================================
FILE: py_clob_client/http_helpers/helpers.py
================================================
import httpx

from py_clob_client.clob_types import (
DropNotificationParams,
BalanceAllowanceParams,
OrderScoringParams,
OrdersScoringParams,
TradeParams,
OpenOrderParams,
)

from ..exceptions import PolyApiException

GET = "GET"
POST = "POST"
DELETE = "DELETE"
PUT = "PUT"

\_http_client = httpx.Client(http2=True)

def overloadHeaders(method: str, headers: dict) -> dict:
if headers is None:
headers = dict()
headers["User-Agent"] = "py_clob_client"

    headers["Accept"] = "*/*"
    headers["Connection"] = "keep-alive"
    headers["Content-Type"] = "application/json"

    if method == GET:
        headers["Accept-Encoding"] = "gzip"

    return headers

def request(endpoint: str, method: str, headers=None, data=None):
try:
headers = overloadHeaders(method, headers)
if isinstance(data, str): # Pre-serialized body: send exact bytes
resp = \_http_client.request(
method=method,
url=endpoint,
headers=headers,
content=data.encode("utf-8"),
)
else:
resp = \_http_client.request(
method=method,
url=endpoint,
headers=headers,
json=data,
)

        if resp.status_code != 200:
            raise PolyApiException(resp)

        try:
            return resp.json()
        except ValueError:
            return resp.text

    except httpx.RequestError:
        raise PolyApiException(error_msg="Request exception!")

def post(endpoint, headers=None, data=None):
return request(endpoint, POST, headers, data)

def get(endpoint, headers=None, data=None):
return request(endpoint, GET, headers, data)

def delete(endpoint, headers=None, data=None):
return request(endpoint, DELETE, headers, data)

def put(endpoint, headers=None, data=None):
return request(endpoint, PUT, headers, data)

def build_query_params(url: str, param: str, val: str) -> str:
url_with_params = url
last = url_with_params[-1] # if last character in url string == "?", append the param directly: api.com?param=value
if last == "?":
url_with_params = "{}{}={}".format(url_with_params, param, val)
else: # else add "&", then append the param
url_with_params = "{}&{}={}".format(url_with_params, param, val)
return url_with_params

def add_query_trade_params(
base_url: str, params: TradeParams = None, next_cursor="MA=="
) -> str:
"""
Adds query parameters to a url
"""
url = base_url
if params:
url = url + "?"
if params.market:
url = build_query_params(url, "market", params.market)
if params.asset_id:
url = build_query_params(url, "asset_id", params.asset_id)
if params.after:
url = build_query_params(url, "after", params.after)
if params.before:
url = build_query_params(url, "before", params.before)
if params.maker_address:
url = build_query_params(url, "maker_address", params.maker_address)
if params.id:
url = build_query_params(url, "id", params.id)
if next_cursor:
url = build_query_params(url, "next_cursor", next_cursor)
return url

def add_query_open_orders_params(
base_url: str, params: OpenOrderParams = None, next_cursor="MA=="
) -> str:
"""
Adds query parameters to a url
"""
url = base_url
if params:
url = url + "?"
if params.market:
url = build_query_params(url, "market", params.market)
if params.asset_id:
url = build_query_params(url, "asset_id", params.asset_id)
if params.id:
url = build_query_params(url, "id", params.id)
if next_cursor:
url = build_query_params(url, "next_cursor", next_cursor)
return url

def drop_notifications_query_params(
base_url: str, params: DropNotificationParams = None
) -> str:
"""
Adds query parameters to a url
"""
url = base_url
if params:
url = url + "?"
if params.ids:
url = build_query_params(url, "ids", ",".join(params.ids))
return url

def add_balance_allowance_params_to_url(
base_url: str, params: BalanceAllowanceParams = None
) -> str:
"""
Adds query parameters to a url
"""
url = base_url
if params:
url = url + "?"
if params.asset_type:
url = build_query_params(url, "asset_type", params.asset_type.**str**())
if params.token_id:
url = build_query_params(url, "token_id", params.token_id)
if params.signature_type is not None:
url = build_query_params(url, "signature_type", params.signature_type)
return url

def add_order_scoring_params_to_url(
base_url: str, params: OrderScoringParams = None
) -> str:
"""
Adds query parameters to a url
"""
url = base_url
if params:
url = url + "?"
if params.orderId:
url = build_query_params(url, "order_id", params.orderId)
return url

def add_orders_scoring_params_to_url(
base_url: str, params: OrdersScoringParams = None
) -> str:
"""
Adds query parameters to a url
"""
url = base_url
if params:
url = url + "?"
if params.orderIds:
url = build_query_params(url, "order_ids", ",".join(params.orderIds))
return url

================================================
FILE: py_clob_client/order_builder/**init**.py
================================================
[Empty file]

================================================
FILE: py_clob_client/order_builder/builder.py
================================================
from py_order_utils.builders import OrderBuilder as UtilsOrderBuilder
from py_order_utils.signer import Signer as UtilsSigner
from py_order_utils.model import (
EOA,
OrderData,
SignedOrder,
BUY as UtilsBuy,
SELL as UtilsSell,
)

from .helpers import (
to_token_decimals,
round_down,
round_normal,
decimal_places,
round_up,
)
from .constants import BUY, SELL
from ..config import get_contract_config
from ..signer import Signer
from ..clob_types import (
OrderArgs,
CreateOrderOptions,
TickSize,
RoundConfig,
MarketOrderArgs,
OrderSummary,
OrderType,
)

ROUNDING_CONFIG: dict[TickSize, RoundConfig] = {
"0.1": RoundConfig(price=1, size=2, amount=3),
"0.01": RoundConfig(price=2, size=2, amount=4),
"0.001": RoundConfig(price=3, size=2, amount=5),
"0.0001": RoundConfig(price=4, size=2, amount=6),
}

class OrderBuilder:
def **init**(self, signer: Signer, sig_type=None, funder=None):
self.signer = signer

        # Signature type used sign orders, defaults to EOA type
        self.sig_type = sig_type if sig_type is not None else EOA

        # Address which holds funds to be used.
        # Used for Polymarket proxy wallets and other smart contract wallets
        # Defaults to the address of the signer
        self.funder = funder if funder is not None else self.signer.address()

    def get_order_amounts(
        self, side: str, size: float, price: float, round_config: RoundConfig
    ):
        raw_price = round_normal(price, round_config.price)

        if side == BUY:
            raw_taker_amt = round_down(size, round_config.size)

            raw_maker_amt = raw_taker_amt * raw_price
            if decimal_places(raw_maker_amt) > round_config.amount:
                raw_maker_amt = round_up(raw_maker_amt, round_config.amount + 4)
                if decimal_places(raw_maker_amt) > round_config.amount:
                    raw_maker_amt = round_down(raw_maker_amt, round_config.amount)

            maker_amount = to_token_decimals(raw_maker_amt)
            taker_amount = to_token_decimals(raw_taker_amt)

            return UtilsBuy, maker_amount, taker_amount
        elif side == SELL:
            raw_maker_amt = round_down(size, round_config.size)

            raw_taker_amt = raw_maker_amt * raw_price
            if decimal_places(raw_taker_amt) > round_config.amount:
                raw_taker_amt = round_up(raw_taker_amt, round_config.amount + 4)
                if decimal_places(raw_taker_amt) > round_config.amount:
                    raw_taker_amt = round_down(raw_taker_amt, round_config.amount)

            maker_amount = to_token_decimals(raw_maker_amt)
            taker_amount = to_token_decimals(raw_taker_amt)

            return UtilsSell, maker_amount, taker_amount
        else:
            raise ValueError(f"order_args.side must be '{BUY}' or '{SELL}'")

    def get_market_order_amounts(
        self, side: str, amount: float, price: float, round_config: RoundConfig
    ):
        raw_price = round_normal(price, round_config.price)

        if side == BUY:
            raw_maker_amt = round_down(amount, round_config.size)
            raw_taker_amt = raw_maker_amt / raw_price
            if decimal_places(raw_taker_amt) > round_config.amount:
                raw_taker_amt = round_up(raw_taker_amt, round_config.amount + 4)
                if decimal_places(raw_taker_amt) > round_config.amount:
                    raw_taker_amt = round_down(raw_taker_amt, round_config.amount)

            maker_amount = to_token_decimals(raw_maker_amt)
            taker_amount = to_token_decimals(raw_taker_amt)

            return UtilsBuy, maker_amount, taker_amount

        elif side == SELL:
            raw_maker_amt = round_down(amount, round_config.size)

            raw_taker_amt = raw_maker_amt * raw_price
            if decimal_places(raw_taker_amt) > round_config.amount:
                raw_taker_amt = round_up(raw_taker_amt, round_config.amount + 4)
                if decimal_places(raw_taker_amt) > round_config.amount:
                    raw_taker_amt = round_down(raw_taker_amt, round_config.amount)

            maker_amount = to_token_decimals(raw_maker_amt)
            taker_amount = to_token_decimals(raw_taker_amt)

            return UtilsSell, maker_amount, taker_amount
        else:
            raise ValueError(f"order_args.side must be '{BUY}' or '{SELL}'")

    def create_order(
        self, order_args: OrderArgs, options: CreateOrderOptions
    ) -> SignedOrder:
        """
        Creates and signs an order
        """
        side, maker_amount, taker_amount = self.get_order_amounts(
            order_args.side,
            order_args.size,
            order_args.price,
            ROUNDING_CONFIG[options.tick_size],
        )

        data = OrderData(
            maker=self.funder,
            taker=order_args.taker,
            tokenId=order_args.token_id,
            makerAmount=str(maker_amount),
            takerAmount=str(taker_amount),
            side=side,
            feeRateBps=str(order_args.fee_rate_bps),
            nonce=str(order_args.nonce),
            signer=self.signer.address(),
            expiration=str(order_args.expiration),
            signatureType=self.sig_type,
        )

        contract_config = get_contract_config(
            self.signer.get_chain_id(), options.neg_risk
        )

        order_builder = UtilsOrderBuilder(
            contract_config.exchange,
            self.signer.get_chain_id(),
            UtilsSigner(key=self.signer.private_key),
        )

        return order_builder.build_signed_order(data)

    def create_market_order(
        self, order_args: MarketOrderArgs, options: CreateOrderOptions
    ) -> SignedOrder:
        """
        Creates and signs a market order
        """
        side, maker_amount, taker_amount = self.get_market_order_amounts(
            order_args.side,
            order_args.amount,
            order_args.price,
            ROUNDING_CONFIG[options.tick_size],
        )

        data = OrderData(
            maker=self.funder,
            taker=order_args.taker,
            tokenId=order_args.token_id,
            makerAmount=str(maker_amount),
            takerAmount=str(taker_amount),
            side=side,
            feeRateBps=str(order_args.fee_rate_bps),
            nonce=str(order_args.nonce),
            signer=self.signer.address(),
            expiration="0",
            signatureType=self.sig_type,
        )

        contract_config = get_contract_config(
            self.signer.get_chain_id(), options.neg_risk
        )

        order_builder = UtilsOrderBuilder(
            contract_config.exchange,
            self.signer.get_chain_id(),
            UtilsSigner(key=self.signer.private_key),
        )

        return order_builder.build_signed_order(data)

    def calculate_buy_market_price(
        self,
        positions: list[OrderSummary],
        amount_to_match: float,
        order_type: OrderType,
    ) -> float:
        if not positions:
            raise Exception("no match")

        sum = 0
        for p in reversed(positions):
            sum += float(p.size) * float(p.price)
            if sum >= amount_to_match:
                return float(p.price)

        if order_type == OrderType.FOK:
            raise Exception("no match")

        return float(positions[0].price)

    def calculate_sell_market_price(
        self,
        positions: list[OrderSummary],
        amount_to_match: float,
        order_type: OrderType,
    ) -> float:
        if not positions:
            raise Exception("no match")

        sum = 0
        for p in reversed(positions):
            sum += float(p.size)
            if sum >= amount_to_match:
                return float(p.price)

        if order_type == OrderType.FOK:
            raise Exception("no match")

        return float(positions[0].price)

================================================
FILE: py_clob_client/order_builder/constants.py
================================================
BUY = "BUY"
SELL = "SELL"

================================================
FILE: py_clob_client/order_builder/helpers.py
================================================
from math import floor, ceil
from decimal import Decimal

def round_down(x: float, sig_digits: int) -> float:
return floor(x \* (10**sig_digits)) / (10**sig_digits)

def round_normal(x: float, sig_digits: int) -> float:
return round(x \* (10**sig_digits)) / (10**sig_digits)

def round_up(x: float, sig_digits: int) -> float:
return ceil(x \* (10**sig_digits)) / (10**sig_digits)

def to_token_decimals(x: float) -> int:
f = (10\*_6) _ x
if decimal_places(f) > 0:
f = round_normal(f, 0)
return int(f)

def decimal_places(x: float) -> int:
return abs(Decimal(x.**str**()).as_tuple().exponent)

================================================
FILE: py_clob_client/rfq/**init**.py
================================================
from .rfq_types import ( # Input types
RfqUserRequest,
RfqUserQuote,
CreateRfqRequestParams,
CreateRfqQuoteParams,
CancelRfqRequestParams,
CancelRfqQuoteParams,
AcceptQuoteParams,
ApproveOrderParams,
GetRfqRequestsParams,
GetRfqQuotesParams,
GetRfqBestQuoteParams, # Response types
RfqRequest,
RfqQuote,
RfqRequestResponse,
RfqQuoteResponse,
RfqPaginatedResponse,
)

from .rfq_helpers import (
parse_units,
to_camel_case,
parse_rfq_requests_params,
parse_rfq_quotes_params,
COLLATERAL_TOKEN_DECIMALS,
CONDITIONAL_TOKEN_DECIMALS,
)

from .rfq_client import RfqClient

**all** = [
# Client
"RfqClient",
# Input types
"RfqUserRequest",
"RfqUserQuote",
"CreateRfqRequestParams",
"CreateRfqQuoteParams",
"CancelRfqRequestParams",
"CancelRfqQuoteParams",
"AcceptQuoteParams",
"ApproveOrderParams",
"GetRfqRequestsParams",
"GetRfqQuotesParams",
"GetRfqBestQuoteParams",
# Response types
"RfqRequest",
"RfqQuote",
"RfqRequestResponse",
"RfqQuoteResponse",
"RfqPaginatedResponse",
# Helpers
"parse_units",
"to_camel_case",
"parse_rfq_requests_params",
"parse_rfq_quotes_params",
"COLLATERAL_TOKEN_DECIMALS",
"CONDITIONAL_TOKEN_DECIMALS",
]

================================================
FILE: py_clob_client/rfq/rfq_client.py
================================================
"""
RFQ (Request for Quote) client for the Polymarket CLOB API.

This module provides the RfqClient class which handles all RFQ operations
including creating requests, quotes, and executing trades.
"""

import logging
from typing import Optional, Any, TYPE_CHECKING

from ..clob_types import RequestArgs, OrderArgs, PartialCreateOrderOptions
from ..headers.headers import create_level_2_headers
from ..http_helpers.helpers import get, post, delete
from ..order_builder.builder import ROUNDING_CONFIG
from ..order_builder.helpers import round_normal, round_down
from ..order_builder.constants import BUY, SELL
from ..endpoints import (
CREATE_RFQ_REQUEST,
CANCEL_RFQ_REQUEST,
GET_RFQ_REQUESTS,
CREATE_RFQ_QUOTE,
CANCEL_RFQ_QUOTE,
GET_RFQ_QUOTES,
GET_RFQ_BEST_QUOTE,
RFQ_REQUESTS_ACCEPT,
RFQ_QUOTE_APPROVE,
RFQ_CONFIG,
)

from .rfq_types import (
RfqUserRequest,
RfqUserQuote,

    CancelRfqRequestParams,
    CancelRfqQuoteParams,
    AcceptQuoteParams,
    ApproveOrderParams,
    GetRfqRequestsParams,
    GetRfqQuotesParams,
    GetRfqBestQuoteParams,

)
from .rfq_helpers import (
parse_units,
parse_rfq_requests_params,
parse_rfq_quotes_params,
COLLATERAL_TOKEN_DECIMALS,
)

if TYPE_CHECKING:
from ..client import ClobClient

class RfqClient:
"""
RFQ client for creating and managing RFQ requests and quotes.

    This client is typically accessed via the parent ClobClient's `rfq` attribute:

        client = ClobClient(host, chain_id, key, creds)
        response = client.rfq.create_rfq_request(user_request)
    """

    def __init__(self, parent: "ClobClient"):
        """
        Initialize the RFQ client.

        Args:
            parent: The parent ClobClient instance providing auth and config.
        """
        self._parent = parent
        self.logger = logging.getLogger(self.__class__.__name__)

    def _ensure_l2_auth(self) -> None:
        """
        Verify that L2 authentication is available.

        Raises:
            PolyException: If signer or creds are not configured.
        """
        self._parent.assert_level_2_auth()

    def _get_l2_headers(self, method: str, endpoint: str, body: Any = None) -> dict:
        """
        Create L2 authentication headers for a request.

        Args:
            method: HTTP method (GET, POST, PUT, DELETE)
            endpoint: API endpoint path
            body: Optional request body

        Returns:
            Dictionary of authentication headers.
        """
        request_args = RequestArgs(method=method, request_path=endpoint, body=body)
        return create_level_2_headers(
            self._parent.signer,
            self._parent.creds,
            request_args,
        )

    def _build_url(self, endpoint: str) -> str:
        """Build full URL from endpoint."""
        return f"{self._parent.host}{endpoint}"

    # =========================================================================
    # Request-side methods
    # =========================================================================

    def create_rfq_request(
        self,
        user_request: RfqUserRequest,
        options: Optional[PartialCreateOrderOptions] = None,
    ) -> dict:
        """
        Create and post an RFQ request from a user request.

        This method:
        1. Resolves the tick size for the token
        2. Rounds price and size according to tick size rules
        3. Calculates amount_in and amount_out based on side
        4. Posts the request to the server

        Args:
            user_order: Simplified order with token_id, price, side, size
            options: Optional tick size override

        Returns:
            Response dict with request_id on success.

        Example:
            >>> response = client.rfq.create_rfq_request(
            ...     RfqUserRequest(
            ...         token_id="123...",
            ...         price=0.5,
            ...         side="BUY",
            ...         size=40,
            ...     )
            ... )
        """
        token_id = user_request.token_id
        price = user_request.price
        side = user_request.side
        size = user_request.size

        # Resolve tick size (from options or fetch from server)
        tick_size = self._parent._ClobClient__resolve_tick_size(
            token_id,
            options.tick_size if options else None,
        )

        # Get rounding configuration (ensure tick_size is a string for lookup)
        tick_size_str = str(tick_size) if not isinstance(tick_size, str) else tick_size
        round_config = ROUNDING_CONFIG[tick_size_str]

        # Round price and size
        rounded_price = round_normal(price, round_config.price)
        rounded_size = round_down(size, round_config.size)

        # Format with correct decimal places
        price_decimals = int(round_config.price)
        size_decimals = int(round_config.size)
        amount_decimals = int(round_config.amount)

        rounded_price_str = f"{rounded_price:.{price_decimals}f}"
        rounded_size_str = f"{rounded_size:.{size_decimals}f}"

        # Parse back to numbers for calculation
        size_num = float(rounded_size_str)
        price_num = float(rounded_price_str)

        # Get signature type from parent's order builder
        user_type = self._parent.builder.sig_type

        # Calculate amounts based on side
        if side == BUY:
            # Buying tokens: pay USDC, receive tokens
            # asset_in = tokens (what requester receives)
            # asset_out = USDC (what requester pays)
            amount_in = parse_units(rounded_size_str, COLLATERAL_TOKEN_DECIMALS)

            usdc_amount = size_num * price_num
            usdc_amount_str = f"{usdc_amount:.{amount_decimals}f}"
            amount_out = parse_units(usdc_amount_str, COLLATERAL_TOKEN_DECIMALS)

            asset_in = token_id
            asset_out = "0"  # USDC
        else:
            # Selling tokens: pay tokens, receive USDC
            # asset_in = USDC (what requester receives)
            # asset_out = tokens (what requester pays)
            usdc_amount = size_num * price_num
            usdc_amount_str = f"{usdc_amount:.{amount_decimals}f}"
            amount_in = parse_units(usdc_amount_str, COLLATERAL_TOKEN_DECIMALS)

            amount_out = parse_units(rounded_size_str, COLLATERAL_TOKEN_DECIMALS)

            asset_in = "0"  # USDC
            asset_out = token_id

        # Post directly to the server
        self._ensure_l2_auth()

        body = {
            "assetIn": asset_in,
            "assetOut": asset_out,
            "amountIn": str(amount_in),
            "amountOut": str(amount_out),
            "userType": user_type,
        }

        headers = self._get_l2_headers("POST", CREATE_RFQ_REQUEST, body)
        return post(self._build_url(CREATE_RFQ_REQUEST), headers=headers, data=body)

    def cancel_rfq_request(self, params: CancelRfqRequestParams) -> str:
        """
        Cancel an RFQ request.

        Args:
            params: Contains request_id to cancel.

        Returns:
            "OK" on success.
        """
        self._ensure_l2_auth()

        body = {"requestId": params.request_id}

        headers = self._get_l2_headers("DELETE", CANCEL_RFQ_REQUEST, body)
        return delete(self._build_url(CANCEL_RFQ_REQUEST), headers=headers, data=body)

    def get_rfq_requests(
        self, params: Optional[GetRfqRequestsParams] = None
    ) -> dict:
        """
        Get RFQ requests with optional filtering.

        Args:
            params: Optional filter parameters.

        Returns:
            Paginated response with RFQ requests.
        """
        self._ensure_l2_auth()

        headers = self._get_l2_headers("GET", GET_RFQ_REQUESTS)
        query_params = parse_rfq_requests_params(params)

        # Build URL with query params
        url = self._build_url(GET_RFQ_REQUESTS)
        if query_params:
            query_string = "&".join(f"{k}={v}" for k, v in query_params.items())
            url = f"{url}?{query_string}"

        return get(url, headers=headers)

    # =========================================================================
    # Quote-side methods
    # =========================================================================

    def create_rfq_quote(
        self,
        user_quote: RfqUserQuote,
        options: Optional[PartialCreateOrderOptions] = None,
    ) -> dict:
        """
        Create and post an RFQ quote in response to an RFQ request.

        This method:
        1. Fetches the RFQ request to get token_id
        2. Resolves the tick size for the token
        3. Rounds price and size according to tick size rules
        4. Calculates amount_in and amount_out based on side
        5. Posts the quote to the server

        Args:
            user_quote: Simplified quote with request_id, token_id, price, side, size
            options: Optional tick size override

        Returns:
            Response dict with quote_id on success.

        Example:
            >>> response = client.rfq.create_rfq_quote(
            ...     RfqUserQuote(
            ...         request_id="019a83a9-f4c7-7c96-9139-2da2b2d934ef",
            ...         token_id="123...",
            ...         price=0.5,
            ...         side="SELL",
            ...         size=100.0,
            ...     )
            ... )
        """
        request_id = user_quote.request_id
        token_id = user_quote.token_id
        price = user_quote.price
        side = user_quote.side
        size = user_quote.size

        # Resolve tick size (from options or fetch from server)
        tick_size = self._parent._ClobClient__resolve_tick_size(
            token_id,
            options.tick_size if options else None,
        )

        # Get rounding configuration (ensure tick_size is a string for lookup)
        tick_size_str = str(tick_size) if not isinstance(tick_size, str) else tick_size
        round_config = ROUNDING_CONFIG[tick_size_str]

        # Round price and size
        rounded_price = round_normal(price, round_config.price)
        rounded_size = round_down(size, round_config.size)

        # Format with correct decimal places
        price_decimals = int(round_config.price)
        size_decimals = int(round_config.size)
        amount_decimals = int(round_config.amount)

        rounded_price_str = f"{rounded_price:.{price_decimals}f}"
        rounded_size_str = f"{rounded_size:.{size_decimals}f}"

        # Parse back to numbers for calculation
        size_num = float(rounded_size_str)
        price_num = float(rounded_price_str)

        # Get signature type from parent's order builder
        user_type = self._parent.builder.sig_type

        # Calculate amounts based on side
        if side == BUY:
            # Buying tokens: pay USDC, receive tokens
            # asset_in = tokens (what quoter receives)
            # asset_out = USDC (what quoter pays)
            amount_in = parse_units(rounded_size_str, COLLATERAL_TOKEN_DECIMALS)

            usdc_amount = size_num * price_num
            usdc_amount_str = f"{usdc_amount:.{amount_decimals}f}"
            amount_out = parse_units(usdc_amount_str, COLLATERAL_TOKEN_DECIMALS)

            asset_in = token_id
            asset_out = "0"  # USDC
        else:
            # Selling tokens: pay tokens, receive USDC
            # asset_in = USDC (what quoter receives)
            # asset_out = tokens (what quoter pays)
            usdc_amount = size_num * price_num
            usdc_amount_str = f"{usdc_amount:.{amount_decimals}f}"
            amount_in = parse_units(usdc_amount_str, COLLATERAL_TOKEN_DECIMALS)

            amount_out = parse_units(rounded_size_str, COLLATERAL_TOKEN_DECIMALS)

            asset_in = "0"  # USDC
            asset_out = token_id

        # Post directly to the server
        self._ensure_l2_auth()

        body = {
            "requestId": request_id,
            "assetIn": asset_in,
            "assetOut": asset_out,
            "amountIn": str(amount_in),
            "amountOut": str(amount_out),
            "userType": user_type,
        }

        headers = self._get_l2_headers("POST", CREATE_RFQ_QUOTE, body)
        return post(self._build_url(CREATE_RFQ_QUOTE), headers=headers, data=body)

    def get_rfq_quotes(self, params: Optional[GetRfqQuotesParams] = None) -> dict:
        """
        Get RFQ quotes with optional filtering.

        Args:
            params: Optional filter parameters.

        Returns:
            Paginated response with RFQ quotes.
        """
        self._ensure_l2_auth()

        headers = self._get_l2_headers("GET", GET_RFQ_QUOTES)
        query_params = parse_rfq_quotes_params(params)

        # Build URL with query params
        url = self._build_url(GET_RFQ_QUOTES)
        if query_params:
            query_string = "&".join(f"{k}={v}" for k, v in query_params.items())
            url = f"{url}?{query_string}"

        return get(url, headers=headers)

    def get_rfq_best_quote(
        self, params: Optional[GetRfqBestQuoteParams] = None
    ) -> dict:
        """
        Get the best quote for an RFQ request.

        Args:
            params: Contains request_id.

        Returns:
            Single quote object representing the best quote.
        """
        self._ensure_l2_auth()

        headers = self._get_l2_headers("GET", GET_RFQ_BEST_QUOTE)

        url = self._build_url(GET_RFQ_BEST_QUOTE)
        if params and params.request_id:
            url = f"{url}?requestId={params.request_id}"

        return get(url, headers=headers)

    def cancel_rfq_quote(self, params: CancelRfqQuoteParams) -> str:
        """
        Cancel an RFQ quote.

        Args:
            params: Contains quote_id to cancel.

        Returns:
            "OK" on success.
        """
        self._ensure_l2_auth()

        body = {"quoteId": params.quote_id}

        headers = self._get_l2_headers("DELETE", CANCEL_RFQ_QUOTE, body)
        return delete(self._build_url(CANCEL_RFQ_QUOTE), headers=headers, data=body)

    # =========================================================================
    # Trade execution methods
    # =========================================================================

    def accept_rfq_quote(self, params: AcceptQuoteParams) -> str:
        """
        Accept an RFQ quote (requester side).

        This method:
        1. Fetches the RFQ quote details
        2. Creates a signed order matching the quote
        3. Submits the acceptance with the order

        Args:
            params: Contains request_id, quote_id, and expiration.

        Returns:
            "OK" on success.
        """
        self._ensure_l2_auth()

        # Step 1: Fetch the RFQ request
        rfq_requests = self.get_rfq_requests(
            GetRfqRequestsParams(request_ids=[params.request_id])
        )

        if not rfq_requests.get("data") or len(rfq_requests["data"]) == 0:
            raise Exception("RFQ request not found")

        rfq_request = rfq_requests["data"][0]

        # Step 2: Create an order based on request details
        # Requester keeps their original side
        side = rfq_request.get("side", BUY)

        # Determine size based on request side
        if side == BUY:
            size = rfq_request.get("sizeIn")
        else:
            size = rfq_request.get("sizeOut")

        token_id = rfq_request.get("token")
        price = rfq_request.get("price")

        order_args = OrderArgs(
            token_id=token_id,
            price=float(price),
            size=float(size),
            side=side,
            expiration=params.expiration,
        )

        order = self._parent.create_order(order_args)

        if not order:
            raise Exception("Error creating order")

        # Step 3: Build accept payload
        order_dict = order.dict()

        accept_payload = {
            "requestId": params.request_id,
            "quoteId": params.quote_id,
            "owner": self._parent.creds.api_key,
            # Order fields from dict
            "salt": int(order_dict["salt"]),
            "maker": order_dict["maker"],
            "signer": order_dict["signer"],
            "taker": order_dict["taker"],
            "tokenId": order_dict["tokenId"],
            "makerAmount": order_dict["makerAmount"],
            "takerAmount": order_dict["takerAmount"],
            "expiration": int(order_dict["expiration"]),
            "nonce": order_dict["nonce"],
            "feeRateBps": order_dict["feeRateBps"],
            "side": side,
            "signatureType": int(order_dict["signatureType"]),
            "signature": order_dict["signature"],
        }

        self.logger.debug(
            "Accept payload: requestId=%s, quoteId=%s, tokenId=%s, side=%s",
            accept_payload.get("requestId"),
            accept_payload.get("quoteId"),
            accept_payload.get("tokenId"),
            accept_payload.get("side"),
        )

        headers = self._get_l2_headers("POST", RFQ_REQUESTS_ACCEPT, accept_payload)
        return post(
            self._build_url(RFQ_REQUESTS_ACCEPT),
            headers=headers,
            data=accept_payload,
        )

    def approve_rfq_order(self, params: ApproveOrderParams) -> str:
        """
        Approve an RFQ order (quoter side).

        This method:
        1. Fetches the RFQ quote details
        2. Creates a signed order based on quote parameters
        3. Submits the approval with the order

        Args:
            params: Contains request_id, quote_id, and expiration.

        Returns:
            "OK" on success.
        """
        self._ensure_l2_auth()

        # Step 1: Fetch the RFQ quote
        rfq_quotes = self.get_rfq_quotes(
            GetRfqQuotesParams(quote_ids=[params.quote_id])
        )

        if not rfq_quotes.get("data") or len(rfq_quotes["data"]) == 0:
            raise Exception("RFQ quote not found")

        rfq_quote = rfq_quotes["data"][0]

        # Step 2: Create an order based on quote details
        # Quoter uses their own quote's side
        side = rfq_quote.get("side", BUY)

        # Determine size based on quote side
        if side == BUY:
            size = rfq_quote.get("sizeIn")
        else:
            size = rfq_quote.get("sizeOut")

        token_id = rfq_quote.get("token")
        price = rfq_quote.get("price")

        order_args = OrderArgs(
            token_id=token_id,
            price=float(price),
            size=float(size),
            side=side,
            expiration=params.expiration,
        )

        order = self._parent.create_order(order_args)

        if not order:
            raise Exception("Error creating order")

        # Step 3: Build approve payload
        order_dict = order.dict()

        approve_payload = {
            "requestId": params.request_id,
            "quoteId": params.quote_id,
            "owner": self._parent.creds.api_key,
            # Order fields from dict
            "salt": int(order_dict["salt"]),
            "maker": order_dict["maker"],
            "signer": order_dict["signer"],
            "taker": order_dict["taker"],
            "tokenId": order_dict["tokenId"],
            "makerAmount": order_dict["makerAmount"],
            "takerAmount": order_dict["takerAmount"],
            "expiration": int(order_dict["expiration"]),
            "nonce": order_dict["nonce"],
            "feeRateBps": order_dict["feeRateBps"],
            "side": side,
            "signatureType": int(order_dict["signatureType"]),
            "signature": order_dict["signature"],
        }

        headers = self._get_l2_headers("POST", RFQ_QUOTE_APPROVE, approve_payload)
        return post(
            self._build_url(RFQ_QUOTE_APPROVE),
            headers=headers,
            data=approve_payload,
        )

    # =========================================================================
    # Configuration
    # =========================================================================

    def rfq_config(self) -> dict:
        """
        Get RFQ configuration from the server.

        Returns:
            Configuration object with RFQ system parameters.
        """
        self._ensure_l2_auth()

        headers = self._get_l2_headers("GET", RFQ_CONFIG)
        return get(self._build_url(RFQ_CONFIG), headers=headers)

================================================
FILE: py_clob_client/rfq/rfq_helpers.py
================================================
"""
RFQ helper functions for the Polymarket CLOB API.

This module provides utility functions for RFQ operations including
query parameter parsing and unit conversion.
"""

from typing import Optional, Dict, Any

from .rfq_types import GetRfqRequestsParams, GetRfqQuotesParams

# Token decimals constants

COLLATERAL_TOKEN_DECIMALS = 6 # USDC has 6 decimals
CONDITIONAL_TOKEN_DECIMALS = 6

def parse_units(value: str, decimals: int) -> int:
"""
Convert a decimal string to smallest units (like wei for ETH).

    Args:
        value: Decimal string (e.g., "1.5")
        decimals: Number of decimal places (e.g., 6 for USDC)

    Returns:
        Integer in smallest units (e.g., 1500000 for "1.5" with 6 decimals)

    Examples:
        >>> parse_units("1.5", 6)
        1500000
        >>> parse_units("100", 6)
        100000000
        >>> parse_units("0.000001", 6)
        1
    """
    if "." in value:
        integer_part, decimal_part = value.split(".")
        # Pad or truncate decimal part to match decimals
        decimal_part = decimal_part[:decimals].ljust(decimals, "0")
        return int(integer_part + decimal_part)
    else:
        return int(value) * (10**decimals)

def to_camel_case(snake_str: str) -> str:
"""
Convert snake_case string to camelCase.

    Args:
        snake_str: String in snake_case format

    Returns:
        String in camelCase format

    Examples:
        >>> to_camel_case("user_address")
        'userAddress'
        >>> to_camel_case("request_id")
        'requestId'
        >>> to_camel_case("size_usdc_min")
        'sizeUsdcMin'
    """
    components = snake_str.split("_")
    return components[0] + "".join(x.title() for x in components[1:])

def parse_rfq_requests_params(params: Optional[GetRfqRequestsParams] = None) -> Dict[str, Any]:
"""
Convert GetRfqRequestsParams to query string parameters.

    Arrays are converted to comma-separated strings.
    Snake_case fields are converted to camelCase.

    Args:
        params: Optional filter parameters

    Returns:
        Dictionary of query parameters ready for HTTP request
    """
    if params is None:
        return {}

    result = {}

    # Single value fields (convert snake_case to camelCase)
    single_fields = [
        ("user_address", "userAddress"),
        ("state", "state"),
        ("size_min", "sizeMin"),
        ("size_max", "sizeMax"),
        ("size_usdc_min", "sizeUsdcMin"),
        ("size_usdc_max", "sizeUsdcMax"),
        ("price_min", "priceMin"),
        ("price_max", "priceMax"),
        ("sort_by", "sortBy"),
        ("sort_dir", "sortDir"),
        ("limit", "limit"),
        ("offset", "offset"),
    ]

    for python_name, api_name in single_fields:
        value = getattr(params, python_name, None)
        if value is not None:
            result[api_name] = value

    # Array fields (convert to comma-separated strings)
    if params.request_ids:
        result["requestIds"] = ",".join(params.request_ids)
    if params.states:
        result["states"] = ",".join(params.states)
    if params.markets:
        result["markets"] = ",".join(params.markets)

    return result

def parse_rfq_quotes_params(params: Optional[GetRfqQuotesParams] = None) -> Dict[str, Any]:
"""
Convert GetRfqQuotesParams to query string parameters.

    Arrays are converted to comma-separated strings.
    Snake_case fields are converted to camelCase.

    Args:
        params: Optional filter parameters

    Returns:
        Dictionary of query parameters ready for HTTP request
    """
    if params is None:
        return {}

    result = {}

    # Single value fields (convert snake_case to camelCase)
    single_fields = [
        ("user_address", "userAddress"),
        ("state", "state"),
        ("size_min", "sizeMin"),
        ("size_max", "sizeMax"),
        ("size_usdc_min", "sizeUsdcMin"),
        ("size_usdc_max", "sizeUsdcMax"),
        ("price_min", "priceMin"),
        ("price_max", "priceMax"),
        ("sort_by", "sortBy"),
        ("sort_dir", "sortDir"),
        ("limit", "limit"),
        ("offset", "offset"),
    ]

    for python_name, api_name in single_fields:
        value = getattr(params, python_name, None)
        if value is not None:
            result[api_name] = value

    # Array fields (convert to comma-separated strings)
    if params.quote_ids:
        result["quoteIds"] = ",".join(params.quote_ids)
    if params.request_ids:
        result["requestIds"] = ",".join(params.request_ids)
    if params.states:
        result["states"] = ",".join(params.states)
    if params.markets:
        result["markets"] = ",".join(params.markets)

    return result

================================================
FILE: py_clob_client/rfq/rfq_types.py
================================================
"""
RFQ (Request for Quote) data types for the Polymarket CLOB API.

This module defines all input and response types used by the RFQ client.
"""

from dataclasses import dataclass, field
from typing import List, Optional, Any
from datetime import datetime

# =============================================================================

# Input Types

# =============================================================================

@dataclass
class RfqUserRequest:
"""
Simplified user input for creating an RFQ request.

    This is the user-facing order format that gets converted to
    CreateRfqRequestParams for the API.
    """

    token_id: str
    """Token ID of the conditional token being traded."""

    price: float
    """Price per token (0 < price < 1)."""

    side: str
    """Order side: "BUY" or "SELL"."""

    size: float
    """Size in conditional tokens."""

@dataclass
class RfqUserQuote:
"""
Simplified user input for creating an RFQ quote.

    This is the user-facing quote format that gets converted to
    CreateRfqQuoteParams for the API.
    """

    request_id: str
    """ID of the RFQ request being quoted."""

    token_id: str
    """Token ID of the conditional token being traded."""

    price: float
    """Price per token (0 < price < 1)."""

    side: str
    """Quoter's side: "BUY" or "SELL"."""

    size: float
    """Size in conditional tokens."""

@dataclass
class CreateRfqRequestParams:
"""
Server payload for creating an RFQ request.

    This is the format sent to the API after converting from RfqUserRequest.
    """

    asset_in: str
    """Asset being received (token ID or "0" for USDC)."""

    asset_out: str
    """Asset being paid (token ID or "0" for USDC)."""

    amount_in: str
    """Amount being received (in smallest units, as string)."""

    amount_out: str
    """Amount being paid (in smallest units, as string)."""

    user_type: int
    """Signature type (0=EOA, 1=POLY_PROXY, 2=POLY_GNOSIS_SAFE)."""

@dataclass
class CreateRfqQuoteParams:
"""
Parameters for creating a quote in response to an RFQ request.
"""

    request_id: str
    """ID of the RFQ request being quoted."""

    asset_in: str
    """Asset the quoter is paying."""

    asset_out: str
    """Asset the quoter is receiving."""

    amount_in: str
    """Amount quoter is paying (in smallest units)."""

    amount_out: str
    """Amount quoter is receiving (in smallest units)."""

    # Note: user_type is auto-filled by the client

@dataclass
class CancelRfqRequestParams:
"""
Parameters for canceling an RFQ request.
"""

    request_id: str
    """ID of the request to cancel."""

@dataclass
class CancelRfqQuoteParams:
"""
Parameters for canceling an RFQ quote.
"""

    quote_id: str
    """ID of the quote to cancel."""

@dataclass
class AcceptQuoteParams:
"""
Parameters for accepting a quote (requester side).

    When a requester accepts a quote, they create a signed order
    and submit it with this payload.
    """

    request_id: str
    """ID of the RFQ request."""

    quote_id: str
    """ID of the quote being accepted."""

    expiration: int
    """Unix timestamp for order expiration."""

@dataclass
class ApproveOrderParams:
"""
Parameters for approving an order (quoter side).

    When a quoter's quote is accepted, they approve by creating
    a signed order and submitting it with this payload.
    """

    request_id: str
    """ID of the RFQ request."""

    quote_id: str
    """ID of the quote being approved."""

    expiration: int
    """Unix timestamp for order expiration."""

@dataclass
class GetRfqRequestsParams:
"""
Query parameters for fetching RFQ requests.

    All fields are optional filters.
    """

    request_ids: Optional[List[str]] = None
    """Filter by specific request IDs."""

    user_address: Optional[str] = None
    """Filter by user address."""

    states: Optional[List[str]] = None
    """Filter by multiple states."""

    state: Optional[str] = None
    """Single state filter ("active" or "inactive")."""

    markets: Optional[List[str]] = None
    """Filter by market condition IDs."""

    size_min: Optional[float] = None
    """Minimum size filter."""

    size_max: Optional[float] = None
    """Maximum size filter."""

    size_usdc_min: Optional[float] = None
    """Minimum USDC size filter."""

    size_usdc_max: Optional[float] = None
    """Maximum USDC size filter."""

    price_min: Optional[float] = None
    """Minimum price filter."""

    price_max: Optional[float] = None
    """Maximum price filter."""

    sort_by: Optional[str] = None
    """Field to sort by."""

    sort_dir: Optional[str] = None
    """Sort direction: "asc" or "desc"."""

    limit: Optional[int] = None
    """Pagination limit."""

    offset: Optional[str] = None
    """Pagination cursor (base64 encoded)."""

@dataclass
class GetRfqQuotesParams:
"""
Query parameters for fetching RFQ quotes.

    All fields are optional filters.
    """

    quote_ids: Optional[List[str]] = None
    """Filter by specific quote IDs."""

    request_ids: Optional[List[str]] = None
    """Filter by request IDs."""

    user_address: Optional[str] = None
    """Filter by user address."""

    states: Optional[List[str]] = None
    """Filter by multiple states."""

    state: Optional[str] = None
    """Single state filter."""

    markets: Optional[List[str]] = None
    """Filter by market condition IDs."""

    size_min: Optional[float] = None
    """Minimum size filter."""

    size_max: Optional[float] = None
    """Maximum size filter."""

    size_usdc_min: Optional[float] = None
    """Minimum USDC size filter."""

    size_usdc_max: Optional[float] = None
    """Maximum USDC size filter."""

    price_min: Optional[float] = None
    """Minimum price filter."""

    price_max: Optional[float] = None
    """Maximum price filter."""

    sort_by: Optional[str] = None
    """Field to sort by."""

    sort_dir: Optional[str] = None
    """Sort direction: "asc" or "desc"."""

    limit: Optional[int] = None
    """Pagination limit."""

    offset: Optional[str] = None
    """Pagination cursor (base64 encoded)."""

@dataclass
class GetRfqBestQuoteParams:
"""
Parameters for fetching the best quote for a request.
"""

    request_id: Optional[str] = None
    """Request ID to get best quote for."""

# =============================================================================

# Response Types

# =============================================================================

@dataclass
class RfqRequest:
"""
Full RFQ request object returned by the API.
"""

    request_id: str
    """Unique request identifier."""

    user_address: str
    """Address of the requester."""

    proxy_address: Optional[str] = None
    """Proxy address if applicable."""

    token: Optional[str] = None
    """Token ID being traded."""

    complement: Optional[str] = None
    """Complement token ID."""

    condition: Optional[str] = None
    """Condition ID (market)."""

    side: Optional[str] = None
    """Order side: "BUY" or "SELL"."""

    size_in: Optional[str] = None
    """Size of asset_in."""

    size_out: Optional[str] = None
    """Size of asset_out."""

    price: Optional[float] = None
    """Price."""

    accepted_quote_id: Optional[str] = None
    """ID of accepted quote (if any)."""

    state: Optional[str] = None
    """Request state."""

    expiry: Optional[str] = None
    """Expiration timestamp."""

    created_at: Optional[str] = None
    """Creation timestamp."""

    updated_at: Optional[str] = None
    """Last update timestamp."""

@dataclass
class RfqQuote:
"""
Full RFQ quote object returned by the API.
"""

    quote_id: str
    """Unique quote identifier."""

    request_id: str
    """Associated request ID."""

    user_address: str
    """Address of the quoter."""

    proxy_address: Optional[str] = None
    """Proxy address if applicable."""

    complement: Optional[str] = None
    """Complement token ID."""

    condition: Optional[str] = None
    """Condition ID (market)."""

    token: Optional[str] = None
    """Token ID."""

    side: Optional[str] = None
    """Quote side: "BUY" or "SELL"."""

    size_in: Optional[str] = None
    """Size of asset_in."""

    size_out: Optional[str] = None
    """Size of asset_out."""

    price: Optional[float] = None
    """Quote price."""

    state: Optional[str] = None
    """Quote state."""

    expiry: Optional[str] = None
    """Expiration timestamp."""

    created_at: Optional[str] = None
    """Creation timestamp."""

    updated_at: Optional[str] = None
    """Last update timestamp."""

@dataclass
class RfqRequestResponse:
"""
Response from creating an RFQ request.
"""

    request_id: Optional[str] = None
    """Created request ID."""

    error: Optional[str] = None
    """Error message if failed."""

@dataclass
class RfqQuoteResponse:
"""
Response from creating an RFQ quote.
"""

    quote_id: Optional[str] = None
    """Created quote ID."""

    error: Optional[str] = None
    """Error message if failed."""

@dataclass
class RfqPaginatedResponse:
"""
Paginated response for list queries.
"""

    data: List[Any] = field(default_factory=list)
    """Array of results (RfqRequest or RfqQuote objects)."""

    next_cursor: Optional[str] = None
    """Cursor for next page."""

    limit: Optional[int] = None
    """Page limit."""

    count: Optional[int] = None
    """Number of results in this page."""

    total_count: Optional[int] = None
    """Total count (optional)."""

================================================
FILE: py_clob_client/signing/**init**.py
================================================
[Empty file]

================================================
FILE: py_clob_client/signing/eip712.py
================================================
from poly_eip712_structs import make_domain
from eth_utils import keccak
from py_order_utils.utils import prepend_zx

from .model import ClobAuth
from ..signer import Signer

CLOB_DOMAIN_NAME = "ClobAuthDomain"
CLOB_VERSION = "1"
MSG_TO_SIGN = "This message attests that I control the given wallet"

def get_clob_auth_domain(chain_id: int):
return make_domain(name=CLOB_DOMAIN_NAME, version=CLOB_VERSION, chainId=chain_id)

def sign_clob_auth_message(signer: Signer, timestamp: int, nonce: int) -> str:
clob_auth_msg = ClobAuth(
address=signer.address(),
timestamp=str(timestamp),
nonce=nonce,
message=MSG_TO_SIGN,
)
chain_id = signer.get_chain_id()
auth_struct_hash = prepend_zx(
keccak(clob_auth_msg.signable_bytes(get_clob_auth_domain(chain_id))).hex()
)
return prepend_zx(signer.sign(auth_struct_hash))

================================================
FILE: py_clob_client/signing/hmac.py
================================================
import hmac
import hashlib
import base64

def build_hmac_signature(
secret: str, timestamp: str, method: str, requestPath: str, body=None
):
"""
Creates an HMAC signature by signing a payload with the secret
"""
base64_secret = base64.urlsafe_b64decode(secret)
message = str(timestamp) + str(method) + str(requestPath)
if body: # NOTE: Necessary to replace single quotes with double quotes # to generate the same hmac message as go and typescript
message += str(body).replace("'", '"')

    h = hmac.new(base64_secret, bytes(message, "utf-8"), hashlib.sha256)

    # ensure base64 encoded
    return (base64.urlsafe_b64encode(h.digest())).decode("utf-8")

================================================
FILE: py_clob_client/signing/model.py
================================================
from poly_eip712_structs import EIP712Struct, Address, String, Uint

class ClobAuth(EIP712Struct):
address = Address()
timestamp = String()
nonce = Uint()
message = String()
