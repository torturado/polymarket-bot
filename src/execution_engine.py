from __future__ import annotations

import asyncio
import csv
import logging
import os
import random
from pathlib import Path
from typing import Callable, Optional, Tuple

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType, PostOrdersArgs
from py_clob_client.config import get_contract_config

from .config import Config
from .position_manager import LegInPosition
from .utils.market_utils import PriceData


class ExecutionEngine:
    def __init__(self, client: ClobClient, config: Config):
        self.client = client
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self._paper_log_path = Path(self.config.paper_trades_log)

    @staticmethod
    def _parse_order_type(value: str) -> str:
        v = (value or "").strip().upper()
        if v in {"FOK", "FAK", "GTC", "GTD"}:
            return v
        return "FOK"

    async def _post_order(self, args: OrderArgs) -> None:
        await asyncio.to_thread(self.client.create_and_post_order, args)

    async def place_limit_buy(
        self,
        *,
        condition_id: str,
        side: str,
        token_id: str,
        price: float,
        size: float,
        action: str = "VACUUM",
        order_type: Optional[str] = None,
    ) -> bool:
        if self.config.paper_trading:
            is_new = not self._paper_log_path.exists()
            self._paper_log_path.parent.mkdir(parents=True, exist_ok=True)
            with self._paper_log_path.open("a", newline="") as f:
                w = csv.writer(f)
                if is_new:
                    w.writerow(
                        [
                            "action",
                            "condition_id",
                            "leg_1_side",
                            "leg_1_price",
                            "leg_1_size",
                            "leg_2_price",
                            "leg_2_size",
                        ]
                    )
                w.writerow(
                    [
                        action,
                        condition_id,
                        side,
                        float(price),
                        float(size),
                        "",
                        "",
                    ]
                )
            return True

        ot = order_type or self.config.vacuum_order_type
        ot_str = self._parse_order_type(ot)
        order_type_v = getattr(OrderType, ot_str, OrderType.GTC)
        order_args = OrderArgs(
            token_id=token_id,
            price=float(price),
            size=float(size),
            side="BUY",
            order_type=order_type_v,
        )
        try:
            await self._post_order(order_args)
            return True
        except Exception as e:
            self.logger.exception("Limit BUY failed action=%s condition=%s side=%s: %s", action, condition_id, side, e)
            return False

    async def execute_burst_buy(
        self,
        *,
        token_id: str,
        total_size: float,
        reference_price: float,
        max_price: Optional[float] = None,
        price_fn: Optional[Callable[[], Optional[float]]] = None,
        best_ask_size_fn: Optional[Callable[[], Optional[float]]] = None,
        stop_on_price_change: Optional[bool] = None,
        on_slice: Optional[Callable[[float, float], None]] = None,
    ) -> Tuple[float, float, int]:
        """
        Executes a BUY as a burst of smaller FOK/FAK orders.

        Returns: (filled_size, vwap_price, n_orders_sent)
        """
        remaining = float(total_size)
        if remaining <= 0:
            return 0.0, float("nan"), 0

        order_type_str = self._parse_order_type(self.config.burst_order_type)
        order_type = getattr(OrderType, order_type_str, OrderType.FOK)
        max_orders = max(int(self.config.burst_max_orders), 1)

        chunk_unit = self.config.burst_chunk_unit.strip().lower()
        min_chunk_shares = float(self.config.burst_min_chunk_shares)
        max_chunk_shares = float(self.config.burst_max_chunk_shares)
        min_chunk_usdc = float(self.config.burst_min_chunk_usdc)
        max_chunk_usdc = float(self.config.burst_max_chunk_usdc)
        min_delay_ms = max(int(self.config.burst_min_delay_ms), 0)
        max_delay_ms = max(int(self.config.burst_max_delay_ms), 0)
        if max_delay_ms < min_delay_ms:
            max_delay_ms = min_delay_ms

        filled = 0.0
        notional = 0.0
        sent = 0
        last_price: Optional[float] = None
        chunk_hint = None
        available_left: Optional[float] = None
        stop_on_move = (
            bool(self.config.burst_stop_on_price_change)
            if stop_on_price_change is None
            else bool(stop_on_price_change)
        )
        tol = float(self.config.burst_price_change_tolerance)
        use_best_ask = bool(self.config.burst_use_best_ask_size) and best_ask_size_fn is not None

        def _current_price() -> Optional[float]:
            if price_fn is None:
                return reference_price
            try:
                return price_fn()
            except Exception:
                return reference_price

        while remaining > 0 and sent < max_orders:
            price = _current_price()
            if price is None:
                break
            price_f = float(price)
            if price_f <= 0:
                break

            ref = float(reference_price)
            if stop_on_move and abs(price_f - ref) > tol:
                break
            max_price_eff = (
                float(max_price)
                if max_price is not None
                else ref + float(self.config.burst_max_price_slippage)
            )
            if max_price_eff > 0 and price_f > max_price_eff:
                break

            if chunk_unit == "usdc":
                base_chunk_usdc = random.uniform(min_chunk_usdc, max_chunk_usdc)
                if chunk_hint is None:
                    chunk_hint = base_chunk_usdc
                else:
                    chunk_hint = base_chunk_usdc

                if self.config.burst_scalein_on_price_drop and last_price is not None:
                    if price_f < last_price:
                        chunk_hint = min(
                            max_chunk_usdc,
                            float(chunk_hint) * float(self.config.burst_scalein_multiplier),
                        )
                desired_shares = float(chunk_hint) / price_f
            else:
                base_chunk_shares = random.uniform(min_chunk_shares, max_chunk_shares)
                if chunk_hint is None:
                    chunk_hint = base_chunk_shares
                else:
                    chunk_hint = base_chunk_shares

                if self.config.burst_scalein_on_price_drop and last_price is not None:
                    if price_f < last_price:
                        chunk_hint = min(
                            max_chunk_shares,
                            float(chunk_hint) * float(self.config.burst_scalein_multiplier),
                        )
                desired_shares = float(chunk_hint)

            size = min(desired_shares, remaining)
            if size <= 0:
                break

            if use_best_ask:
                try:
                    ask_sz = best_ask_size_fn()
                except Exception:
                    ask_sz = None
                ask_sz_f = float(ask_sz) if ask_sz not in (None, "") else 0.0
                if ask_sz_f > 0:
                    if self.config.paper_trading and self.config.burst_consume_book_in_paper:
                        if available_left is None:
                            available_left = ask_sz_f
                        else:
                            available_left = min(available_left, ask_sz_f)
                        size = min(size, available_left)
                    else:
                        size = min(size, ask_sz_f)
                    if size <= 0:
                        break

            if self.config.paper_trading:
                if (
                    use_best_ask
                    and self.config.burst_simulate_fok_by_depth
                    and available_left is not None
                    and size > available_left
                ):
                    break
                filled += size
                notional += price_f * size
                remaining -= size
                sent += 1
                if available_left is not None:
                    available_left = max(0.0, available_left - size)
                if on_slice is not None:
                    on_slice(price_f, size)
            else:
                order_args = OrderArgs(
                    token_id=token_id,
                    price=price_f,
                    size=size,
                    side="BUY",
                    order_type=order_type,
                )
                try:
                    await self._post_order(order_args)
                except Exception as e:
                    self.logger.exception("Burst BUY order failed: %s", e)
                    break

                filled += size
                notional += price_f * size
                remaining -= size
                sent += 1
                if on_slice is not None:
                    on_slice(price_f, size)

            last_price = price_f

            if max_delay_ms > 0:
                delay_ms = random.randint(min_delay_ms, max_delay_ms)
                if delay_ms > 0:
                    await asyncio.sleep(delay_ms / 1000.0)

        vwap_price = (notional / filled) if filled > 0 else float("nan")
        return filled, vwap_price, sent

    async def execute_burst_buy_batch(
        self,
        *,
        token_id: str,
        total_size: float,
        price: float,
    ) -> Tuple[float, float, int]:
        """
        Batch variant: posts multiple same-price orders via `client.post_orders`.
        Intended to mimic "many trades in the same second" without per-order RTT.

        Returns: (filled_size, vwap_price, n_orders_sent)
        """
        remaining = float(total_size)
        if remaining <= 0:
            return 0.0, float("nan"), 0

        order_type_str = self._parse_order_type(self.config.burst_order_type)
        order_type = getattr(OrderType, order_type_str, OrderType.FOK)

        min_chunk = float(self.config.burst_min_chunk_shares)
        max_chunk = float(self.config.burst_max_chunk_shares)
        chunk_unit = self.config.burst_chunk_unit.strip().lower()
        min_chunk_usdc = float(self.config.burst_min_chunk_usdc)
        max_chunk_usdc = float(self.config.burst_max_chunk_usdc)
        max_orders = max(int(self.config.burst_max_orders), 1)
        batch_size = max(int(self.config.burst_batch_size), 1)

        if self.config.paper_trading:
            # No need to simulate RTT savings; just fall back to the normal burst path.
            return await self.execute_burst_buy(
                token_id=token_id,
                total_size=total_size,
                reference_price=price,
                max_price=price,
                price_fn=None,
                best_ask_size_fn=None,
                stop_on_price_change=False,
                on_slice=None,
            )

        def _post_batch_sync(sizes: list[float]) -> None:
            signed = []
            for sz in sizes:
                args = OrderArgs(
                    token_id=token_id,
                    price=float(price),
                    size=float(sz),
                    side="BUY",
                    order_type=order_type,
                )
                signed_order = self.client.create_order(args)
                signed.append(PostOrdersArgs(order=signed_order, orderType=order_type))
            self.client.post_orders(signed)

        filled = 0.0
        notional = 0.0
        sent = 0

        while remaining > 0 and sent < max_orders:
            batch_remaining = min(batch_size, max_orders - sent)
            sizes: list[float] = []
            for _ in range(batch_remaining):
                if remaining <= 0:
                    break
                if chunk_unit == "usdc":
                    chunk_usdc = random.uniform(min_chunk_usdc, max_chunk_usdc)
                    chunk = float(chunk_usdc) / float(price)
                else:
                    chunk = random.uniform(min_chunk, max_chunk)
                sz = min(float(chunk), remaining)
                if sz <= 0:
                    break
                sizes.append(sz)
                remaining -= sz

            if not sizes:
                break

            try:
                await asyncio.to_thread(_post_batch_sync, sizes)
            except Exception as e:
                self.logger.exception("Burst BUY batch failed: %s", e)
                break

            batch_filled = float(sum(sizes))
            filled += batch_filled
            notional += float(price) * batch_filled
            sent += len(sizes)

        vwap_price = (notional / filled) if filled > 0 else float("nan")
        return filled, vwap_price, sent

    def _log_paper_slice_leg1(self, position: LegInPosition, *, price: float, size: float) -> None:
        is_new = not self._paper_log_path.exists()
        self._paper_log_path.parent.mkdir(parents=True, exist_ok=True)
        with self._paper_log_path.open("a", newline="") as f:
            w = csv.writer(f)
            if is_new:
                w.writerow(
                    [
                        "action",
                        "condition_id",
                        "leg_1_side",
                        "leg_1_price",
                        "leg_1_size",
                        "leg_2_price",
                        "leg_2_size",
                    ]
                )
            w.writerow(
                [
                    "LEG1_SLICE",
                    position.condition_id,
                    position.leg_1_side,
                    float(price),
                    float(size),
                    "",
                    "",
                ]
            )

    def _log_paper_slice_leg2(self, position: LegInPosition, *, price: float, size: float) -> None:
        is_new = not self._paper_log_path.exists()
        self._paper_log_path.parent.mkdir(parents=True, exist_ok=True)
        with self._paper_log_path.open("a", newline="") as f:
            w = csv.writer(f)
            if is_new:
                w.writerow(
                    [
                        "action",
                        "condition_id",
                        "leg_1_side",
                        "leg_1_price",
                        "leg_1_size",
                        "leg_2_price",
                        "leg_2_size",
                    ]
                )
            w.writerow(
                [
                    "LEG2_SLICE",
                    position.condition_id,
                    position.leg_1_side,
                    position.leg_1_entry_price,
                    position.leg_1_size,
                    float(price),
                    float(size),
                ]
            )

    async def execute_leg_1(
        self,
        position: LegInPosition,
        *,
        price_data_fn: Optional[Callable[[], Optional[PriceData]]] = None,
    ) -> bool:
        token_id = position.yes_token_id if position.leg_1_side == "YES" else position.no_token_id

        def _price_fn() -> Optional[float]:
            if price_data_fn is None:
                return position.leg_1_entry_price
            pd = price_data_fn()
            if pd is None:
                return position.leg_1_entry_price
            return float(pd.best_ask)

        def _ask_size_fn() -> Optional[float]:
            if price_data_fn is None:
                return None
            pd = price_data_fn()
            if pd is None:
                return None
            return float(getattr(pd, "best_ask_size", 0.0))

        try:
            if self.config.use_burst_execution and self.config.burst_use_batch_endpoint:
                filled, vwap_price, _ = await self.execute_burst_buy_batch(
                    token_id=token_id, total_size=position.leg_1_size, price=position.leg_1_entry_price
                )
            elif self.config.use_burst_execution:
                filled, vwap_price, _ = await self.execute_burst_buy(
                    token_id=token_id,
                    total_size=position.leg_1_size,
                    reference_price=position.leg_1_entry_price,
                    max_price=None,
                    price_fn=_price_fn,
                    best_ask_size_fn=_ask_size_fn,
                    stop_on_price_change=None,
                    on_slice=(
                        (lambda p, s: self._log_paper_slice_leg1(position, price=p, size=s))
                        if (self.config.paper_trading and self.config.burst_log_slices_in_paper)
                        else None
                    ),
                )
            else:
                if self.config.paper_trading:
                    filled = float(position.leg_1_size)
                    vwap_price = float(position.leg_1_entry_price)
                else:
                    order_args = OrderArgs(
                        token_id=token_id,
                        price=position.leg_1_entry_price,
                        size=position.leg_1_size,
                        side="BUY",
                        order_type=OrderType.FOK,
                    )
                    await self._post_order(order_args)
                    filled = float(position.leg_1_size)
                    vwap_price = float(position.leg_1_entry_price)

            if filled <= 0:
                return False
            position.leg_1_entry_price = float(vwap_price) if vwap_price == vwap_price else position.leg_1_entry_price
            position.leg_1_size = float(filled)
            position.leg_1_filled = True
            if self.config.paper_trading:
                self._log_paper_trade("LEG1", position)
            return True
        except Exception as e:
            self.logger.exception("Leg 1 order failed: %s", e)
            return False

    async def execute_leg_2(
        self,
        position: LegInPosition,
        price: float,
        size: float,
        *,
        price_data_fn: Optional[Callable[[], Optional[PriceData]]] = None,
    ) -> bool:
        opp_token_id = (
            position.no_token_id if position.leg_1_side == "YES" else position.yes_token_id
        )

        def _price_fn() -> Optional[float]:
            if price_data_fn is None:
                return price
            pd = price_data_fn()
            if pd is None:
                return price
            return float(pd.best_ask)

        def _ask_size_fn() -> Optional[float]:
            if price_data_fn is None:
                return None
            pd = price_data_fn()
            if pd is None:
                return None
            return float(getattr(pd, "best_ask_size", 0.0))
        try:
            if self.config.use_burst_execution and self.config.burst_use_batch_endpoint:
                filled, vwap_price, _ = await self.execute_burst_buy_batch(
                    token_id=opp_token_id, total_size=size, price=price
                )
            elif self.config.use_burst_execution:
                filled, vwap_price, _ = await self.execute_burst_buy(
                    token_id=opp_token_id,
                    total_size=size,
                    reference_price=price,
                    max_price=None,
                    price_fn=_price_fn,
                    best_ask_size_fn=_ask_size_fn,
                    stop_on_price_change=None,
                    on_slice=(
                        (lambda p, s: self._log_paper_slice_leg2(position, price=p, size=s))
                        if (self.config.paper_trading and self.config.burst_log_slices_in_paper)
                        else None
                    ),
                )
            else:
                if self.config.paper_trading:
                    filled = float(size)
                    vwap_price = float(price)
                else:
                    order_args = OrderArgs(
                        token_id=opp_token_id,
                        price=price,
                        size=size,
                        side="BUY",
                        order_type=OrderType.FOK,
                    )
                    await self._post_order(order_args)
                    filled = float(size)
                    vwap_price = float(price)

            if filled <= 0:
                return False

            position.leg_2_entry_price = float(vwap_price) if vwap_price == vwap_price else price
            position.leg_2_size = float(filled)
            position.leg_2_filled = True
            if self.config.paper_trading:
                self._log_paper_trade("LEG2", position)
            return True
        except Exception as e:
            self.logger.exception("Leg 2 order failed: %s", e)
            return False

    async def execute_unwind(self, position: LegInPosition, price: float, size: float) -> bool:
        if self.config.paper_trading:
            self._log_paper_trade("UNWIND", position)
            return True
        # Unwind by selling the leg 1 token back.
        token_id = position.yes_token_id if position.leg_1_side == "YES" else position.no_token_id
        order_args = OrderArgs(
            token_id=token_id,
            price=price,
            size=size,
            side="SELL",
            order_type=OrderType.FAK,
        )
        try:
            await asyncio.to_thread(self.client.create_and_post_order, order_args)
            return True
        except Exception as e:
            self.logger.exception("Unwind order failed: %s", e)
            return False

    async def merge_tokens(self, position: LegInPosition) -> bool:
        if self.config.paper_trading:
            self._log_paper_trade("MERGE", position)
            return True

        merge_amount_shares = min(
            float(position.leg_1_size),
            float(position.leg_2_size if position.leg_2_size is not None else position.leg_1_size),
        )

        for method_name in ("merge_positions", "merge_tokens", "merge", "redeem_positions", "redeem"):
            method = getattr(self.client, method_name, None)
            if method is None or not callable(method):
                continue
            try:
                await asyncio.to_thread(method, position.condition_id, merge_amount_shares)
                return True
            except TypeError:
                try:
                    await asyncio.to_thread(method, condition_id=position.condition_id, amount=merge_amount_shares)
                    return True
                except Exception:
                    self.logger.exception("API merge via client.%s failed", method_name)
                    break
            except Exception:
                self.logger.exception("API merge via client.%s failed", method_name)
                break

        provider_url = os.getenv("WEB3_PROVIDER_URL") or os.getenv("POLYGON_RPC_URL")
        if not provider_url:
            self.logger.warning(
                "No WEB3_PROVIDER_URL/POLYGON_RPC_URL set; cannot merge tokens on-chain"
            )
            return False

        if not self.config.private_key or not self.config.funder:
            self.logger.warning("Missing PRIVATE_KEY/FUNDER; cannot merge tokens on-chain")
            return False

        def _merge_via_web3_sync() -> bool:
            try:
                from web3 import Web3  # type: ignore[import-not-found]
            except Exception:
                self.logger.warning("web3 is not installed; cannot merge tokens on-chain")
                return False

            contract_cfg = get_contract_config(self.config.chain_id)
            w3 = Web3(Web3.HTTPProvider(provider_url))
            to_checksum = getattr(Web3, "to_checksum_address", None) or getattr(
                Web3, "toChecksumAddress"
            )
            funder = to_checksum(self.config.funder)

            conditional_tokens = w3.eth.contract(
                address=to_checksum(contract_cfg.conditional_tokens),
                abi=[
                    {
                        "inputs": [
                            {"internalType": "address", "name": "account", "type": "address"},
                            {"internalType": "uint256", "name": "id", "type": "uint256"},
                        ],
                        "name": "balanceOf",
                        "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
                        "stateMutability": "view",
                        "type": "function",
                    },
                    {
                        "inputs": [
                            {
                                "internalType": "address",
                                "name": "collateralToken",
                                "type": "address",
                            },
                            {
                                "internalType": "bytes32",
                                "name": "parentCollectionId",
                                "type": "bytes32",
                            },
                            {"internalType": "bytes32", "name": "conditionId", "type": "bytes32"},
                            {
                                "internalType": "uint256[]",
                                "name": "partition",
                                "type": "uint256[]",
                            },
                            {"internalType": "uint256", "name": "amount", "type": "uint256"},
                        ],
                        "name": "mergePositions",
                        "outputs": [],
                        "stateMutability": "nonpayable",
                        "type": "function",
                    },
                ],
            )

            yes_id = int(position.yes_token_id)
            no_id = int(position.no_token_id)
            bal_yes = conditional_tokens.functions.balanceOf(funder, yes_id).call()
            bal_no = conditional_tokens.functions.balanceOf(funder, no_id).call()
            amount = min(bal_yes, bal_no)
            if amount <= 0:
                return True

            to_bytes = getattr(Web3, "to_bytes", None) or getattr(Web3, "toBytes")
            condition_id_bytes = to_bytes(hexstr=position.condition_id)
            parent_collection = b"\x00" * 32
            partition = [1, 2]

            fn = conditional_tokens.functions.mergePositions(
                to_checksum(contract_cfg.collateral),
                parent_collection,
                condition_id_bytes,
                partition,
                amount,
            )

            nonce = w3.eth.get_transaction_count(funder)
            gas_price = w3.eth.gas_price
            gas = fn.estimate_gas({"from": funder})
            tx = fn.build_transaction(
                {
                    "from": funder,
                    "nonce": nonce,
                    "chainId": self.config.chain_id,
                    "gas": int(gas * 12 // 10),
                    "gasPrice": gas_price,
                }
            )
            signed = w3.eth.account.sign_transaction(tx, private_key=self.config.private_key)
            raw_tx = getattr(signed, "rawTransaction", None) or getattr(signed, "raw_transaction", None)
            if raw_tx is None:
                raise RuntimeError("Signed transaction missing raw bytes")
            tx_hash = w3.eth.send_raw_transaction(raw_tx)
            receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
            return bool(getattr(receipt, "status", 0) == 1)

        try:
            return await asyncio.to_thread(_merge_via_web3_sync)
        except Exception:
            self.logger.exception("On-chain merge failed")
            return False

    def _log_paper_trade(self, action: str, position: LegInPosition) -> None:
        is_new = not self._paper_log_path.exists()
        self._paper_log_path.parent.mkdir(parents=True, exist_ok=True)
        with self._paper_log_path.open("a", newline="") as f:
            w = csv.writer(f)
            if is_new:
                w.writerow(
                    [
                        "action",
                        "condition_id",
                        "leg_1_side",
                        "leg_1_price",
                        "leg_1_size",
                        "leg_2_price",
                        "leg_2_size",
                    ]
                )
            w.writerow(
                [
                    action,
                    position.condition_id,
                    position.leg_1_side,
                    position.leg_1_entry_price,
                    position.leg_1_size,
                    position.leg_2_entry_price,
                    position.leg_2_size,
                ]
            )
