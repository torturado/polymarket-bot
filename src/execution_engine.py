from __future__ import annotations

import asyncio
import csv
import logging
import os
from pathlib import Path
from typing import Optional

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType
from py_clob_client.config import get_contract_config

from .config import Config
from .position_manager import LegInPosition


class ExecutionEngine:
    def __init__(self, client: ClobClient, config: Config):
        self.client = client
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self._paper_log_path = Path(self.config.paper_trades_log)

    async def execute_leg_1(self, position: LegInPosition) -> bool:
        if self.config.paper_trading:
            self._log_paper_trade("LEG1", position)
            position.leg_1_filled = True
            return True

        token_id = position.yes_token_id if position.leg_1_side == "YES" else position.no_token_id
        order_args = OrderArgs(
            token_id=token_id,
            price=position.leg_1_entry_price,
            size=position.leg_1_size,
            side="BUY",
            order_type=OrderType.FOK,
        )
        try:
            await asyncio.to_thread(self.client.create_and_post_order, order_args)
            position.leg_1_filled = True
            return True
        except Exception as e:
            self.logger.exception("Leg 1 order failed: %s", e)
            return False

    async def execute_leg_2(self, position: LegInPosition, price: float, size: float) -> bool:
        if self.config.paper_trading:
            position.leg_2_entry_price = price
            position.leg_2_size = size
            position.leg_2_filled = True
            self._log_paper_trade("LEG2", position)
            return True

        opp_token_id = (
            position.no_token_id if position.leg_1_side == "YES" else position.yes_token_id
        )
        order_args = OrderArgs(
            token_id=opp_token_id,
            price=price,
            size=size,
            side="BUY",
            order_type=OrderType.FOK,
        )
        try:
            await asyncio.to_thread(self.client.create_and_post_order, order_args)
            position.leg_2_entry_price = price
            position.leg_2_size = size
            position.leg_2_filled = True
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
