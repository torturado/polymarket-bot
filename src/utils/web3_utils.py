from __future__ import annotations

from typing import Optional


async def redeem_positions_via_web3(
    *,
    web3_provider_url: str,
    private_key: str,
    funder: str,
    conditional_tokens_address: str,
    collateral_address: str,
    condition_id: str,
    index_sets: list[int],
) -> bool:
    """
    Fallback merge/redeem using ConditionalTokens via web3.py.

    This is intentionally left as a stub until on-chain parameters are finalized.
    """
    raise NotImplementedError(
        "web3 redeem not implemented yet; provide API merge endpoint or fill this stub"
    )

