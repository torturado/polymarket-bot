"""
Reporting utilities for logging trades, positions, and PnL.
"""
import csv
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

from config import BotConfig
from models import Position, PnLRecord


logger = logging.getLogger(__name__)


class Reporter:
    """Handles logging and CSV reporting of trades and PnL."""

    def __init__(self, config: BotConfig):
        """Initialize the reporter with configuration."""
        self.config = config
        self.csv_file: Optional[Path] = None
        self.csv_writer: Optional[csv.DictWriter] = None

        if config.log_to_csv:
            self._initialize_csv()

    def _initialize_csv(self):
        """Initialize CSV file for logging trades."""
        self.csv_file = Path(self.config.csv_output_path)

        # Create CSV file with headers if it doesn't exist
        if not self.csv_file.exists():
            with open(self.csv_file, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=[
                    'timestamp',
                    'action',
                    'position_id',
                    'pair_id',
                    'entry_up_price',
                    'entry_down_price',
                    'entry_combined_price',
                    'up_size_usd',
                    'down_size_usd',
                    'total_cost_usd',
                    'realized_pnl_usd',
                    'unrealized_pnl_usd',
                    'status'
                ])
                writer.writeheader()

    def log_trade(self, position: Position, action: str, realized_pnl: Optional[float] = None):
        """Log a trade action."""
        message = (
            f"[{action}] Position {position.position_id[:8]}... | "
            f"Pair: {position.pair_id[:16]}... | "
            f"Entry: UP={position.entry_up_price:.4f}, DOWN={position.entry_down_price:.4f} "
            f"(combined={position.entry_combined_price:.4f}) | "
            f"Size: UP=${position.up_size_usd:.2f}, DOWN=${position.down_size_usd:.2f} | "
            f"Cost: ${position.total_cost_usd:.2f}"
        )

        if realized_pnl is not None:
            message += f" | Realized PnL: ${realized_pnl:.2f}"

        logger.info(message)

        # Write to CSV
        if self.config.log_to_csv and self.csv_file:
            self._write_trade_to_csv(position, action, realized_pnl)

    def _write_trade_to_csv(self, position: Position, action: str, realized_pnl: Optional[float]):
        """Write a trade record to CSV."""
        try:
            with open(self.csv_file, 'a', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=[
                    'timestamp',
                    'action',
                    'position_id',
                    'pair_id',
                    'entry_up_price',
                    'entry_down_price',
                    'entry_combined_price',
                    'up_size_usd',
                    'down_size_usd',
                    'total_cost_usd',
                    'realized_pnl_usd',
                    'unrealized_pnl_usd',
                    'status'
                ])

                row = {
                    'timestamp': datetime.now().isoformat(),
                    'action': action,
                    'position_id': position.position_id,
                    'pair_id': position.pair_id,
                    'entry_up_price': f"{position.entry_up_price:.6f}",
                    'entry_down_price': f"{position.entry_down_price:.6f}",
                    'entry_combined_price': f"{position.entry_combined_price:.6f}",
                    'up_size_usd': f"{position.up_size_usd:.2f}",
                    'down_size_usd': f"{position.down_size_usd:.2f}",
                    'total_cost_usd': f"{position.total_cost_usd:.2f}",
                    'realized_pnl_usd': f"{position.realized_pnl_usd:.2f}" if position.realized_pnl_usd else "",
                    'unrealized_pnl_usd': f"{position.unrealized_pnl_usd:.2f}" if position.unrealized_pnl_usd else "",
                    'status': position.status.value if hasattr(position.status, 'value') else str(position.status)
                }

                writer.writerow(row)
        except Exception as e:
            logger.error(f"Failed to write trade to CSV: {e}")

    def log_summary(self, pnl_record: PnLRecord):
        """Log a PnL summary."""
        message = (
            f"[SUMMARY] "
            f"Realized PnL: ${pnl_record.total_realized_pnl_usd:.2f} | "
            f"Unrealized PnL: ${pnl_record.total_unrealized_pnl_usd:.2f} | "
            f"Total PnL: ${pnl_record.total_pnl_usd:.2f} | "
            f"Open Positions: {pnl_record.open_positions_count} | "
            f"Capital Deployed: ${pnl_record.total_capital_deployed:.2f}"
        )
        logger.info(message)

    def save_final_report(self, positions: dict, pnl_history: list[PnLRecord]):
        """Save a final report to JSON."""
        report_path = Path("final_report.json")

        report = {
            'timestamp': datetime.now().isoformat(),
            'summary': {
                'total_positions': len(positions),
                'open_positions': len([p for p in positions.values() if str(p.status) == 'open']),
                'closed_positions': len([p for p in positions.values() if str(p.status) != 'open']),
                'total_realized_pnl': sum(p.realized_pnl_usd for p in positions.values()),
                'total_unrealized_pnl': sum(p.unrealized_pnl_usd for p in positions.values()),
            },
            'positions': [
                {
                    'position_id': p.position_id,
                    'pair_id': p.pair_id,
                    'entry_up_price': p.entry_up_price,
                    'entry_down_price': p.entry_down_price,
                    'entry_combined_price': p.entry_combined_price,
                    'up_size_usd': p.up_size_usd,
                    'down_size_usd': p.down_size_usd,
                    'total_cost_usd': p.total_cost_usd,
                    'realized_pnl_usd': p.realized_pnl_usd,
                    'unrealized_pnl_usd': p.unrealized_pnl_usd,
                    'status': p.status.value if hasattr(p.status, 'value') else str(p.status),
                    'entry_timestamp': p.entry_timestamp.isoformat(),
                    'exit_timestamp': p.exit_timestamp.isoformat() if p.exit_timestamp else None,
                }
                for p in positions.values()
            ],
            'pnl_history': [
                {
                    'timestamp': r.timestamp.isoformat(),
                    'total_realized_pnl_usd': r.total_realized_pnl_usd,
                    'total_unrealized_pnl_usd': r.total_unrealized_pnl_usd,
                    'total_pnl_usd': r.total_pnl_usd,
                    'open_positions_count': r.open_positions_count,
                    'total_capital_deployed': r.total_capital_deployed,
                }
                for r in pnl_history
            ]
        }

        try:
            with open(report_path, 'w') as f:
                json.dump(report, f, indent=2)
            logger.info(f"Final report saved to {report_path}")
        except Exception as e:
            logger.error(f"Failed to save final report: {e}")
