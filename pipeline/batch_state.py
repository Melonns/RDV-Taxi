"""Batch ingestion state management untuk incremental processing.

State file menyimpan metadata:
- last_processed_month: Bulan terakhir yang berhasil diproses (1-12)
- last_processed_year: Tahun terakhir yang berhasil diproses
- total_batches_processed: Jumlah batch yang sudah selesai
- last_update_timestamp: Kapan state terakhir diupdate

Jika no state file exists, mulai dari bulan 1 (January).
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict

logger = logging.getLogger(__name__)

STATE_FILE = Path("./data/pipeline_state.json")
DEFAULT_START_MONTH = 1
DEFAULT_START_YEAR = 2025
DEFAULT_END_MONTH = 6
DEFAULT_END_YEAR = 2025


def init_state_file(start_month: int = DEFAULT_START_MONTH, start_year: int = DEFAULT_START_YEAR):
    """Initialize state file jika belum ada."""
    if not STATE_FILE.exists():
        state = {
            "last_processed_month": start_month - 1,  # Start dari 0, next run ambil bulan 1
            "last_processed_year": start_year,
            "total_batches_processed": 0,
            "last_update_timestamp": None,
        }
        STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(STATE_FILE, "w") as f:
            json.dump(state, f, indent=2)
        logger.info(f"✓ Initialized state file: {STATE_FILE}")


def read_state() -> Dict:
    """Baca current state dari file."""
    init_state_file()
    
    try:
        with open(STATE_FILE, "r") as f:
            state = json.load(f)
        return state
    except Exception as e:
        logger.error(f"Failed to read state file: {e}")
        raise


def write_state(state: Dict):
    """Tulis state ke file."""
    state["last_update_timestamp"] = datetime.now().isoformat()
    
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(state, f, indent=2)
        logger.info(f"✓ Updated state file: {state}")
    except Exception as e:
        logger.error(f"Failed to write state file: {e}")
        raise


def get_next_batch_month() -> Optional[Dict]:
    """Tentukan batch month yang harus diproses berikutnya.
    
    Returns:
        Dict dengan keys:
        - month: Bulan (1-12)
        - year: Tahun
        - is_valid: True jika ada batch yang perlu diproses, False jika sudah selesai
        - reason: Penjelasan status
    """
    state = read_state()
    last_month = state.get("last_processed_month", 0)
    last_year = state.get("last_processed_year", DEFAULT_START_YEAR)
    
    # Determine next batch
    next_month = last_month + 1
    next_year = last_year
    
    # Handle year boundary (setelah Desember, ke tahun berikutnya)
    if next_month > 12:
        next_month = 1
        next_year += 1
    
    # Check jika sudah melewati end date
    if next_year > DEFAULT_END_YEAR or (next_year == DEFAULT_END_YEAR and next_month > DEFAULT_END_MONTH):
        return {
            "month": None,
            "year": None,
            "is_valid": False,
            "reason": f"All batches completed (up to {DEFAULT_END_MONTH}/{DEFAULT_END_YEAR})",
            "total_processed": state.get("total_batches_processed", 0),
        }
    
    return {
        "month": next_month,
        "year": next_year,
        "is_valid": True,
        "reason": f"Processing batch: {next_month:02d}/{next_year}",
        "total_processed": state.get("total_batches_processed", 0),
    }


def mark_batch_complete(month: int, year: int):
    """Mark batch sebagai sudah diproses."""
    state = read_state()
    state["last_processed_month"] = month
    state["last_processed_year"] = year
    state["total_batches_processed"] = state.get("total_batches_processed", 0) + 1
    
    write_state(state)
    logger.info(f"✓ Marked batch {month:02d}/{year} as complete")


def reset_state(start_month: int = DEFAULT_START_MONTH, start_year: int = DEFAULT_START_YEAR):
    """Reset state ke awal (untuk testing/restart)."""
    state = {
        "last_processed_month": start_month - 1,
        "last_processed_year": start_year,
        "total_batches_processed": 0,
        "last_update_timestamp": None,
    }
    write_state(state)
    logger.info("✓ State reset to initial state")


if __name__ == "__main__":
    # Setup logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    
    print("\n=== Batch State Management Demo ===\n")
    
    # Initialize
    init_state_file()
    print(f"Current state: {read_state()}\n")
    
    # Get next batch
    next_batch = get_next_batch_month()
    print(f"Next batch: {next_batch}\n")
    
    # Mark as complete
    if next_batch["is_valid"]:
        mark_batch_complete(next_batch["month"], next_batch["year"])
        print(f"Current state after marking complete: {read_state()}\n")
