#!/usr/bin/env python
"""Quick verification script untuk batch ingestion logic.

Ini test batch month determination tanpa melakukan actual data ingestion.
Perfect untuk validate logic sebelum full pipeline run.
"""

import json
from datetime import datetime, timedelta
from pathlib import Path
from pipeline.batch_state import get_next_batch_month, mark_batch_complete, read_state, reset_state

def test_batch_sequence():
    """Test batch processing untuk 6 bulan (Jan-Jun 2025)."""
    
    print("\n" + "=" * 70)
    print("🧪 BATCH INGESTION VERIFICATION TEST")
    print("=" * 70)
    
    # Reset state ke awal
    print("\n[1] Reset state ke initial...")
    reset_state()
    current = read_state()
    print(f"    ✓ State: {current['last_processed_month']} batches processed")
    
    # Simulate 6 months + extra
    months_data = []
    for i in range(8):  # Run 8 times (6 valid + 2 "no new data")
        print(f"\n[Execution {i+1}]")
        
        batch_info = get_next_batch_month()
        
        if batch_info["is_valid"]:
            month = batch_info["month"]
            year = batch_info["year"]
            print(f"  Status: {batch_info['reason']}")
            print(f"  Date range: {year}-{month:02d}-01 to {year}-{month:02d}-{28 if month == 2 else 30 if month in [4,6,9,11] else 31}")
            print(f"  Action: Would extract {month:02d}/{year} data")
            
            # Simulate processing
            mark_batch_complete(month, year)
            print(f"  ✓ Batch {month:02d}/{year} marked complete")
            
            months_data.append(f"{month:02d}/{year}")
        else:
            print(f"  Status: {batch_info['reason']}")
            print(f"  Action: Skip ingestion (no new data)")
            print(f"  ✓ Pipeline continues gracefully")
        
        current = read_state()
        print(f"  State: {current['total_batches_processed']} total completed")
    
    print("\n" + "-" * 70)
    print("📊 SUMMARY")
    print("-" * 70)
    print(f"Months processed: {', '.join(months_data)}")
    print(f"Total batches: {current['total_batches_processed']}")
    print(f"Last processed: {current['last_processed_month']:02d}/{current['last_processed_year']}")
    print(f"State file: {Path('data/pipeline_state.json').resolve()}")
    
    print("\n✅ VERIFICATION COMPLETE")
    print("=" * 70)
    print("\nNext steps:")
    print("  1. python pipeline/prefect_flow.py              # Manual single run")
    print("  2. python pipeline/prefect_flow.py --mode demo   # Every 5 min")
    print("  3. python pipeline/prefect_flow.py --mode prod   # Daily at 00:00")
    print("=" * 70)

if __name__ == "__main__":
    test_batch_sequence()
