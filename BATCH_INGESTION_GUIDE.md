# 🚀 Batch Ingestion Guide - Incremental Monthly Processing

## Overview

Pipeline telah diubah dari **"process all months at once"** menjadi **"process 1 month per execution"** dengan state tracking dan scheduled runs. Ini mensimulasikan realistic production behavior untuk demonstrasi dan testing.

---

## Architecture

### Sebelumnya (All-at-once)
```
Execution 1: Jan + Feb + Mar + Apr + May + Jun (6 months sekaligus)
↓
Done.
```

### Sekarang (Incremental Batch)
```
Execution 1 (00:00): Check state → Process Jan 2025 → Update state → Done
Execution 2 (00:05): Check state → Process Feb 2025 → Update state → Done
Execution 3 (00:10): Check state → Process Mar 2025 → Update state → Done
Execution 4 (00:15): Check state → Process Apr 2025 → Update state → Done
Execution 5 (00:20): Check state → Process May 2025 → Update state → Done
Execution 6 (00:25): Check state → Process Jun 2025 → Update state → Done
Execution 7 (00:30): Check state → No new batch → Skip ingestion gracefully → Done
Execution 8 (00:35): Check state → No new batch → Skip ingestion gracefully → Done
... (continues indefinitely, ready for new months)
```

---

## Key Components

### 1. **State Management** (`pipeline/batch_state.py`)

**Purpose:** Track "last processed month" untuk determinate next batch

**State File:** `data/pipeline_state.json`

```json
{
  "last_processed_month": 1,
  "last_processed_year": 2025,
  "total_batches_processed": 1,
  "last_update_timestamp": "2026-05-01T01:39:11.612300"
}
```

**Functions:**
- `init_state_file()` - Initialize state jika belum ada
- `read_state()` - Baca state saat ini
- `write_state(state)` - Tulis state ke file
- `get_next_batch_month()` - Tentukan batch mana yang perlu diproses
- `mark_batch_complete(month, year)` - Update state setelah sukses
- `reset_state()` - Reset ke awal (untuk testing)

---

### 2. **Modified Pipeline Flow** (`pipeline/prefect_flow.py`)

**Key Changes:**

1. **Import batch state:**
   ```python
   from pipeline.batch_state import get_next_batch_month, mark_batch_complete, read_state
   ```

2. **State checking di awal pipeline:**
   ```python
   batch_info = get_next_batch_month()
   if not batch_info["is_valid"]:
       # All batches completed - skip ingestion gracefully
       logger.info("✓ All monthly batches processed. Skipping ingestion.")
       skip_weather_ingestion = True
   else:
       # Calculate date range untuk batch bulan ini
       batch_month = batch_info["month"]
       batch_year = batch_info["year"]
       # ... date calculation ...
   ```

3. **State update di akhir pipeline:**
   ```python
   if batch_info["is_valid"]:
       mark_batch_complete(batch_info["month"], batch_info["year"])
   ```

4. **New parameter:** `skip_state_management` untuk bypass state (manual mode)

---

### 3. **Scheduling**

#### A. Demo Mode (Development/Testing)
```bash
python pipeline/prefect_flow.py --mode demo
```
- **Frequency:** Every 5 minutes (`*/5 * * * *`)
- **Use case:** Rapid testing dan simulation
- **Output:** Process 1 month every 5 minutes
- **Timeline:** All 6 months processed in ~30 minutes

#### B. Production Mode (Recommended)
```bash
python pipeline/prefect_flow.py --mode production
```
- **Frequency:** Daily at 00:00 (midnight) (`0 0 * * *`)
- **Use case:** Realistic production deployment
- **Output:** Process 1 month per day
- **Timeline:** All 6 months processed in ~6 days

#### C. Manual Mode (One-time Execution)
```bash
python pipeline/prefect_flow.py
```
- **Frequency:** Single run
- **Use case:** Testing, debugging, or ad-hoc execution
- **Output:** Process next unprocessed month

---

## How It Works

### Execution Flow

1. **[State Check]** Pipeline dimulai dengan membaca `pipeline_state.json`
2. **[Determine Next Batch]**
   - Jika `last_processed_month = 0` → Process January
   - Jika `last_processed_month = 1` → Process February
   - ... dan seterusnya
   - Jika `last_processed_month = 6` dan sudah June → Skip ingestion
3. **[Determine Date Range]**
   - Month 1 (Jan) → 2025-01-01 to 2025-01-31
   - Month 2 (Feb) → 2025-02-01 to 2025-02-28
   - ... dan seterusnya
4. **[Stages 1-3]** Extract, Load, Transform (hanya untuk batch bulan ini)
5. **[Stages 4-5]** Create star schema, Finalize (always run untuk incremental rebuild)
6. **[State Update]** Update state file dengan `last_processed_month = current_month`
7. **[Next Execution]** Tunggu scheduler → repeat

### "No New Data" Handling

Ketika semua bulan sudah processed (Juni selesai):
```
Execution N: 
  → Check state: last_processed_month = 6 (Juni)
  → Determine next: Harus bulan 7, tapi max bulan 6
  → Status: "All batches completed"
  → Action: ✓ Skip ingestion (Stages 1A-C)
           ✓ Still run transformation (Stages 2-5)
           ✓ Continue normal pipeline flow
  → Result: No error, just graceful skip
```

This simulates real production where:
- Data ingestion depends on data availability
- Pipeline continues running even without new data
- Transformation/analytics can still benefit from incremental updates

---

## File Structure

```
RDV-Taxi/
├── pipeline/
│   ├── prefect_flow.py           ← Modified (batch logic + scheduling)
│   ├── batch_state.py            ← NEW (state management)
│   └── ...
├── data/
│   ├── pipeline_state.json       ← NEW (state tracking)
│   ├── final/
│   │   └── tlc.duckdb
│   ├── intermediate/
│   ├── raw/
│   └── ...
└── ...
```

---

## State Tracking Timeline

Example run dengan `--mode production`:

| Time      | Execution | State                     | Action              | Result                |
|-----------|-----------|---------------------------|---------------------|-----------------------|
| Day 1, 00:00 | Run 1     | month=0 → Jan 2025  | Extract Jan, Transform | month→1 ✓             |
| Day 2, 00:00 | Run 2     | month=1 → Feb 2025  | Extract Feb, Transform | month→2 ✓             |
| Day 3, 00:00 | Run 3     | month=2 → Mar 2025  | Extract Mar, Transform | month→3 ✓             |
| Day 4, 00:00 | Run 4     | month=3 → Apr 2025  | Extract Apr, Transform | month→4 ✓             |
| Day 5, 00:00 | Run 5     | month=4 → May 2025  | Extract May, Transform | month→5 ✓             |
| Day 6, 00:00 | Run 6     | month=5 → Jun 2025  | Extract Jun, Transform | month→6 ✓             |
| Day 7, 00:00 | Run 7     | month=6 → No new    | Skip ingestion        | No error, continue ✓  |
| Day 8, 00:00 | Run 8     | month=6 → No new    | Skip ingestion        | No error, continue ✓  |
| ...       | ...       | ...                       | ...                 | ...                   |

---

## Usage Examples

### 1. Single Test Run
```bash
cd /path/to/RDV-Taxi
python pipeline/prefect_flow.py
```
**Output:** Process January 2025, update state

### 2. Rapid Demo (5-minute intervals)
```bash
python pipeline/prefect_flow.py --mode demo
```
**Output:** Start scheduler, process 1 month every 5 minutes

### 3. Production Setup (daily at midnight)
```bash
python pipeline/prefect_flow.py --mode production
```
**Output:** Start scheduler, process 1 month per day

### 4. Check Current State
```bash
python -c "from pipeline.batch_state import read_state; print(read_state())"
```
**Output:**
```
{'last_processed_month': 3, 'last_processed_year': 2025, 'total_batches_processed': 3, ...}
```

### 5. Reset State (for testing)
```bash
python -c "from pipeline.batch_state import reset_state; reset_state()"
```
**Output:** Reset state to initial (month=0)

---

## Logs & Monitoring

### Key Log Messages

**Batch ingestion starting:**
```
[STATE CHECK] Determining next batch month to process
Status: Processing batch: 02/2025
Batch month: 02/2025
Date range: 2025-02-01 to 2025-02-28
```

**No new batches:**
```
[STATE CHECK] Determining next batch month to process
Status: All monthly batches have been processed successfully!
Pipeline will now skip ingestion and proceed to analytics stages.
```

**State update successful:**
```
✓ State updated: Batch 01/2025 marked as complete
Total batches processed so far: 1
Next run will process: 02/2025
```

---

## Testing Checklist

- [ ] `python pipeline/batch_state.py` - State management works
- [ ] `python pipeline/prefect_flow.py` - Manual mode processes 1 month
- [ ] Check `data/pipeline_state.json` created with correct month
- [ ] `python -c "from pipeline.batch_state import get_next_batch_month; print(get_next_batch_month())"` - Shows Feb next
- [ ] Run again - February processed, state updates
- [ ] Continue until June
- [ ] Run after June - "No new batches" message appears, graceful skip
- [ ] `python pipeline/prefect_flow.py --mode demo` - 5-minute schedule appears
- [ ] `python pipeline/prefect_flow.py --mode production` - Daily schedule appears

---

## Benefits

✅ **Resumable:** Jika pipeline crash, next run lanjut dari batch yang sama (idempotent)

✅ **Scalable:** Mudah adjust scheduling sesuai data availability

✅ **Realistic:** Simulasi production behavior yang sebenarnya

✅ **Traceable:** State file untuk audit dan debugging

✅ **Graceful:** Handle "no new data" tanpa error

✅ **Incremental:** Star schema rebuild dengan new data setiap batch

---

## Configuration

To customize:

1. **Change batch frequency:** Edit `cron` in `--mode demo` atau `--mode production`
   - Demo: `*/5 * * * *` (5 min) → `*/10 * * * *` (10 min)
   - Production: `0 0 * * *` (daily) → `0 0 1 * *` (monthly)

2. **Change date range:** Edit `DEFAULT_END_MONTH` di `batch_state.py`
   - Current: 6 (June) → Can set to 12 (December)

3. **Change state file location:** Edit `STATE_FILE` path di `batch_state.py`

---

## Architecture Diagram

```
                    ┌─────────────────────────────────────┐
                    │  Scheduler (Prefect)                │
                    │  - Mode: Demo (5 min)               │
                    │  - Mode: Production (daily 00:00)   │
                    │  - Mode: Manual (once)              │
                    └────────────┬────────────────────────┘
                                 │
                                 ▼
                    ┌─────────────────────────────────────┐
                    │  Pipeline Start                     │
                    │  main_pipeline()                    │
                    └────────────┬────────────────────────┘
                                 │
                                 ▼
                    ┌─────────────────────────────────────┐
                    │  [STATE CHECK]                      │
                    │  Read: data/pipeline_state.json     │
                    │  Determine: next_batch_month        │
                    └────────────┬────────────────────────┘
                                 │
                    ┌────────────┴─────────────┐
                    │                          │
                    ▼                          ▼
        ┌──────────────────────┐  ┌──────────────────────┐
        │  Batch Valid?        │  │  No New Batch?       │
        │  YES: Jan-Jun data   │  │  YES: Skip ingestion │
        └────────┬─────────────┘  └────────┬─────────────┘
                 │                         │
                 ▼                         ▼
        ┌──────────────────────┐  ┌──────────────────────┐
        │  [STAGES 1A-C]       │  │  [STAGES 2-5]        │
        │  Extract batch month │  │  (Transformation     │
        │  from sources        │  │   only, incremental) │
        └────────┬─────────────┘  │                      │
                 │                └────────┬─────────────┘
                 ▼                         │
        ┌──────────────────────┐          │
        │  [STAGES 2-5]        │          │
        │  Load & Transform    │          │
        └────────┬─────────────┘          │
                 │                        │
                 └────────────┬───────────┘
                              │
                              ▼
                    ┌─────────────────────────────────────┐
                    │  [STATE UPDATE]                     │
                    │  Write: last_processed_month = N    │
                    │  Increment: total_batches_processed │
                    └────────────┬────────────────────────┘
                                 │
                                 ▼
                    ┌─────────────────────────────────────┐
                    │  Pipeline Success ✓                │
                    │  Ready for next scheduler trigger   │
                    └─────────────────────────────────────┘
```

---

## Troubleshooting

**Q: Pipeline always processes the same month?**
A: Check `data/pipeline_state.json` exists. If not, state wasn't updated. 
   Ensure `skip_state_management=False` (default).

**Q: Want to restart from January?**
A: `python -c "from pipeline.batch_state import reset_state; reset_state()"`

**Q: Scheduler not running?**
A: Check `prefect dashboard open` to see runs. Or check terminal logs.

**Q: How to process multiple batches manually?**
A: Run `python pipeline/prefect_flow.py` repeatedly (once per month).

---

## Next Steps

1. ✅ Implementation complete
2. Test with `python pipeline/prefect_flow.py --mode demo` (5-min scheduling)
3. Monitor logs and `data/pipeline_state.json` updates
4. Once satisfied, switch to `--mode production` for daily scheduling
5. Monitor for ~6 days until all months processed
6. Verify graceful "no new data" behavior on day 7+

