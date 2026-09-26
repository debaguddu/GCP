import json
import os
import nbformat
from nbconvert.preprocessors import ExecutePreprocessor

def build_notebook_4():
    nb = nbformat.v4.new_notebook()
    
    nb.metadata = {
        "kernelspec": {
            "display_name": "Python 3",
            "language": "python",
            "name": "python3"
        },
        "language_info": {
            "codemirror_mode": {
                "name": "ipython",
                "version": 3
            },
            "file_extension": ".py",
            "mimetype": "text/x-python",
            "name": "python",
            "nbconvert_exporter": "python",
            "pygments_lexer": "ipython3",
            "version": "3.12"
        }
    }

    cells = []

    # Cell 1: Markdown - Title & Architecture
    cell_1_md = """# Apache Beam: Financial Transactions Enrichment via Side Inputs & Sliding Window Rolling Averages
## Advanced Stream Processing, Side Input Joins, and PySpark Broadcast Contrast

This notebook implements an end-to-end **Apache Beam Python pipeline** designed for financial fraud detection and risk analytics:
1. **Main Ingestion**: Ingests high-volume financial transactions from `credit_card_fraud_10k.csv`.
2. **Side Input Reference Data**: Ingests a smaller reference dataset of merchant category risk scores (`merchant_risk_scores.csv`).
3. **Side Input Enrichment**: Uses Beam's **Side Input** pattern (`beam.pvalue.AsDict`) to enrich transactions with merchant risk scores without requiring an expensive distributed shuffle join (`CoGroupByKey`).
4. **Native Event Timestamp Assignment**: Assigns continuous event-time timestamps (`TimestampedValue`) derived from transaction hours and sequence.
5. **Sliding Window Aggregation**: Implements a **Sliding Window of 2 Hours with a 30-Minute Slide Period** (`SlidingWindows(size=7200, period=1800)`) to calculate a continuous **rolling average of transaction 'amount' per cardholder**.
6. **Architectural Deep Dives**:
   - In-depth comparison of **Beam Side Inputs vs. PySpark Broadcast Variables**.
   - Step-by-step breakdown of the **Sliding Window implementation mechanics**.

---

### Pipeline Architecture:

```
┌────────────────────────────────────────────────────────┐   ┌────────────────────────────────────────────────────────┐
│ Main Transactions: credit_card_fraud_10k.csv (10k rows)│   │  Reference Risk Data: merchant_risk_scores.csv (5 rows)│
└───────────────────────────┬────────────────────────────┘   └───────────────────────────┬────────────────────────────┘
                            │                                                            │
                            ▼ (beam.io.ReadFromText)                                     ▼ (beam.io.ReadFromText)
                Raw Text Lines PCollection                                  Raw Risk Lines PCollection
                            │                                                            │
                            ▼ (beam.ParDo - ParseTransactionDoFn)                        ▼ (beam.ParDo - ParseMerchantRiskDoFn)
                 Transactions PCollection                                   KV Pairs: (category, risk_score)
                            │                                                            │
                            │ ◄──────────────────────── Side Input: AsDict(risk_pcoll) ──┘
                            ▼
                ┌────────────────────────────────────────────────────────┐
                │ EnrichTransactionWithRiskDoFn(tx, risk_dict)           │
                │ -> Enriches transaction with risk score & tier         │
                │ -> Assigns native event timestamp (TimestampedValue)   │
                └───────────────────────────┬────────────────────────────┘
                                            │
                                            ▼ (beam.WindowInto - SlidingWindows: size=2h, period=30m)
                ┌────────────────────────────────────────────────────────┐
                │ Windowed PCollection: Elements in overlapping windows  │
                └───────────────────────────┬────────────────────────────┘
                                            │
                                            ▼ (beam.Map - (cardholder_id, tx))
                                            ▼ (beam.GroupByKey - groups per (cardholder_id, Window)!)
                ┌────────────────────────────────────────────────────────┐
                │ Windowed Cardholder Sessions: (cardholder_id, [txs])   │
                └───────────────────────────┬────────────────────────────┘
                                            │
                                            ▼ (beam.ParDo - ComputeRollingAverageDoFn)
                                            │ -> Computes rolling avg amount, total amount, tx count
                                            │ -> Injects window boundaries via beam.DoFn.WindowParam
                                            ▼
                ┌────────────────────────────────────────────────────────┐
                │ Rolling Average Summary Records (JSON Lines)           │
                └───────────────────────────┬────────────────────────────┘
                                            │ (beam.io.WriteToText)
                                            ▼
                ┌────────────────────────────────────────────────────────┐
                │ Sink: local_output/rolling_cardholder_summary-*.json   │
                └────────────────────────────────────────────────────────┘
```
"""
    cells.append(nbformat.v4.new_markdown_cell(cell_1_md))

    # Cell 2: Code - Environment & Imports
    cell_2_code = """# Cell 1: Environment verification and library imports
import os
import sys
import csv
import json
import datetime
import glob
from typing import Dict, List, Tuple, Any, Generator

import apache_beam as beam
from apache_beam import window
from apache_beam import pvalue
from apache_beam.options.pipeline_options import PipelineOptions

print(f"Python Executable: {sys.executable}")
print(f"Apache Beam Version: {beam.__version__}")
"""
    cells.append(nbformat.v4.new_code_cell(cell_2_code))

    # Cell 3: Markdown - Step 1: Datasets Setup
    cell_3_md = """---
## 1. Source Data Verification & Reference Dataset Setup

We verify two datasets:
1. **Main Transactions Dataset (`credit_card_fraud_10k.csv`)**: Contains 10,000 credit card transaction records with fields:
   `transaction_id, amount, transaction_hour, merchant_category, foreign_transaction, location_mismatch, device_trust_score, velocity_last_24h, cardholder_age, is_fraud`.
2. **Smaller Reference Dataset (`merchant_risk_scores.csv`)**: A lookup reference table mapping merchant categories to standardized risk scores and tiers (`Travel: 0.85`, `Electronics: 0.72`, `Clothing: 0.40`, `Food: 0.25`, `Grocery: 0.15`).
"""
    cells.append(nbformat.v4.new_markdown_cell(cell_3_md))

    # Cell 4: Code - Data Inspection & Setup
    cell_4_code = """# Cell 2: Verify main transactions dataset and create/verify merchant risk dataset

# Path resolution for main transactions dataset
tx_candidates = [
    "../SourceFiles/credit_card_fraud_10k.csv",
    "SourceFiles/credit_card_fraud_10k.csv",
    os.path.abspath(os.path.join(os.getcwd(), "..", "SourceFiles", "credit_card_fraud_10k.csv")),
    os.path.abspath(os.path.join(os.getcwd(), "SourceFiles", "credit_card_fraud_10k.csv"))
]
tx_file_path = next((f for f in tx_candidates if os.path.exists(f)), None)

if not tx_file_path:
    raise FileNotFoundError("Could not find 'credit_card_fraud_10k.csv' in SourceFiles directory!")

source_dir = os.path.dirname(os.path.abspath(tx_file_path))
risk_file_path = os.path.join(source_dir, "merchant_risk_scores.csv")

# Create merchant_risk_scores.csv if not present
if not os.path.exists(risk_file_path):
    print(f"Generating reference dataset at: {risk_file_path}")
    risk_records = [
        ["merchant_category", "risk_score", "risk_level"],
        ["Travel", "0.85", "HIGH"],
        ["Electronics", "0.72", "HIGH"],
        ["Clothing", "0.40", "MEDIUM"],
        ["Food", "0.25", "LOW"],
        ["Grocery", "0.15", "LOW"]
    ]
    with open(risk_file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerows(risk_records)

print(f"Main Transactions File: {tx_file_path} ({os.path.getsize(tx_file_path):,} bytes)")
print(f"Merchant Risk File:     {risk_file_path} ({os.path.getsize(risk_file_path):,} bytes)\\n")

# Display samples
print("--- Sample Transactions (First 3 Lines) ---")
with open(tx_file_path, "r", encoding="utf-8") as f:
    for _ in range(4):
        print(f.readline().strip())

print("\\n--- Reference Merchant Risk Table ---")
with open(risk_file_path, "r", encoding="utf-8") as f:
    for line in f:
        print(line.strip())
"""
    cells.append(nbformat.v4.new_code_cell(cell_4_code))

    # Cell 5: Markdown - Step 2: DoFns and Side Inputs
    cell_5_md = """---
## 2. Custom Transformation Classes (`DoFn`) & Side Input Pattern

### 1. `ParseMerchantRiskDoFn`:
- Parses `merchant_risk_scores.csv` and emits Key-Value tuples: `(merchant_category, risk_score)`.
- Downstream in the pipeline, this `PCollection` is wrapped with **`beam.pvalue.AsDict()`** to convert it into a dictionary view passed as a side input.

### 2. `ParseTransactionDoFn`:
- Parses raw CSV lines from `credit_card_fraud_10k.csv`.
- Maps transactions to cardholder IDs: `cardholder_id = f"CH_{(tx_id % 100) + 1:03d}"` (partitioning 10,000 transactions across 100 distinct cardholders).
- Calculates a continuous event-time epoch timestamp based on `transaction_hour` and sequence.

### 3. `EnrichTransactionWithRiskDoFn` (Side Input Consumer):
- Takes the main transaction `PCollection` element as its primary input.
- Receives the side input dictionary `risk_dict=pvalue.AsDict(risk_pcoll)` as a keyword argument.
- Performs an $O(1)$ in-memory dictionary lookup: `risk_score = risk_dict.get(category, 0.50)`.
- Attaches the risk score and emits **`window.TimestampedValue(enriched_record, timestamp)`** to enable event-time sliding windowing.

### 4. `ComputeRollingAverageDoFn`:
- Applied after grouping by cardholder and window: `(cardholder_id, Iterable[transactions])`.
- Injects **`win=beam.DoFn.WindowParam`** to inspect the 2-hour window boundaries (`win.start`, `win.end`).
- Computes the **rolling average transaction amount**, **total rolling expenditure**, **transaction frequency**, and **average merchant risk score** within that sliding interval.
"""
    cells.append(nbformat.v4.new_markdown_cell(cell_5_md))

    # Cell 6: Code - DoFn Implementations
    cell_6_code = """# Cell 3: Define Custom Transformation Functions and DoFns

class ParseMerchantRiskDoFn(beam.DoFn):
    \"\"\"Parses the reference CSV into (merchant_category, risk_score) Key-Value pairs.\"\"\"
    def process(self, line: str):
        line = line.strip()
        if not line or line.startswith("merchant_category"):
            return
        row = list(csv.reader([line]))[0]
        if len(row) >= 2:
            category = row[0].strip()
            score = float(row[1].strip())
            yield (category, score)


class ParseTransactionDoFn(beam.DoFn):
    \"\"\"
    Parses transaction CSV lines, casts numeric columns, assigns cardholder IDs,
    and computes event epoch timestamps.
    \"\"\"
    def process(self, line: str):
        line = line.strip()
        if not line or line.startswith("transaction_id"):
            return
        
        row = list(csv.reader([line]))[0]
        if len(row) >= 10:
            tx_id = int(row[0].strip())
            amount = float(row[1].strip())
            hour = int(row[2].strip())
            category = row[3].strip()
            foreign = int(row[4].strip())
            mismatch = int(row[5].strip())
            device_trust = float(row[6].strip())
            cardholder_age = int(row[8].strip())
            is_fraud = int(row[9].strip())
            
            # Map transactions to 100 distinct cardholders
            cardholder_id = f"CARDHOLDER_{(tx_id % 100) + 1:03d}"
            
            # Establish continuous event-time timeline (Base date: 2024-06-01 00:00:00 UTC)
            base_epoch = 1717200000  # 2024-06-01 00:00:00 UTC
            minute = (tx_id * 7) % 60
            second = (tx_id * 13) % 60
            epoch_timestamp = base_epoch + (hour * 3600) + (minute * 60) + second
            
            yield {
                "transaction_id": tx_id,
                "amount": amount,
                "transaction_hour": hour,
                "merchant_category": category,
                "foreign_transaction": foreign,
                "location_mismatch": mismatch,
                "device_trust_score": device_trust,
                "cardholder_age": cardholder_age,
                "is_fraud": is_fraud,
                "cardholder_id": cardholder_id,
                "epoch_timestamp": epoch_timestamp
            }


class EnrichTransactionWithRiskDoFn(beam.DoFn):
    \"\"\"
    Enriches main transaction records using the merchant risk scores dictionary
    passed as a side input (beam.pvalue.AsDict).
    Attaches native Beam event timestamps using window.TimestampedValue.
    \"\"\"
    def process(self, transaction: dict, risk_dict: dict):
        tx = dict(transaction)
        category = tx["merchant_category"]
        
        # O(1) in-memory side input dictionary lookup
        risk_score = risk_dict.get(category, 0.50)
        tx["merchant_risk_score"] = risk_score
        tx["risk_tier"] = "HIGH" if risk_score >= 0.70 else ("MEDIUM" if risk_score >= 0.35 else "LOW")
        
        # Attach native Beam event timestamp for sliding windowing
        yield window.TimestampedValue(tx, tx["epoch_timestamp"])


class ComputeRollingAverageDoFn(beam.DoFn):
    \"\"\"
    Computes rolling metrics per cardholder across the sliding window.
    Extracts sliding window boundaries via beam.DoFn.WindowParam.
    \"\"\"
    def process(self, element, win=beam.DoFn.WindowParam):
        cardholder_id, txs_iter = element
        txs = list(txs_iter)
        if not txs:
            return
            
        amounts = [t["amount"] for t in txs]
        risks = [t["merchant_risk_score"] for t in txs]
        fraud_flags = [t["is_fraud"] for t in txs]
        
        # Format window boundaries in UTC
        win_start = datetime.datetime.fromtimestamp(float(win.start), tz=datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        win_end = datetime.datetime.fromtimestamp(float(win.end), tz=datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        
        record = {
            "cardholder_id": cardholder_id,
            "window_start": win_start,
            "window_end": win_end,
            "window_size_hours": 2,
            "slide_period_minutes": 30,
            "transaction_count": len(amounts),
            "total_amount": round(sum(amounts), 2),
            "rolling_avg_amount": round(sum(amounts) / len(amounts), 2),
            "max_single_amount": round(max(amounts), 2),
            "avg_merchant_risk": round(sum(risks) / len(risks), 3),
            "fraud_events_in_window": sum(fraud_flags)
        }
        
        yield json.dumps(record, ensure_ascii=False)

print("Custom DoFn classes and side input enrichment logic defined successfully.")
"""
    cells.append(nbformat.v4.new_code_cell(cell_6_code))

    # Cell 7: Markdown - Step 3: Pipeline Execution
    cell_7_md = """---
## 3. Pipeline Construction & Execution (DirectRunner)

### Sliding Window Configuration:
- **Window Size**: `7,200 seconds` (2 Hours)
- **Slide Period**: `1,800 seconds` (30 Minutes)
- **Overlap Ratio**: $\\frac{7200}{1800} = 4$. Every transaction contributes to **4 overlapping sliding windows**, enabling a smooth rolling metric calculation updated every 30 minutes!
"""
    cells.append(nbformat.v4.new_markdown_cell(cell_7_md))

    # Cell 8: Code - Execute Pipeline
    cell_8_code = """# Cell 4: Construct and execute the Apache Beam pipeline locally

output_dir = "local_output/rolling_cardholder_summary"
os.makedirs(output_dir, exist_ok=True)
output_prefix = os.path.join(output_dir, "cardholder_rolling_avg")

print(f"Reading main transactions from: {tx_file_path}")
print(f"Reading merchant risk data from: {risk_file_path}")
print(f"Writing rolling summary to:     {output_prefix}-*.json")
print("\\nExecuting pipeline with DirectRunner...")

with beam.Pipeline(runner="DirectRunner") as p:
    # 1. Read side input reference dataset into a PCollection
    merchant_risk_pcoll = (
        p
        | "1a_ReadRiskScores" >> beam.io.ReadFromText(risk_file_path)
        | "1b_ParseRiskScores" >> beam.ParDo(ParseMerchantRiskDoFn())
    )
    
    # 2. Read and parse main transactions
    main_transactions = (
        p
        | "2a_ReadMainTransactions" >> beam.io.ReadFromText(tx_file_path)
        | "2b_ParseTransactions" >> beam.ParDo(ParseTransactionDoFn())
    )
    
    # 3. Enrich main transactions with merchant risk scores via Side Input
    enriched_transactions = (
        main_transactions
        | "3_EnrichWithSideInput" >> beam.ParDo(
            EnrichTransactionWithRiskDoFn(),
            risk_dict=pvalue.AsDict(merchant_risk_pcoll)  # Side Input View
        )
    )
    
    # 4. Apply Sliding Windows (Size: 2 Hours, Slide: 30 Minutes)
    windowed_transactions = (
        enriched_transactions
        | "4_SlidingWindow2h30m" >> beam.WindowInto(
            window.SlidingWindows(size=7200, period=1800)
        )
    )
    
    # 5. Key by Cardholder ID and calculate rolling average
    rolling_averages = (
        windowed_transactions
        | "5a_KeyByCardholder" >> beam.Map(lambda tx: (tx["cardholder_id"], tx))
        | "5b_GroupPerCardholderWindow" >> beam.GroupByKey()
        | "5c_ComputeRollingMetrics" >> beam.ParDo(ComputeRollingAverageDoFn())
    )
    
    # 6. Sink results to JSON output files
    rolling_averages | "6_WriteJSONOutput" >> beam.io.WriteToText(
        file_path_prefix=output_prefix,
        file_name_suffix=".json",
        shard_name_template="-SSSSS-of-NNNNN"
    )

print("Pipeline execution completed successfully!")
"""
    cells.append(nbformat.v4.new_code_cell(cell_8_code))

    # Cell 9: Code - Inspect Output
    cell_9_code = """# Cell 5: Inspect and validate generated rolling average summary records

output_files = glob.glob(f"{output_prefix}*.json")
print(f"Generated output partition file(s): {output_files}\\n")

rolling_records = []
for file_path in output_files:
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                rolling_records.append(json.loads(line.strip()))

# Sort by Cardholder ID and Window Start time
rolling_records.sort(key=lambda r: (r["cardholder_id"], r["window_start"]))

print(f"Total windowed cardholder evaluations generated: {len(rolling_records):,}\\n")

print("--- Sample Rolling Window Records (Cardholder 001 Over Time) ---")
ch_001_records = [r for r in rolling_records if r["cardholder_id"] == "CARDHOLDER_001"][:5]
print(json.dumps(ch_001_records, indent=2))

print("\\n--- High-Spending Cardholder Rolling Averages (Over $500 Avg) ---")
high_spenders = [r for r in rolling_records if r["rolling_avg_amount"] > 500.0][:3]
print(json.dumps(high_spenders, indent=2))
"""
    cells.append(nbformat.v4.new_code_cell(cell_9_code))

    # Cell 10: Markdown - Deep Dive: Side Inputs vs PySpark Broadcast
    cell_10_md = """---
## 4. Architectural Deep Dive: Apache Beam Side Inputs vs. PySpark Broadcast Variables

Both Apache Beam and Apache Spark provide mechanisms to distribute smaller reference datasets to worker nodes to enrich high-volume streaming or batch data. However, their internal execution models, state lifetimes, and streaming capabilities differ fundamentally.

---

### 1. Comparison Matrix: Apache Beam Side Inputs vs. PySpark Broadcast Variables

| Architectural Dimension | Apache Beam (`beam.pvalue.AsDict`) | PySpark (`sc.broadcast()`) |
| :--- | :--- | :--- |
| **Origin & Abstraction** | **Dynamic PCollection View**: Created from any pipeline transform (`pvalue.AsDict`, `AsList`, `AsIter`, `AsSingleton`, `AsMultimap`). | **Static Driver Variable**: Created exclusively on the Spark Driver node from local JVM/Python memory (`sc.broadcast(data)`). |
| **Streaming & Windowing Capability** | **Windowed Side Inputs (Dynamic Temporal Joins)**: Side inputs can be windowed alongside main inputs. Beam automatically maps main input windows to corresponding side input windows (`WindowMappingFn`). | **Static Snapshot (Immutable)**: Cannot be updated dynamically mid-stream. In Structured Streaming, broadcast variables remain fixed to their initial broadcast state. |
| **Worker Materialization & Caching** | **Lazy On-Demand Caching**: Workers pull side input views lazily from the runner's storage/state backend when the first element requires it. | **Eager Peer-to-Peer Distribution**: Pushed eagerly to executors via BitTorrent-like chunked protocol upon invocation. |
| **Driver Node Memory Footprint** | **Zero Driver Bottleneck**: The side input data is processed distributedly in worker memory and pipeline state; the driver never holds the full dataset. | **Driver Bottleneck Risk**: The entire reference dataset must fit comfortably in the Spark Driver process memory before broadcasting. |
| **Data Size Flexibility** | **Medium to Large Reference Data**: Supports iterable and paginated access (`AsIter`) allowing side inputs that exceed single-machine RAM. | **Strictly In-Memory**: Limited strictly by executor heap/off-heap RAM. Broadcasting >1GB can cause GC pauses and driver OOM. |
| **Execution Portability** | Runs uniformly across Google Cloud Dataflow, Apache Flink, Apache Spark, or local DirectRunner. | Tightly bound to the Apache Spark execution engine and SparkContext. |

---

### 2. The Power of Beam's Windowed Side Inputs (Temporal Joins)

In PySpark, broadcasting a lookup table is strictly a static point-in-time operation:
```python
# PySpark Broadcast (Static snapshot)
risk_broadcast = spark.sparkContext.broadcast(risk_dict)
df.rdd.map(lambda row: enrich(row, risk_broadcast.value))
```
If merchant risk scores change every hour (e.g. fluctuating fraud risk index), PySpark Structured Streaming cannot natively update the broadcast variable on an hourly schedule without restarting the query or performing complex stateful stream-stream joins.

In **Apache Beam**, side inputs can have **windows**:
```python
# Apache Beam Windowed Side Input
hourly_risk = (
    p 
    | "ReadRiskStream" >> beam.io.ReadFromPubSub(...)
    | "HourlyWindow"   >> beam.WindowInto(window.FixedWindows(3600))
    | "RiskDict"       >> beam.combiners.ToDict()
)

enriched = (
    transactions
    | "TxHourlyWindow" >> beam.WindowInto(window.FixedWindows(3600))
    | "Enrich"         >> beam.ParDo(EnrichDoFn(), risk_map=pvalue.AsDict(hourly_risk))
)
```
- When a transaction from window `[10:00, 11:00)` enters `EnrichDoFn`, Beam's **`WindowMappingFn`** automatically fetches the side input view belonging to that **exact same window `[10:00, 11:00)`**!
- This gives Beam unmatched power for temporal lookups, dynamic currency conversion, and real-time risk index updates in streaming pipelines.
"""
    cells.append(nbformat.v4.new_markdown_cell(cell_10_md))

    # Cell 11: Markdown - Step-by-Step Sliding Window Breakdown
    cell_11_md = """---
## 5. Technical Breakdown: Step-by-Step Sliding Window Implementation

A **Sliding Window** (also known as a **Hopping Window**) partitions data into fixed-duration intervals that advance periodically.

Unlike Fixed (Tumbling) Windows where every element belongs to exactly **one** window, an element in a Sliding Window belongs to **multiple concurrent overlapping windows**.

---

### Step-by-Step Execution Mechanics:

#### Step 1: Assigning Native Event Timestamps
Elements must carry an event timestamp within Beam's metadata wrapper:
```python
yield window.TimestampedValue(transaction_dict, epoch_timestamp_seconds)
```
Without native timestamps, Beam defaults to processing time (`MIN_TIMESTAMP` in batch), which prevents meaningful temporal windowing.

---

#### Step 2: Defining Sliding Window Parameters
```python
beam.WindowInto(window.SlidingWindows(size=7200, period=1800))
```
- **Window Size ($S$)**: Duration of each window interval ($7,200\\text{ seconds} = 2\\text{ Hours}$).
- **Slide Period ($P$)**: How frequently a new window starts ($1,800\\text{ seconds} = 30\\text{ Minutes}$).
- **Overlap Factor ($N$)**:
  $$N = \\frac{S}{P} = \\frac{7200}{1800} = 4$$
  Every individual transaction is assigned to **4 concurrent overlapping windows**!

---

#### Step 3: Mathematical Window Assignment
For an element with event timestamp $t$, Beam computes all window start times $W_{\\text{start}}$ satisfying:
$$t - S < W_{\\text{start}} \\le t \\quad \\text{and} \\quad W_{\\text{start}} \\equiv 0 \\pmod{P}$$
For example, a transaction occurring at `10:15:00 UTC`:
- **Window 1**: `[08:30:00, 10:30:00)`
- **Window 2**: `[09:00:00, 11:00:00)`
- **Window 3**: `[09:30:00, 11:30:00)`
- **Window 4**: `[10:00:00, 12:00:00)`

```
Timeline:    08:30    09:00    09:30    10:00    10:30    11:00    11:30    12:00
Win 1:       [─────────────────────────────)
Win 2:                [─────────────────────────────)
Win 3:                         [─────────────────────────────)
Win 4:                                  [─────────────────────────────)
Tx @ 10:15:                                  * (Captured by all 4 windows!)
```

---

#### Step 4: Keying by Cardholder ID
```python
beam.Map(lambda tx: (tx["cardholder_id"], tx))
```
We structure elements as Key-Value pairs: `(cardholder_id, transaction_dict)`.

---

#### Step 5: Windowed Grouping & Aggregation
```python
beam.GroupByKey()
```
- In Apache Beam, grouping operations are strictly evaluated per **(Key, Window)**:
  $$\\text{Grouping Key} = (\\text{cardholder\\_id}, \\text{IntervalWindow})$$
- Each worker node receives the list of all transactions executed by that cardholder inside that specific 2-hour window slice.

---

#### Step 6: Window Metadata Extraction via `DoFn.WindowParam`
```python
class ComputeRollingAverageDoFn(beam.DoFn):
    def process(self, element, win=beam.DoFn.WindowParam):
        cardholder_id, txs = element
        # win.start and win.end provide the exact window boundary timestamps
```
By declaring `win=beam.DoFn.WindowParam` in the `process()` method arguments, Beam injects the `IntervalWindow` object containing:
- `win.start`: Microsecond start boundary of the window.
- `win.end`: Microsecond end boundary of the window.
- `win.max_timestamp()`: Latest possible timestamp for elements in this window.

---

#### Step 7: Output Sinking & Watermark Eviction
Once the watermark passes `win.end`, the runner emits the final rolling average record and automatically evicts the window's state from worker memory to avoid resource leakage.
"""
    cells.append(nbformat.v4.new_markdown_cell(cell_11_md))

    # Cell 12: Code - PySpark Equivalent Reference
    cell_12_code = """# Cell 6: Complete PySpark Equivalent Implementation (Reference)
# This code snippet demonstrates how the exact same side-input enrichment and sliding window
# rolling average calculation is expressed in PySpark:

pyspark_equivalent_code = \"\"\"
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, broadcast, window, to_timestamp, from_unixtime,
    round as spark_round, avg as spark_avg, sum as spark_sum,
    count as spark_count, max as spark_max, to_json, struct
)

# 1. Initialize SparkSession
spark = SparkSession.builder \\
    .appName("FraudRollingAverageEnrichment") \\
    .master("local[*]") \\
    .getOrCreate()

# 2. Read smaller dataset and broadcast it (PySpark Broadcast Variable)
risk_df = spark.read.option("header", "true").csv("SourceFiles/merchant_risk_scores.csv")
risk_df_typed = risk_df.select(
    col("merchant_category"),
    col("risk_score").cast("double"),
    col("risk_level")
)

# 3. Read main transactions
tx_df = spark.read.option("header", "true").csv("SourceFiles/credit_card_fraud_10k.csv")
tx_typed = tx_df.select(
    col("transaction_id").cast("long"),
    col("amount").cast("double"),
    col("transaction_hour").cast("int"),
    col("merchant_category"),
    col("is_fraud").cast("int")
)

# 4. Enrich main transactions using Broadcast Join
enriched_df = tx_typed.join(
    broadcast(risk_df_typed),  # Broadcast Side Input equivalent in Spark
    on="merchant_category",
    how="left"
)

# 5. Derive cardholder_id and continuous event timestamp
enriched_with_time = enriched_df.withColumn(
    "cardholder_id", 
    concat(lit("CARDHOLDER_"), lpad((col("transaction_id") % 100 + 1).cast("string"), 3, "0"))
).withColumn(
    "event_timestamp",
    to_timestamp(from_unixtime(lit(1717200000) + col("transaction_hour") * 3600 + (col("transaction_id") * 7 % 60) * 60))
)

# 6. Apply Sliding Window (Duration: 2 Hours, Slide: 30 Minutes)
windowed_rolling = enriched_with_time.groupBy(
    col("cardholder_id"),
    window(col("event_timestamp"), "2 hours", "30 minutes")  # PySpark Sliding Window
).agg(
    spark_count("amount").alias("transaction_count"),
    spark_round(spark_avg("amount"), 2).alias("rolling_avg_amount"),
    spark_round(spark_sum("amount"), 2).alias("total_amount"),
    spark_round(spark_max("amount"), 2).alias("max_single_amount"),
    spark_round(spark_avg("risk_score"), 3).alias("avg_merchant_risk")
)

# 7. Format as JSON Lines output
json_output = windowed_rolling.select(
    to_json(struct(col("*"))).alias("value")
)
json_output.write.mode("overwrite").text("local_output/pyspark_cardholder_rolling_avg.json")

print("PySpark pipeline executed successfully.")
\"\"\"

print("PySpark equivalent pipeline logic registered.")
print("Side-by-side contrast between Beam's Side Input / SlidingWindows and PySpark's Broadcast / Window() is complete.")
"""
    cells.append(nbformat.v4.new_code_cell(cell_12_code))

    nb.cells = cells
    return nb

def main():
    print("Building notebook 4.ipynb structure...")
    nb = build_notebook_4()
    
    notebook_dir = os.path.join(os.getcwd(), "PythonNotebook")
    os.makedirs(notebook_dir, exist_ok=True)
    target_ipynb = os.path.join(notebook_dir, "4.ipynb")
    
    with open(target_ipynb, "w", encoding="utf-8") as f:
        nbformat.write(nb, f)
    print(f"Saved initial notebook to {target_ipynb}")
    
    print("Executing 4.ipynb with nbconvert to populate real cell outputs...")
    ep = ExecutePreprocessor(timeout=600, kernel_name="python3")
    try:
        ep.preprocess(nb, {"metadata": {"path": notebook_dir}})
        print("Notebook 4.ipynb executed successfully!")
    except Exception as e:
        print(f"Execution notice: {e}")
        
    with open(target_ipynb, "w", encoding="utf-8") as f:
        nbformat.write(nb, f)
    print(f"Final executed notebook saved at {target_ipynb}")

if __name__ == "__main__":
    main()
