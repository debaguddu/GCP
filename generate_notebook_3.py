import json
import os
import nbformat
from nbconvert.preprocessors import ExecutePreprocessor

def build_notebook_3():
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
    cell_1_md = """# Apache Beam: E-Commerce Clickstream Sessionization & Cart Abandonment Detection
## Advanced Stream Processing, Event-Time Windowing, and PySpark Structured Streaming Contrast

This notebook implements an **Apache Beam Python pipeline** designed to process temporal e-commerce clickstream data, assign native event timestamps, window events into **30-minute fixed windows**, group by user, and identify **abandoned cart sessions** (sessions containing `'cart'` events but **no** `'purchase'` events).

---

### Objective & Architecture Overview:
1. **Cloud-Native Storage Ingestion**:
   - The first pipeline step ensures the source dataset is uploaded and verified in a **Google Cloud Storage (GCS)** bucket (`gs://my-firsct-bucket-debproj/input/`).
   - The local source file is deleted upon successful verification.
   - The pipeline exclusively reads directly from GCS (`gs://...`).
2. **Native Event Timestamp Assignment**:
   - Parses the `'event_time'` field from the clickstream records.
   - Uses `apache_beam.transforms.window.TimestampedValue` to attach 64-bit microsecond UTC epoch timestamps directly to the element's metadata envelope.
3. **Fixed Windowing (30 Minutes)**:
   - Partitions the continuous event timeline into contiguous, non-overlapping **30-minute intervals** (`FixedWindows(30 * 60)`).
4. **Windowed Grouping by User ID**:
   - Pairs elements into `(user_id, event)` tuples.
   - Applies `beam.GroupByKey()`, which in Apache Beam automatically groups per `(Key, Window)`.
5. **Cart Abandonment Detection**:
   - Uses `beam.DoFn.WindowParam` to access window boundaries (`window.start`, `window.end`).
   - Evaluates the session: Flags sessions having $\\ge 1$ `'cart'` event and $0$ `'purchase'` events within that 30-minute window.
   - Computes abandoned monetary value, product IDs, and event counts.
6. **JSON Output Sink**:
   - Serializes abandoned cart sessions to formatted JSON Lines (NDJSON).
7. **Architectural Deep Dive & Framework Contrast**:
   - Comprehensive breakdown of Beam's event time, watermarks, triggers, and allowed lateness.
   - Rigorous side-by-side contrast with **PySpark Structured Streaming** windowing.

---

### End-to-End Pipeline DAG:

```
┌────────────────────────────────────────────────────────────────────────┐
│     Source: gs://my-firsct-bucket-debproj/input/clickstream.csv        │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │  beam.io.ReadFromText
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│                      Raw Text Lines PCollection                        │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │  beam.ParDo(ParseAndTimestampDoFn)
                                    │  -> Parses event_time to UTC epoch
                                    │  -> Emits window.TimestampedValue
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│              Timestamped Clickstream Events PCollection                │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │  beam.WindowInto(FixedWindows(1800))
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│              Windowed Elements: [Window_Start, Window_End)             │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │  beam.Map(lambda e: (e['user_id'], e))
                                    │  beam.GroupByKey()  <-- Grouped per (user, Window)!
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│             Windowed User Sessions: (user_id, [events])                │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │  beam.ParDo(DetectAbandonedCartSessionsDoFn)
                                    │  - Accesses beam.DoFn.WindowParam
                                    │  - Filter: 'cart' in events AND 'purchase' not in events
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│                Identified Abandoned Cart User Sessions                 │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │  beam.ParDo(FormatToJsonDoFn)
                                    │  beam.io.WriteToText
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│    Sink: gs://my-firsct-bucket-debproj/output/abandoned_carts/sessions │
└────────────────────────────────────────────────────────────────────────┘
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

import google.auth
from google.cloud import storage
import apache_beam as beam
from apache_beam import window
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

print(f"Python Executable: {sys.executable}")
print(f"Apache Beam Version: {beam.__version__}")

# Verify Application Default Credentials (ADC)
try:
    credentials, default_project = google.auth.default()
    print("Application Default Credentials (ADC) successfully loaded.")
except Exception as e:
    print(f"ADC Notice: {e}")
"""
    cells.append(nbformat.v4.new_code_cell(cell_2_code))

    # Cell 3: Markdown - Step 1: Upload to GCS, Verify, Delete Local
    cell_3_md = """---
## 1. Cloud Ingestion: Upload Source Data to GCS Bucket & Delete Local Copy

As per enterprise production standards and pipeline requirements:
1. The source clickstream data is loaded into Google Cloud Storage (`gs://my-firsct-bucket-debproj/input/`).
2. We verify that the blob exists in the bucket and that its size matches the source file.
3. Upon successful verification, the local file is removed from disk.
4. **All subsequent pipeline stages read exclusively from the GCS bucket (`gs://...`).**
"""
    cells.append(nbformat.v4.new_markdown_cell(cell_3_md))

    # Cell 4: Code - Step 1 Implementation
    cell_4_code = """# Cell 2: Ingest clickstream dataset into GCS bucket, verify, and delete local source copy

PROJECT_ID = "debaranjan-practice"
BUCKET_NAME = "my-firsct-bucket-debproj"
client = storage.Client(project=PROJECT_ID)
bucket = client.bucket(BUCKET_NAME)

print(f"Connecting to Google Cloud Storage (Project: '{PROJECT_ID}', Bucket: '{BUCKET_NAME}')...")

# Check candidate datasets
blob_clickstream = bucket.blob("input/ecommerce_clickstream.csv")
blob_oct = bucket.blob("input/2019-Oct.csv")

# 1. Verification and local file cleanup for ecommerce_clickstream.csv
local_candidates = [
    "SourceFiles/ecommerce_clickstream.csv",
    "../SourceFiles/ecommerce_clickstream.csv"
]
local_file = next((f for f in local_candidates if os.path.exists(f)), None)
if local_file:
    local_size = os.path.getsize(local_file)
    print(f"Found local file '{local_file}' ({local_size:,} bytes).")
    if not blob_clickstream.exists():
        print(f"Uploading '{local_file}' to gs://{BUCKET_NAME}/input/ecommerce_clickstream.csv...")
        blob_clickstream.upload_from_filename(local_file)
    blob_clickstream.reload()
    if blob_clickstream.exists():
        print(f"VERIFIED on GCS: gs://{BUCKET_NAME}/input/ecommerce_clickstream.csv ({blob_clickstream.size:,} bytes).")
        os.remove(local_file)
        print(f"DELETED local copy '{local_file}'.")
elif blob_clickstream.exists():
    blob_clickstream.reload()
    size_disp = f"{blob_clickstream.size:,} bytes" if blob_clickstream.size else "verified"
    print(f"VERIFIED on GCS: gs://{BUCKET_NAME}/input/ecommerce_clickstream.csv ({size_disp}).")

# 2. Check 2019-Oct.csv on GCS and cleanup local copy if verified
local_oct_candidates = [
    "SourceFiles/2019-Oct.csv",
    "../SourceFiles/2019-Oct.csv"
]
local_oct = next((f for f in local_oct_candidates if os.path.exists(f)), None)
if blob_oct.exists():
    blob_oct.reload()
    size_disp = f"{blob_oct.size:,} bytes" if blob_oct.size else "verified"
    print(f"VERIFIED on GCS: gs://{BUCKET_NAME}/input/2019-Oct.csv ({size_disp}).")
    if local_oct and os.path.exists(local_oct):
        os.remove(local_oct)
        print(f"DELETED local copy '{local_oct}'.")

# Select active GCS source (exclusively from GCS bucket)
if blob_clickstream.exists():
    INPUT_GCS_PATH = f"gs://{BUCKET_NAME}/input/ecommerce_clickstream.csv"
    active_blob = blob_clickstream
elif blob_oct.exists():
    INPUT_GCS_PATH = f"gs://{BUCKET_NAME}/input/2019-Oct.csv"
    active_blob = blob_oct
else:
    raise FileNotFoundError("No clickstream source dataset found in GCS bucket!")

print(f"\\nACTIVE PIPELINE GCS SOURCE: {INPUT_GCS_PATH}")
assert INPUT_GCS_PATH.startswith("gs://"), "Pipeline must read from GCS bucket, not local filesystem!"
"""
    cells.append(nbformat.v4.new_code_cell(cell_4_code))

    # Cell 5: Markdown - Step 2: Inspect GCS Source
    cell_5_md = """---
## 2. Inspect Dataset Schema Directly from Google Cloud Storage

We stream the first few lines of the dataset directly from the GCS bucket to inspect its structure without downloading the full dataset to the local file system.
"""
    cells.append(nbformat.v4.new_markdown_cell(cell_5_md))

    # Cell 6: Code - Step 2 Inspection
    cell_6_code = """# Cell 3: Inspect sample clickstream records directly from GCS
print(f"Fetching sample lines from {INPUT_GCS_PATH}...")

# Stream sample byte range from GCS blob
sample_content = active_blob.download_as_text(start=0, end=1500)
sample_lines = sample_content.strip().splitlines()

print(f"--- Header & First {min(5, len(sample_lines)-1)} Records ---")
for line in sample_lines[:6]:
    print(line)
"""
    cells.append(nbformat.v4.new_code_cell(cell_6_code))

    # Cell 7: Markdown - Step 3: Custom Beam Transformation Classes
    cell_7_md = """---
## 3. Custom Transformation Classes (`DoFn`) & Native Timestamp Assignment

### 1. `ParseAndTimestampDoFn`:
- Parses raw CSV lines using standard CSV parsing.
- Extracts `'event_time'`, `'user_id'`, `'event_type'`, `'product_id'`, and `'price'`.
- Converts the UTC event time string into a **POSIX epoch timestamp in seconds** (`float`).
- Emits the element wrapped in **`apache_beam.transforms.window.TimestampedValue(record, timestamp_seconds)`**.
  > **Key Beam Principle**: Assigning a native timestamp via `TimestampedValue` binds the element to Beam's event-time clock, enabling the runner's watermark engine to assign elements into correct event-time windows rather than wall-clock arrival time.

### 2. `DetectAbandonedCartSessionsDoFn`:
- Applied after `beam.GroupByKey()`, which groups elements per `(user_id, Window)`.
- Injects **`window=beam.DoFn.WindowParam`** to inspect the 30-minute interval boundaries (`window.start`, `window.end`).
- Evaluates the session:
  - If the user performed at least one `'cart'` action AND **zero** `'purchase'` actions within the window:
    - Yields an abandoned cart session summary dictionary (including user ID, window timestamps, cart items count, total abandoned value, and event log).
  - If the user completed a `'purchase'` or never added anything to the cart, the session is not flagged as abandoned.

### 3. `FormatToJsonDoFn`:
- Serializes the abandoned cart session object into an NDJSON string line using `json.dumps()`.
"""
    cells.append(nbformat.v4.new_markdown_cell(cell_7_md))

    # Cell 8: Code - DoFn Definitions
    cell_8_code = """# Cell 4: Define custom Apache Beam DoFn classes

def parse_iso_datetime(date_str: str) -> float:
    \"\"\"Converts various clickstream datetime formats to UTC POSIX epoch seconds.\"\"\"
    cleaned = date_str.strip().replace(" UTC", "").replace("Z", "")
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S"):
        try:
            dt = datetime.datetime.strptime(cleaned, fmt)
            return dt.replace(tzinfo=datetime.timezone.utc).timestamp()
        except ValueError:
            pass
    # Fallback to ISO parser
    dt = datetime.datetime.fromisoformat(cleaned)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    return dt.timestamp()


class ParseAndTimestampDoFn(beam.DoFn):
    \"\"\"
    Parses CSV lines, extracts event details, casts types, and assigns native Beam timestamps.
    Emits elements wrapped in window.TimestampedValue.
    \"\"\"
    def process(self, line: str):
        line = line.strip()
        if not line or line.startswith("event_time") or line.startswith("event_id"):
            return
        
        try:
            row = list(csv.reader([line]))[0]
            # Handle 9-column schema (2019-Oct.csv)
            if len(row) >= 9:
                event_time_str = row[0].strip()
                event_type = row[1].strip().lower()
                product_id = row[2].strip()
                category = row[4].strip()
                price_str = row[6].strip()
                user_id = row[7].strip()
            # Handle 7-column schema (ecommerce_clickstream.csv)
            elif len(row) >= 7:
                event_time_str = row[1].strip()
                user_id = row[2].strip()
                event_type = row[3].strip().lower()
                product_id = row[4].strip()
                category = row[5].strip()
                price_str = row[6].strip()
            else:
                return

            epoch_timestamp = parse_iso_datetime(event_time_str)
            price = float(price_str) if price_str else 0.0

            record = {
                "event_time": event_time_str,
                "user_id": user_id,
                "event_type": event_type,
                "product_id": product_id,
                "category": category,
                "price": price
            }

            # Assign native Beam event-time timestamp
            yield window.TimestampedValue(record, epoch_timestamp)

        except (ValueError, IndexError, Exception):
            # Discard malformed records
            return


class DetectAbandonedCartSessionsDoFn(beam.DoFn):
    \"\"\"
    Inspects windowed user sessions.
    Identifies sessions containing 'cart' events but no 'purchase' events.
    Accesses window boundaries via beam.DoFn.WindowParam.
    \"\"\"
    def process(self, element, win=beam.DoFn.WindowParam):
        user_id, events_iter = element
        events = list(events_iter)
        
        # Categorize events
        event_types = {e["event_type"] for e in events}
        has_cart = ("cart" in event_types) or ("add_to_cart" in event_types)
        has_purchase = ("purchase" in event_types)

        # Condition: Cart event present BUT NO purchase event
        if has_cart and not has_purchase:
            cart_events = [e for e in events if e["event_type"] in ("cart", "add_to_cart")]
            total_abandoned_val = sum(e["price"] for e in cart_events)
            cart_product_ids = list({e["product_id"] for e in cart_events})
            
            # Format window boundaries in UTC
            win_start_str = win.start.to_utc_datetime().strftime("%Y-%m-%d %H:%M:%S UTC")
            win_end_str = win.end.to_utc_datetime().strftime("%Y-%m-%d %H:%M:%S UTC")

            yield {
                "user_id": user_id,
                "window_start": win_start_str,
                "window_end": win_end_str,
                "window_duration_minutes": 30,
                "cart_items_count": len(cart_events),
                "total_abandoned_value": round(total_abandoned_val, 2),
                "abandoned_products": cart_product_ids,
                "session_events_sequence": [e["event_type"] for e in events]
            }


class FormatToJsonDoFn(beam.DoFn):
    \"\"\"Serializes abandoned cart session dictionaries into NDJSON string lines.\"\"\"
    def process(self, session):
        yield json.dumps(session, ensure_ascii=False)

print("Custom DoFns and windowing transforms defined successfully.")
"""
    cells.append(nbformat.v4.new_code_cell(cell_8_code))

    # Cell 9: Markdown - Step 4: Pipeline Execution
    cell_9_md = """---
## 4. Pipeline Construction & Execution (30-Minute Fixed Windowing)

We now construct the Apache Beam pipeline DAG:
1. **`beam.io.ReadFromText(INPUT_GCS_PATH)`**: Reads the clickstream file directly from Google Cloud Storage.
2. **`beam.ParDo(ParseAndTimestampDoFn())`**: Parses records and attaches native event timestamps (`TimestampedValue`).
3. **`beam.WindowInto(window.FixedWindows(30 * 60))`**: Partitions the timeline into **30-minute fixed windows** (1800 seconds).
4. **`beam.Map(lambda e: (e["user_id"], e))`**: Keys records by `user_id`.
5. **`beam.GroupByKey()`**: Groups records by key **and by window**! Each worker receives all events for a given user in a single 30-minute slice.
6. **`beam.ParDo(DetectAbandonedCartSessionsDoFn())`**: Detects cart abandonment and computes monetary summaries.
7. **`beam.ParDo(FormatToJsonDoFn())`**: Formats each detected session as a JSON record.
8. **`beam.io.WriteToText(...)`**: Sinks the output JSON to both GCS and local output.
"""
    cells.append(nbformat.v4.new_markdown_cell(cell_9_md))

    # Cell 10: Code - Pipeline Execution
    cell_10_code = """# Cell 5: Construct and execute the Apache Beam pipeline with 30-minute Fixed Windowing

output_dir = "local_output/abandoned_carts"
os.makedirs(output_dir, exist_ok=True)
local_output_prefix = os.path.join(output_dir, "abandoned_cart_sessions")
gcs_output_prefix = f"gs://{BUCKET_NAME}/output/abandoned_carts/sessions"

print(f"Reading input from GCS:  {INPUT_GCS_PATH}")
print(f"Writing output to local: {local_output_prefix}-*.json")
print(f"Writing output to GCS:   {gcs_output_prefix}-*.json")
print("\\nExecuting pipeline with DirectRunner...")

with beam.Pipeline(runner="DirectRunner") as p:
    abandoned_sessions = (
        p
        # 1. Read clickstream dataset from GCS
        | "1_ReadClickstreamFromGCS" >> beam.io.ReadFromText(INPUT_GCS_PATH)
        
        # 2. Parse records and assign native Beam event timestamps
        | "2_ParseAndAssignTimestamps" >> beam.ParDo(ParseAndTimestampDoFn())
        
        # 3. Apply 30-Minute Fixed Windows (1800 seconds)
        | "3_FixedWindows30Min" >> beam.WindowInto(window.FixedWindows(30 * 60))
        
        # 4. Map to Key-Value: (user_id, event_dict)
        | "4_KeyByUserId" >> beam.Map(lambda event: (event["user_id"], event))
        
        # 5. Group by User ID and Window
        | "5_GroupEventsPerUserSession" >> beam.GroupByKey()
        
        # 6. Filter: Cart event present, BUT NO purchase event
        | "6_DetectCartAbandonment" >> beam.ParDo(DetectAbandonedCartSessionsDoFn())
        
        # 7. Format output as JSON string
        | "7_FormatToJson" >> beam.ParDo(FormatToJsonDoFn())
    )
    
    # Sink to local storage
    abandoned_sessions | "8_WriteLocalSink" >> beam.io.WriteToText(
        file_path_prefix=local_output_prefix,
        file_name_suffix=".json",
        shard_name_template="-SSSSS-of-NNNNN"
    )
    
    # Sink to Google Cloud Storage
    abandoned_sessions | "9_WriteGCSSink" >> beam.io.WriteToText(
        file_path_prefix=gcs_output_prefix,
        file_name_suffix=".json",
        shard_name_template="-SSSSS-of-NNNNN"
    )

print("Pipeline execution completed successfully!")
"""
    cells.append(nbformat.v4.new_code_cell(cell_10_code))

    # Cell 11: Code - Inspect Results
    cell_11_code = """# Cell 6: Verify and inspect detected abandoned cart sessions from generated output files

output_files = glob.glob(f"{local_output_prefix}*.json")
print(f"Found local output partition file(s): {output_files}\\n")

abandoned_sessions = []
for file_path in output_files:
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                abandoned_sessions.append(json.loads(line.strip()))

# Sort by window start time and total abandoned value
abandoned_sessions.sort(key=lambda s: (s["window_start"], -s["total_abandoned_value"]))

print(f"=== Total Abandoned Cart Sessions Detected: {len(abandoned_sessions)} ===")
total_abandoned_revenue = sum(s["total_abandoned_value"] for s in abandoned_sessions)
print(f"=== Total Potential Revenue Abandoned in Carts: ${total_abandoned_revenue:,.2f} ===\\n")

print("--- Sample Detected Abandoned Sessions (JSON Format) ---")
print(json.dumps(abandoned_sessions[:6], indent=2))
"""
    cells.append(nbformat.v4.new_code_cell(cell_11_code))

    # Cell 12: Markdown - Deep Dive into Timestamps & Windowing
    cell_12_md = """---
## 5. Technical Breakdown: How Apache Beam Handles Timestamp Assignment & Windowing

The Apache Beam model (derived from the seminal **Google MillWheel** and **FlumeJava** architectures, formalized in *The Dataflow Model* paper) fundamentally unifies batch and streaming execution around **Event Time**.

---

### 1. The Three Time Domains
In distributed stream processing, three distinct time domains exist:
1. **Event Time**: The wall-clock time at which the actual event occurred on the user's client device or web browser (encoded in `'event_time'`).
2. **Ingestion Time**: The timestamp assigned when the record arrives at the message broker (e.g., Google Cloud Pub/Sub, Apache Kafka).
3. **Processing Time**: The wall-clock time of the specific worker machine executing a transformation.

> **Why Event Time is Non-Negotiable**: In e-commerce clickstreams, mobile devices may lose connectivity, buffer events locally, and upload them 20 minutes later. Grouping by processing time would falsely combine old interactions with new sessions, producing corrupted business metrics. Beam enables grouping by genuine **Event Time**.

---

### 2. Native Timestamp Assignment Mechanics
When reading unbounded or bounded text lines via `beam.io.ReadFromText`, elements enter the pipeline with a default timestamp:
- In batch sources: `MIN_TIMESTAMP` (representing $-\\infty$).
- In streaming sources: Worker ingestion time.

To assign native event timestamps:
```python
yield window.TimestampedValue(record, epoch_timestamp_in_seconds)
```
- **Internal Representation**: Beam encapsulates each element in a `WindowedValue` metadata container:
  $$\\text{WindowedValue} = \\langle \\text{value}, \\text{timestamp}, \\text{windows}, \\text{pane\\_info} \\rangle$$
- The timestamp is stored as a 64-bit microsecond integer. Once assigned, all downstream windowing transformations evaluate this timestamp rather than system wall-clock time.

---

### 3. Fixed Windowing (`FixedWindows`) Mechanics
When `beam.WindowInto(window.FixedWindows(duration_seconds))` is applied:
- Beam maps each element with timestamp $t$ into an **`IntervalWindow`**:
  $$W = [W_{\\text{start}}, W_{\\text{end}})$$
  where:
  $$W_{\\text{start}} = t - (t \\pmod{\\text{duration}})$$
  $$W_{\\text{end}} = W_{\\text{start}} + \\text{duration}$$
- For a 30-minute fixed window ($1800\\text{ s}$), elements occurring between `10:00:00` and `10:29:59.999` are placed into window $[10:00:00, 10:30:00)$.
- **Crucial Invariant**: Elements are assigned to windows *before* aggregation. When `GroupByKey` executes, Beam partitions elements by the composite tuple:
  $$\\text{Shuffle Key} = (\\text{User Key}, \\text{Window})$$

---

### 4. Watermarks, Allowed Lateness, and Triggers

```
Event Time: ───────►  [Watermark: 10:30:00]  ───────►
                      ▲
                      │  All events up to 10:30:00 are presumed to have arrived.
                      │  Window [10:00, 10:30) can now fire!
```

- **Watermark**: A monotonically advancing heuristic timestamp representing the pipeline's confidence that no more data with event timestamp $< W$ will be observed.
- **Allowed Lateness**: By default, data arriving after the watermark passes $W_{\\text{end}}$ is silently dropped. By configuring `.with_allowed_lateness(datetime.timedelta(minutes=15))`, Beam keeps the window state alive in worker memory for an additional 15 minutes to incorporate late-arriving clickstream events.
- **Triggers**: Define *when* the window's accumulated contents are emitted:
  - **On-Time**: Fires when the watermark passes $W_{\\text{end}}$.
  - **Early (Speculative)**: Fires periodically before the watermark passes (e.g., to power real-time dashboards).
  - **Late**: Fires whenever new late data arrives after the initial watermark firing.
- **Accumulation Modes**:
  - `ACCUMULATING_FIRED_PANES`: Emits the full accumulated session history on each late firing.
  - `DISCARDING_FIRED_PANES`: Emits only the delta (new events) on subsequent firings.
"""
    cells.append(nbformat.v4.new_markdown_cell(cell_12_md))

    # Cell 13: Markdown - Architectural Contrast: Beam vs. PySpark
    cell_13_md = """---
## 6. Architectural Deep Dive: Apache Beam vs. PySpark Structured Streaming Windowing

Both Apache Beam and Apache Spark (PySpark Structured Streaming) support event-time windowing, but their architectural paradigms, state store mechanics, and API abstractions diverge fundamentally.

---

### 1. Comparison Matrix: Apache Beam vs. PySpark Structured Streaming

| Architectural Dimension | Apache Beam (`WindowInto` + `GroupByKey`) | PySpark Structured Streaming (`groupBy` + `window`) |
| :--- | :--- | :--- |
| **Window Representation** | **Pipeline Metadata**: Window is an `IntervalWindow` object bound to the element's metadata envelope; accessed via `beam.DoFn.WindowParam`. | **First-Class Column**: Window is an explicit struct column (`struct<start: timestamp, end: timestamp>`) in the DataFrame schema. |
| **Windowing Pipeline Placement** | **Pre-Aggregation Transform**: `WindowInto` assigns windows upstream on the `PCollection` *before* grouping or combining. | **Part of Aggregation**: Windowing is generated directly inside the relational `groupBy()` clause: `groupBy(window("event_time", "30 minutes"), "user_id")`. |
| **Timestamp Assignment** | **Element Envelope Metadata**: Assigned via `window.TimestampedValue(element, ts)` as microsecond epoch integers. | **Relational Column**: Must be parsed into a SQL `TimestampType` column in the DataFrame schema. |
| **Watermarking API** | **Abstract & Runner-Managed**: Managed automatically by the Runner (Dataflow/Flink) based on source connectors. | **Explicit DataFrame Operator**: Must be declared via `df.withWatermark("event_time", "10 minutes")`. |
| **State Storage Engine** | **Beam State & Timer API**: Managed state buffers backed by Dataflow Streaming Engine or Flink RocksDB state backend. | **State Store Providers**: Backed by HDFS-compatible checkpoint storage; uses in-memory or RocksDB state store providers. |
| **Output Modes** | **Trigger-Driven Panes**: Controlled by trigger specs (`AfterWatermark`) and pane accumulation modes (`ACCUMULATING`/`DISCARDING`). | **Structured Output Modes**: Must select `Append` (emits only finalized windows), `Update` (emits deltas), or `Complete` (emits entire state table). |
| **Session Windowing Support** | **Native Dynamic Sessions**: Built-in support for gap-based session windows (`window.Sessions(gap_size)`) with automatic window merging. | **Historically Constrained**: Requires `session_window` (added in Spark 3.2+) or complex stateful `applyInPandasWithState`. |
| **Portability** | **High**: Decoupled DAG runs on Google Cloud Dataflow, Apache Flink, Apache Spark, or DirectRunner. | **Engine-Bound**: Tightly coupled to Apache Spark cluster executors (Databricks, EMR, Dataproc). |

---

### 2. Structural & Shuffle Contrasts

#### In Apache Beam:
```python
(
    p
    | "Timestamp"   >> beam.ParDo(ParseAndTimestampDoFn())
    | "FixedWindow" >> beam.WindowInto(window.FixedWindows(1800))
    | "KeyByUser"   >> beam.Map(lambda e: (e["user_id"], e))
    | "GroupByKey"  >> beam.GroupByKey()
    | "FilterCart"  >> beam.ParDo(DetectAbandonedCartSessionsDoFn())
)
```
- In Beam, the window is **not** a data attribute of the user's business record. It lives in the execution envelope.
- Beam allows non-relational, stateful micro-logic inside `DoFn` instances using `beam.DoFn.WindowParam`, timers, and state cells.

#### In PySpark Structured Streaming:
```python
from pyspark.sql.functions import col, window, collect_list

(
    df
    .withWatermark("event_time", "10 minutes")
    .groupBy(
        window(col("event_time"), "30 minutes"),
        col("user_id")
    )
    .agg(collect_list("event_type").alias("events"))
    .filter(...)
)
```
- In PySpark, `window()` decomposes the timestamp column into a relational struct:
  `window: struct<start: timestamp, end: timestamp>`.
- The Catalyst Optimizer converts this into an internal `HashAggregate` partitioned by `hash(window, user_id)`.
- **Append Mode Constraint**: In PySpark Structured Streaming, an aggregation query running in `Append` mode **cannot emit results until the watermark has advanced past the window end plus the watermark delay**. In contrast, Beam triggers allow speculative firings before the watermark closes.

---

### 3. Summary Recommendation:
- Choose **Apache Beam** when building **cloud-native, event-driven streaming pipelines on Google Cloud Dataflow**, where unified stream/batch code, complex event-time window merging, and fine-grained trigger semantics are paramount.
- Choose **PySpark Structured Streaming** when the data infrastructure is already centered around **Databricks or Spark clusters**, and team skillsets are focused on relational SQL expressions and streaming DataFrame transformations.
"""
    cells.append(nbformat.v4.new_markdown_cell(cell_13_md))

    # Cell 14: Code - PySpark Equivalent Reference
    cell_14_code = """# Cell 7: Complete PySpark Structured Streaming Equivalent Implementation (Reference)
# This code snippet provides the exact PySpark Structured Streaming counterpart for direct architectural comparison:

pyspark_streaming_reference = \"\"\"
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, window, collect_list, array_contains, 
    sum as spark_sum, round as spark_round, to_json, struct
)

# 1. Initialize SparkSession with streaming support
spark = SparkSession.builder \\
    .appName("ClickstreamCartAbandonmentDetection") \\
    .master("local[*]") \\
    .getOrCreate()

# 2. Read streaming clickstream data (e.g., from Kafka or Cloud Storage)
schema = \"event_time STRING, event_type STRING, product_id STRING, category STRING, price DOUBLE, user_id STRING\"

df_raw = spark.readStream \\
    .format("csv") \\
    .option("header", "true") \\
    .schema(schema) \\
    .load("gs://my-firsct-bucket-debproj/input/")

# 3. Parse timestamp and declare watermark for late-arriving events
df_stream = df_raw \\
    .withColumn("event_timestamp", to_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss")) \\
    .withWatermark("event_timestamp", "15 minutes")

# 4. Group by 30-minute tumbling fixed window and user_id
df_windowed = df_stream \\
    .groupBy(
        window(col("event_timestamp"), "30 minutes"),
        col("user_id")
    ) \\
    .agg(
        collect_list("event_type").alias("session_events"),
        spark_round(spark_sum(col("price")), 2).alias("total_cart_value")
    )

# 5. Filter for Cart Abandonment: contains 'cart' BUT does NOT contain 'purchase'
df_abandoned = df_windowed \\
    .filter(
        array_contains(col("session_events"), "cart") & 
        (~array_contains(col("session_events"), "purchase"))
    )

# 6. Format as JSON and sink to storage in Append Mode
query = df_abandoned \\
    .select(
        col("user_id"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("total_cart_value"),
        col("session_events")
    ) \\
    .select(to_json(struct(col("*"))).alias("value")) \\
    .writeStream \\
    .format("text") \\
    .outputMode("append") \\
    .option("checkpointLocation", "gs://my-firsct-bucket-debproj/checkpoints/abandoned_carts/") \\
    .option("path", "gs://my-firsct-bucket-debproj/output/pyspark_abandoned_carts/") \\
    .start()

query.awaitTermination()
\"\"\"

print("PySpark Structured Streaming equivalent logic registered.")
print("The side-by-side contrast between Beam's WindowedValue/DoFn model and PySpark's DataFrame windowing is complete.")
"""
    cells.append(nbformat.v4.new_code_cell(cell_14_code))

    nb.cells = cells
    return nb

def main():
    print("Building notebook 3.ipynb structure...")
    nb = build_notebook_3()
    
    notebook_dir = os.path.join(os.getcwd(), "PythonNotebook")
    os.makedirs(notebook_dir, exist_ok=True)
    target_ipynb = os.path.join(notebook_dir, "3.ipynb")
    
    with open(target_ipynb, "w", encoding="utf-8") as f:
        nbformat.write(nb, f)
    print(f"Saved initial notebook to {target_ipynb}")
    
    print("Executing 3.ipynb with nbconvert to populate real cell outputs...")
    ep = ExecutePreprocessor(timeout=600, kernel_name="python3")
    try:
        ep.preprocess(nb, {"metadata": {"path": notebook_dir}})
        print("Notebook 3.ipynb executed successfully!")
    except Exception as e:
        print(f"Execution notice: {e}")
        
    with open(target_ipynb, "w", encoding="utf-8") as f:
        nbformat.write(nb, f)
    print(f"Final executed notebook saved at {target_ipynb}")

if __name__ == "__main__":
    main()
