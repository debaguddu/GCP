import json
import os
import nbformat
from nbconvert.preprocessors import ExecutePreprocessor

def build_notebook():
    nb = nbformat.v4.new_notebook()
    
    # Metadata
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

    # Cell 1: Markdown - Header & Architecture
    cell_1_md = """# Apache Beam Pipeline: Retail Sales Monthly Aggregation
## Advanced Data Engineering in Python

This notebook implements an end-to-end **Apache Beam** data processing pipeline in Python.

### Objective & Requirements:
1. **Data Ingestion**: Ingest retail sales transactions from `SourceFiles/retail_sales_dataset.csv`.
2. **Data Cleansing**: Filter out any rows with missing, null, or invalid `'Total Amount'` values.
3. **Feature Engineering**: Parse the `'Date'` column (`YYYY-MM-DD`) and extract the **month** and **year**.
4. **Grouping & Aggregation**: Group records by `'Product Category'` and the extracted month/year, and calculate the **sum of `'Total Amount'`**.
5. **Output Sink**: Format the aggregated results as **JSON** and write them to disk.
6. **Architectural Deep Dive**:
   - Comprehensive explanation of each specific **PTransform** utilized.
   - Rigorous architectural contrast between Apache Beam's grouping/aggregation model and **PySpark's `groupBy` and `agg`** methods.

---

### Pipeline Architecture:

```
┌────────────────────────────────────────────────────────┐
│      Source: SourceFiles/retail_sales_dataset.csv      │
└───────────────────────────┬────────────────────────────┘
                            │  beam.io.ReadFromText
                            ▼
┌────────────────────────────────────────────────────────┐
│             Raw Text Lines PCollection                 │
└───────────────────────────┬────────────────────────────┘
                            │  beam.ParDo(ParseCSVRowDoFn)
                            ▼
┌────────────────────────────────────────────────────────┐
│         Parsed Transaction Dictionaries                │
└───────────────────────────┬────────────────────────────┘
                            │  beam.Filter(is_valid_total_amount)
                            ▼
┌────────────────────────────────────────────────────────┐
│         Cleaned Transactions (Total Amount > 0)        │
└───────────────────────────┬────────────────────────────┘
                            │  beam.ParDo(ExtractCategoryMonthKVDoFn)
                            ▼
┌────────────────────────────────────────────────────────┐
│   Key-Value Pairs: ((Category, Month, Year), Amount)   │
└───────────────────────────┬────────────────────────────┘
                            │  beam.CombinePerKey(sum)  [Combiner Lifting]
                            ▼
┌────────────────────────────────────────────────────────┐
│  Aggregated Totals: ((Category, Month, Year), TotalSum)│
└───────────────────────────┬────────────────────────────┘
                            │  beam.ParDo(FormatToJsonDoFn)
                            ▼
┌────────────────────────────────────────────────────────┐
│            JSON Formatted String Records               │
└───────────────────────────┬────────────────────────────┘
                            │  beam.io.WriteToText
                            ▼
┌────────────────────────────────────────────────────────┐
│   Sink: local_output/retail_sales_monthly_summary.json │
└────────────────────────────────────────────────────────┘
```
"""
    cells.append(nbformat.v4.new_markdown_cell(cell_1_md))

    # Cell 2: Code - Imports & Environment
    cell_2_code = """# Cell 1: Environment check and imports
import os
import sys
import csv
import json
import datetime
import glob
from typing import Dict, Tuple, Any, Generator

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam.transforms.combiners as combiners

print(f"Python Executable: {sys.executable}")
print(f"Apache Beam Version: {beam.__version__}")
"""
    cells.append(nbformat.v4.new_code_cell(cell_2_code))

    # Cell 3: Markdown - Dataset Inspection
    cell_3_md = """---
## 1. Source Data Exploration & Schema Verification

We first inspect `SourceFiles/retail_sales_dataset.csv`. The schema contains:
- `Transaction ID` (int)
- `Date` (YYYY-MM-DD string)
- `Customer ID` (string)
- `Gender` (string)
- `Age` (int)
- `Product Category` (string: Beauty, Clothing, Electronics)
- `Quantity` (int)
- `Price per Unit` (int/float)
- `Total Amount` (int/float)
"""
    cells.append(nbformat.v4.new_markdown_cell(cell_3_md))

    # Cell 4: Code - Inspect Dataset
    cell_4_code = """# Cell 2: Locate and inspect the retail sales CSV dataset
candidate_paths = [
    "../SourceFiles/retail_sales_dataset.csv",
    "SourceFiles/retail_sales_dataset.csv",
    os.path.abspath(os.path.join(os.getcwd(), "..", "SourceFiles", "retail_sales_dataset.csv")),
    os.path.abspath(os.path.join(os.getcwd(), "SourceFiles", "retail_sales_dataset.csv"))
]
csv_file_path = next((p for p in candidate_paths if os.path.exists(p)), None)

if not csv_file_path:
    raise FileNotFoundError("Could not find 'retail_sales_dataset.csv' in SourceFiles.")

print(f"Dataset located at: {os.path.abspath(csv_file_path)}\\n")

print("--- Header and First 5 Records ---")
with open(csv_file_path, "r", encoding="utf-8") as f:
    for i in range(6):
        print(f.readline().strip())
"""
    cells.append(nbformat.v4.new_code_cell(cell_4_code))

    # Cell 5: Markdown - Transformation Definitions
    cell_5_md = """---
## 2. Define Custom Transformation Classes (`DoFn`) & Predicates

In Apache Beam:
1. **`ParseCSVRowDoFn` (`beam.DoFn`)**: Parses raw text lines using Python's standard `csv.reader` (handling embedded commas, quotes, and trimming whitespace) and skips the CSV header row.
2. **`is_valid_total_amount` (Predicate Function)**: Applied via `beam.Filter` to discard any record where `'Total Amount'` is `None`, empty string, whitespace, or cannot be parsed as a valid numeric value.
3. **`ExtractCategoryMonthKVDoFn` (`beam.DoFn`)**: Parses the `'Date'` column into a `datetime` object, extracts the **year** (`2023`, `2024`), **numeric month** (`'01'` to `'12'`), and **month name** (`'January'` to `'December'`). It yields a Key-Value pair where:
   - **Key**: `(product_category, year, month, month_name)`
   - **Value**: `total_amount` (as a float).
4. **`FormatToJsonDoFn` (`beam.DoFn`)**: Receives the grouped and aggregated sum `(Key, Sum)` and constructs a clean Python dictionary, serializing it into an NDJSON string line using `json.dumps()`.
"""
    cells.append(nbformat.v4.new_markdown_cell(cell_5_md))

    # Cell 6: Code - DoFn Definitions
    cell_6_code = """# Cell 3: Define Custom Transformation Functions and DoFns

class ParseCSVRowDoFn(beam.DoFn):
    \"\"\"
    Parses raw CSV lines into structured dictionaries using Python's csv module.
    Automatically handles commas within quoted fields and skips the header row.
    \"\"\"
    def process(self, line: str) -> Generator[Dict[str, str], None, None]:
        line = line.strip()
        # Skip empty lines and CSV header row
        if not line or line.startswith("Transaction ID"):
            return
        
        reader = csv.reader([line])
        for row in reader:
            if len(row) >= 9:
                yield {
                    "transaction_id": row[0].strip(),
                    "date": row[1].strip(),
                    "customer_id": row[2].strip(),
                    "gender": row[3].strip(),
                    "age": row[4].strip(),
                    "product_category": row[5].strip(),
                    "quantity": row[6].strip(),
                    "price_per_unit": row[7].strip(),
                    "total_amount": row[8].strip(),
                }


def is_valid_total_amount(record: Dict[str, str]) -> bool:
    \"\"\"
    Predicate function for beam.Filter.
    Filters out any row where 'Total Amount' is missing, empty, null, or non-numeric.
    \"\"\"
    val = record.get("total_amount")
    if val is None:
        return False
    val_str = str(val).strip()
    if not val_str:
        return False
    try:
        amount = float(val_str)
        return amount >= 0
    except ValueError:
        return False


class ExtractCategoryMonthKVDoFn(beam.DoFn):
    \"\"\"
    Parses 'Date' (YYYY-MM-DD), extracts year and month,
    extracts 'Product Category', casts 'Total Amount' to float,
    and emits a Key-Value pair: ((Product Category, Year, Month, Month Name), Total Amount).
    \"\"\"
    def process(self, record: Dict[str, str]) -> Generator[Tuple[Tuple[str, int, str, str], float], None, None]:
        try:
            # Parse Date column: 'YYYY-MM-DD'
            date_str = record["date"]
            dt = datetime.datetime.strptime(date_str, "%Y-%m-%d")
            year = dt.year
            month_num = dt.strftime("%m")       # e.g., '01', '11'
            month_name = dt.strftime("%B")     # e.g., 'January', 'November'
            
            category = record["product_category"]
            total_amount = float(record["total_amount"])
            
            # Key: (Product Category, Year, Month, Month Name)
            # Value: Total Amount (numeric float for sum aggregation)
            yield ((category, year, month_num, month_name), total_amount)
        except (ValueError, KeyError):
            # Skip malformed records
            return


class FormatToJsonDoFn(beam.DoFn):
    \"\"\"
    Converts aggregated Key-Value pairs into formatted JSON string records.
    Input: ((Product Category, Year, Month, Month Name), Sum(Total Amount))
    Output: JSON string line
    \"\"\"
    def process(self, element: Tuple[Tuple[str, int, str, str], float]) -> Generator[str, None, None]:
        (category, year, month_num, month_name), total_sum = element
        record = {
            "product_category": category,
            "year": year,
            "month": month_num,
            "month_name": month_name,
            "total_amount": round(total_sum, 2)
        }
        # Yield serialized JSON string
        yield json.dumps(record, ensure_ascii=False)

print("Custom DoFns and filter predicate defined successfully.")
"""
    cells.append(nbformat.v4.new_code_cell(cell_6_code))

    # Cell 7: Markdown - Execution Walkthrough
    cell_7_md = """---
## 3. Construct and Execute the Apache Beam Pipeline (DirectRunner)

We now construct the pipeline Directed Acyclic Graph (DAG) and execute it locally using `DirectRunner`.

### Chaining with the Pipe (`|`) and Label (`>>`) Operator:
- Each stage takes an input `PCollection` and produces an output `PCollection`.
- Labels (e.g. `'1_ReadCSV' >> ...`) uniquely identify each step in execution monitoring and telemetry graphs.
- Aggregation is achieved using **`beam.CombinePerKey(sum)`**, which automatically performs **Combiner Lifting** (map-side pre-aggregation) to minimize shuffle overhead.
"""
    cells.append(nbformat.v4.new_markdown_cell(cell_7_md))

    # Cell 8: Code - Execute Pipeline
    cell_8_code = """# Cell 4: Construct and Execute the Apache Beam Pipeline locally

# Set up local output directory
output_dir = "local_output"
os.makedirs(output_dir, exist_ok=True)
output_prefix = os.path.join(output_dir, "retail_sales_monthly_summary")

print("Executing Apache Beam pipeline with DirectRunner...")

with beam.Pipeline(runner="DirectRunner") as p:
    (
        p
        # Step 1: Read raw CSV lines from file
        | "1_ReadCSV" >> beam.io.ReadFromText(csv_file_path)
        
        # Step 2: Parse raw lines into transaction dictionaries
        | "2_ParseCSVRows" >> beam.ParDo(ParseCSVRowDoFn())
        
        # Step 3: Filter out any records with missing/invalid 'Total Amount'
        | "3_FilterMissingTotalAmount" >> beam.Filter(is_valid_total_amount)
        
        # Step 4: Extract month, year, product category and emit Key-Value pairs
        | "4_ExtractCategoryMonthKV" >> beam.ParDo(ExtractCategoryMonthKVDoFn())
        
        # Step 5: Group by Key and calculate sum of 'Total Amount'
        | "5_SumTotalAmountPerKey" >> beam.CombinePerKey(sum)
        
        # Step 6: Format the aggregated Key-Value pairs as JSON records
        | "6_FormatOutputAsJSON" >> beam.ParDo(FormatToJsonDoFn())
        
        # Step 7: Write JSON records to output file sink
        | "7_WriteJSONSink" >> beam.io.WriteToText(
            file_path_prefix=output_prefix,
            file_name_suffix=".json",
            shard_name_template="-SSSSS-of-NNNNN"
        )
    )

print("Pipeline execution completed successfully!")
"""
    cells.append(nbformat.v4.new_code_cell(cell_8_code))

    # Cell 9: Code - Verify & Inspect JSON Output
    cell_9_code = """# Cell 5: Inspect and Validate Generated JSON Output

generated_files = glob.glob(f"{output_prefix}*.json")
print(f"Generated output file(s): {generated_files}\\n")

all_records = []
for file_path in generated_files:
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                all_records.append(json.loads(line))

# Sort results for display by Product Category, Year, and Month
all_records.sort(key=lambda r: (r["product_category"], r["year"], r["month"]))

print(f"Total aggregated groups: {len(all_records)}\\n")
print("--- Sample JSON Output (First 10 Groups) ---")
print(json.dumps(all_records[:10], indent=2))

print(f"\\n--- Verification: Grand Total Across All Groups: ${sum(r['total_amount'] for r in all_records):,.2f} ---")
"""
    cells.append(nbformat.v4.new_code_cell(cell_9_code))

    # Cell 10: Markdown - Explanation of Specific PTransforms
    cell_10_md = """---
## 4. Deep Dive: Explanation of Specific PTransforms Used

In Apache Beam, a **`PTransform`** represents a data processing operation or step in the pipeline. It takes one or more `PCollection` objects as input and produces one or more `PCollection` objects as output.

| PTransform | Operation Type | Exact Role in Our Pipeline |
| :--- | :--- | :--- |
| **`beam.io.ReadFromText`** | Source Transform / I/O Read | Reads raw text files line-by-line from local storage or cloud buckets (GCS `gs://`, S3 `s3://`). Handles file splitting across worker threads. |
| **`beam.ParDo(DoFn)`** | Element-wise Parallel Processing | Core parallel processing primitive in Beam. Executes user-defined `DoFn.process()` on each element. Can yield zero, one, or multiple outputs. Used in `ParseCSVRowDoFn`, `ExtractCategoryMonthKVDoFn`, and `FormatToJsonDoFn`. |
| **`beam.Filter`** | Filtering Transform | Specialized `ParDo` that evaluates a boolean predicate for each element. Elements returning `True` pass downstream; elements returning `False` are dropped. Used to eliminate records missing `'Total Amount'`. |
| **`beam.CombinePerKey(sum)`** | Grouping & Aggregation Transform | Combines values associated with each key using an associative and commutative function (`sum`). Employs **Combiner Lifting** (map-side partial aggregation) prior to data shuffle. |
| **`beam.io.WriteToText`** | Sink Transform / I/O Write | Writes elements of a `PCollection` to sharded text files on disk or cloud storage with automatic partitioning (`-00000-of-00001.json`). |

---

### Detailed Mechanics of `beam.CombinePerKey` vs. `beam.GroupByKey`:
1. **`beam.GroupByKey()` (Raw Shuffle)**:
   - Takes `PCollection<K, V>` and outputs `PCollection<K, Iterable<V>>`.
   - **Problem**: Transmits **all** individual values across the network to reducers. If key `("Clothing", 2023, "11")` has 100,000 records, all 100,000 floats are serialized, buffered, and shuffled across the network.
   - High risk of worker Out-Of-Memory (OOM) on skewed keys.
2. **`beam.CombinePerKey(sum)` (Combiner Lifting)**:
   - Takes `PCollection<K, V>` and outputs `PCollection<K, OutputT>`.
   - Because `sum` is associative ($a + (b + c) = (a + b) + c$) and commutative ($a + b = b + a$), Beam's runner performs **map-side combining** in worker memory *before* shuffling.
   - Only the partial sums are transmitted over the network, reducing network I/O from $O(N)$ elements to $O(K)$ unique keys per worker partition!
"""
    cells.append(nbformat.v4.new_markdown_cell(cell_10_md))

    # Cell 11: Markdown - Contrast with PySpark
    cell_11_md = """---
## 5. Architectural Deep Dive: Apache Beam vs. PySpark (`groupBy` & `agg`)

Both Apache Beam and Apache Spark (PySpark) are premier distributed data processing frameworks, but their execution models, design philosophies, and aggregation mechanics differ fundamentally.

### 1. Comparison Matrix: Apache Beam vs. PySpark

| Feature / Dimension | Apache Beam (`CombinePerKey`) | PySpark (`groupBy` & `agg`) |
| :--- | :--- | :--- |
| **Primary Abstraction** | `PCollection` (parallel immutable elements, often Key-Value tuples `(K, V)`) | `DataFrame` / `Dataset` (distributed table with named columns, strict schema, and Catalyst relations) |
| **API Paradigm** | Functional / Dataflow DAG pipeline composition (`pipe` operator `\|`) | Declarative SQL / Relational DataFrame API (`df.groupBy().agg()`) |
| **Grouping Mechanism** | Key-Value structure: must explicitly map data into `(key, value)` tuples prior to grouping | Columnar expression: passes column references (`df.groupBy("Product Category", "Month")`) |
| **Aggregation Optimization** | **Combiner Lifting**: Associative & commutative `CombineFn` collapses data on worker threads prior to shuffle | **Catalyst Optimizer**: Injects `HashAggregate` (map-side partial aggregation) followed by `ShuffleExchange` and final `HashAggregate` |
| **Execution Engine & Memory** | Execution runner decoupled from pipeline; uses Python native objects, pickling, or Beam Schemas | Spark Engine with **Project Tungsten**; uses off-heap binary row representation and whole-stage code generation |
| **Streaming Integration** | **Unified Dataflow Model**: Batch and streaming share identical semantics (Windowing, Watermarks, Triggers, Accumulation) | **Structured Streaming**: Micro-batch processing model (or continuous processing) layered on top of the Spark engine |
| **Portability** | **High**: Write once, execute on Google Cloud Dataflow, Apache Flink, Apache Spark, or DirectRunner | **Engine-Bound**: Tightly coupled to Apache Spark runtime cluster (Databricks, EMR, Dataproc) |

---

### 2. Physical Execution & Shuffle Dynamics

#### In Apache Beam:
```
[Worker 1]  (Key A: 10, Key A: 20) ──► Local Combine (Key A: 30) ──┐
                                                                    ├──► [Network Shuffle] ──► Final Merge: (Key A: 75)
[Worker 2]  (Key A: 15, Key A: 30) ──► Local Combine (Key A: 45) ──┘
```
- In Beam, the developer explicitly shapes the data into a 2-tuple: `(K, V)`.
- When passing a combiner function (such as `sum` or a custom `beam.CombineFn`), the Beam runner invokes `add_input` locally across worker memory buffers.
- The network shuffle transmits pre-aggregated accumulator objects rather than individual transaction records.

#### In PySpark:
```python
df.groupBy("Product Category", "Month").agg(sum("Total Amount").alias("total_amount"))
```
- PySpark's **Catalyst Optimizer** analyzes the unresolved logical plan and compiles it into an optimized physical execution plan.
- The physical plan decomposes the aggregation into two phases:
  1. **Partial Aggregation (`HashAggregate(keys=[...], functions=[partial_sum(...)])`)**: Spark allocates an in-memory hash map (`BytesToBytesMap` managed by Tungsten off-heap) on each executor partition to compute running totals.
  2. **Shuffle Exchange (`Exchange hashpartitioning(...)`)**: Keys are hashed into reducer partitions.
  3. **Final Aggregation (`HashAggregate(keys=[...], functions=[sum(...)])`)**: Merges partial sums from all partitions.

---

### 3. Memory Representation & Serialization: Tungsten vs. Python Pickling
- **PySpark**: Avoids Python object creation during aggregation. Even though you write PySpark code, the aggregation execution runs inside JVM executors utilizing **Tungsten binary memory format**. Data remains off-heap in contiguous memory blocks, bypassing Java garbage collection and Python GIL bottlenecks.
- **Apache Beam (Python)**: Default Beam transforms pass native Python dictionaries and tuples between `DoFn` instances. To optimize this, Beam introduced **Beam Schemas** and `beam.Row`, which allow Beam runners (like Dataflow and Flink) to use optimized columnar coders and schema-aware combiners.

---

### 4. Code Comparison: Side-by-Side

#### Apache Beam (Key-Value Pipeline):
```python
# Apache Beam
(
    p
    | "ExtractKV" >> beam.Map(lambda row: ((row["Product Category"], row["Month"]), float(row["Total Amount"])))
    | "SumPerKey" >> beam.CombinePerKey(sum)
    | "ToJson"    >> beam.Map(lambda kv: json.dumps({"category": kv[0][0], "month": kv[0][1], "total": kv[1]}))
)
```

#### PySpark (Relational DataFrame API):
```python
# PySpark
from pyspark.sql.functions import col, month, year, sum as spark_sum, to_json, struct

(
    df
    .filter(col("Total Amount").isNotNull())
    .withColumn("Month", month(col("Date")))
    .groupBy("Product Category", "Month")
    .agg(spark_sum("Total Amount").alias("total_amount"))
    .select(to_json(struct(col("*"))).alias("json_record"))
)
```

**Key Takeaway**:
- Use **Apache Beam** when you require **cloud portability** (e.g. running on Google Cloud Dataflow), **unified real-time streaming with complex event-time windowing**, or modular reusable `DoFn` micro-transforms.
- Use **PySpark** when doing **large-scale relational data warehousing, interactive SQL analytics, or machine learning** where Catalyst query optimization and Tungsten off-heap memory offer out-of-the-box performance without manual combiner tuning.
"""
    cells.append(nbformat.v4.new_markdown_cell(cell_11_md))

    # Cell 12: Code - PySpark Equivalent Illustration
    cell_12_code = """# Cell 6: Complete PySpark Equivalent Implementation (Reference)
# This snippet demonstrates the equivalent PySpark code for direct side-by-side verification:

pyspark_equivalent_code = \"\"\"
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, month, year, sum as spark_sum, round as spark_round, to_json, struct

# Initialize SparkSession
spark = SparkSession.builder \\
    .appName("RetailSalesMonthlyAggregation") \\
    .master("local[*]") \\
    .getOrCreate()

# 1. Read CSV with inferred schema
df = spark.read \\
    .option("header", "true") \\
    .option("inferSchema", "true") \\
    .csv("SourceFiles/retail_sales_dataset.csv")

# 2. Filter out missing or invalid 'Total Amount'
df_clean = df.filter(col("Total Amount").isNotNull() & (col("Total Amount") >= 0))

# 3. Extract year and month from 'Date' column
df_parsed = df_clean.withColumn("ParsedDate", to_date(col("Date"), "yyyy-MM-dd")) \\
                    .withColumn("Year", year(col("ParsedDate"))) \\
                    .withColumn("Month", month(col("ParsedDate")))

# 4. Group by 'Product Category', Year, and Month, then calculate sum of 'Total Amount'
df_aggregated = df_parsed.groupBy("Product Category", "Year", "Month") \\
                         .agg(spark_round(spark_sum("Total Amount"), 2).alias("total_amount"))

# 5. Format output as JSON Lines
df_json = df_aggregated.select(to_json(struct(col("*"))).alias("value"))
df_json.write.mode("overwrite").text("local_output/pyspark_retail_sales_monthly_summary.json")

print("PySpark pipeline executed successfully.")
\"\"\"

print("PySpark equivalent pipeline logic registered.")
print("The side-by-side contrast between Beam's KV combiner lifting and PySpark's Catalyst HashAggregate is complete.")
"""
    cells.append(nbformat.v4.new_code_cell(cell_12_code))

    nb.cells = cells
    return nb

def main():
    print("Building notebook structure...")
    nb = build_notebook()
    
    notebook_dir = os.path.join(os.getcwd(), "PythonNotebook")
    os.makedirs(notebook_dir, exist_ok=True)
    
    target_ipynb = os.path.join(notebook_dir, "2.ipynb")
    
    # Save unexecuted first
    with open(target_ipynb, "w", encoding="utf-8") as f:
        nbformat.write(nb, f)
        
    print(f"Saved initial notebook to {target_ipynb}")
    
    # Execute notebook to populate outputs
    print("Executing notebook to populate cell execution outputs...")
    ep = ExecutePreprocessor(timeout=600, kernel_name="python3")
    try:
        ep.preprocess(nb, {"metadata": {"path": notebook_dir}})
        print("Notebook executed successfully!")
    except Exception as e:
        print(f"Execution notice: {e}")
        # Still continue to save
        
    with open(target_ipynb, "w", encoding="utf-8") as f:
        nbformat.write(nb, f)
    print(f"Updated executed notebook at {target_ipynb}")

if __name__ == "__main__":
    main()
