import argparse
import csv
import logging
import os
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam.transforms.combiners as combiners


class ParseAndCleanEmployeeDoFn(beam.DoFn):
    """
    Parses CSV lines, skips header row, handles basic cleansing, and casts types.
    Fields: Employee_ID,Name,Age,Gender,Department,Job_Title,Experience_Years,Education_Level,Location,Salary
    """
    def process(self, line: str):
        line = line.strip()
        if not line or line.startswith("Employee_ID"):
            return
        
        reader = csv.reader([line])
        for row in reader:
            if len(row) >= 10:
                try:
                    yield {
                        "employee_id": int(row[0].strip()),
                        "name": row[1].strip(),
                        "age": int(row[2].strip()),
                        "gender": row[3].strip(),
                        "department": row[4].strip(),
                        "job_title": row[5].strip(),
                        "experience_years": int(row[6].strip()),
                        "education_level": row[7].strip(),
                        "location": row[8].strip(),
                        "salary": float(row[9].strip()),
                    }
                except (ValueError, IndexError):
                    continue


class ExtractDepartmentSalaryDoFn(beam.DoFn):
    """Extracts (department, salary) tuples for key-based aggregation."""
    def process(self, record: dict):
        yield (record["department"], record["salary"])


class FormatSummaryStatsDoFn(beam.DoFn):
    """Formats (department, average_salary) tuple into a CSV line."""
    def process(self, element):
        department, avg_salary = element
        yield f"{department},{avg_salary:.2f}"


def run_pipeline(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input",
        dest="input",
        default="SourceFiles/Employers_data.csv",
        help="Input file path (local path or gs://bucket/path)"
    )
    parser.add_argument(
        "--output",
        dest="output",
        default="local_output/dept_salary_summary",
        help="Output file prefix (local path or gs://bucket/path)"
    )
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args, save_main_session=True)

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "ReadInput" >> beam.io.ReadFromText(known_args.input)
            | "ParseAndClean" >> beam.ParDo(ParseAndCleanEmployeeDoFn())
            | "ExtractDeptSalary" >> beam.ParDo(ExtractDepartmentSalaryDoFn())
            | "ComputeAvgSalary" >> combiners.Mean.PerKey()
            | "FormatResults" >> beam.ParDo(FormatSummaryStatsDoFn())
            | "WriteOutput" >> beam.io.WriteToText(
                file_path_prefix=known_args.output,
                file_name_suffix=".csv",
                header="Department,Average_Salary"
            )
        )
    print(f"Pipeline executed successfully. Output written to {known_args.output}*")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_pipeline()
