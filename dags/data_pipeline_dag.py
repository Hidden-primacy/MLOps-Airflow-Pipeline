import csv
from pathlib import Path
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

P = Path("/opt/airflow/dags/pipeline_output")

def extract():
    P.mkdir(exist_ok=True)
    (P / "raw.csv").write_text(
        "Name,Marks\nAmit,80\nPriya,91\nRahul,\nSneha,88\nKaran,-5\n"
    )
    print("Extracted 5 student records")

def validate():
    rows = list(csv.DictReader((P / "raw.csv").open()))
    missing = sum(not r["Marks"] for r in rows)
    invalid = sum(bool(r["Marks"]) and not 0 <= int(r["Marks"]) <= 100 for r in rows)
    print("Total records:", len(rows))
    print("Missing values:", missing)
    print("Invalid records:", invalid)
    (P / "validation.txt").write_text(
        f"Total: {len(rows)}\nMissing: {missing}\nInvalid: {invalid}"
    )

def process():
    rows = list(csv.DictReader((P / "raw.csv").open()))
    clean = [r for r in rows if r["Marks"] and 0 <= int(r["Marks"]) <= 100]
    with (P / "processed.csv").open("w") as f:
        writer = csv.DictWriter(f, fieldnames=["Name", "Marks"])
        writer.writeheader()
        writer.writerows(clean)
    print("Processed records:", len(clean))

def report():
    rows = list(csv.DictReader((P / "processed.csv").open()))
    result = (P / "validation.txt").read_text()
    result += f"\nProcessed: {len(rows)}\nStatus: SUCCESS"
    (P / "report.txt").write_text(result)
    print(result)

def notify():
    print("SUCCESS: Data pipeline completed successfully!")

with DAG(
    dag_id="data_pipeline_dag",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1
) as dag:
    t1 = PythonOperator(task_id="extract", python_callable=extract)
    t2 = PythonOperator(task_id="validate", python_callable=validate)
    t3 = PythonOperator(task_id="process", python_callable=process)
    t4 = PythonOperator(task_id="report", python_callable=report)
    t5 = PythonOperator(task_id="notify", python_callable=notify)
    t1 >> t2 >> t3 >> t4 >> t5
