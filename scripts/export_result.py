import os
import re
import csv
import clickhouse_connect

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
QUERY_FILE = os.getenv("QUERY_FILE", "/sql/02_query_max_category_per_state.sql")
OUTPUT_CSV = os.getenv("OUTPUT_CSV", "/output/result.csv")

def _load_single_query(path: str) -> str:
    with open(path, "r", encoding="utf-8-sig") as f:
        text = f.read().strip()
    text = "\n".join([ln.rstrip() for ln in text.splitlines()]).strip()

    parts = [p.strip() for p in text.split(";") if p.strip()]
    if not parts:
        raise ValueError(f"Empty SQL in {path}")
    query = parts[-1].strip()

    query = re.sub(r";\s*$", "", query)
    return query

def main():
    if not os.path.exists(QUERY_FILE):
        raise FileNotFoundError(f"Query file not found: {QUERY_FILE}")

    query = _load_single_query(QUERY_FILE)

    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
    )

    result = client.query(query)
    cols = result.column_names
    rows = result.result_rows

    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
    with open(OUTPUT_CSV, "w", encoding="utf-8", newline="") as out:
        w = csv.writer(out)
        w.writerow(cols)
        w.writerows(rows)

if __name__ == "__main__":
    main()
