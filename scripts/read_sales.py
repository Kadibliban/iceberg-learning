import csv
from pathlib import Path

# Find the project root
project_root = Path(__file__).resolve().parent.parent

# Path to the raw sales data
sales_file = project_root / "data" / "sales.csv"

# Read the CSV file
with open(sales_file, "r", encoding="utf-8") as file:
    reader = csv.DictReader(file)

    rows = list(reader)

# Display basic information
print(f"Total sales records: {len(rows)}")

print("\nFirst 3 records:")
for row in rows[:3]:
    print(row)