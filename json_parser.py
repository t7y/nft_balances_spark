import csv
import json
import os
import sys

# Increase CSV field size limit
csv.field_size_limit(sys.maxsize)

def main():
    # Path to your CSV file
    csv_file = "input/base_23635928.csv"
    parsed_results = []

    with open(csv_file, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Extract the JSON string from the 'json' column.
            # The CSV has escaped double quotes (i.e. doubled) that we need to convert.
            json_str = row["json"].replace('""', '"')
            # Now parse the JSON string into a Python object.
            parsed_json = json.loads(json_str)
            parsed_results.append(parsed_json)

    os.makedirs("input", exist_ok=True)
    with open("input/base_23635928.json", "w", encoding="utf-8") as f_out:
        json.dump(parsed_results, f_out, indent=2)

if __name__ == '__main__':
    main()