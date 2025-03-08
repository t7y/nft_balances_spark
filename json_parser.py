import csv
import json
import os

def main():
    # Path to your CSV file
    csv_file = "input/bwt_single.csv"
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

    os.makedirs("output", exist_ok=True)
    with open("output/parsed.json", "w", encoding="utf-8") as f_out:
        json.dump(parsed_results, f_out, indent=2)

if __name__ == '__main__':
    main()