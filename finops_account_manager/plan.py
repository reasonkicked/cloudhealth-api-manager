# plan.py
"""
Generates a Terraform-style plan for updating CloudHealth AWS accounts based on AWS Organizations data.
Auto-detects CSV types by header to avoid swapped inputs.
Only includes accounts where CloudHealth name is currently the AWS ID.
"""
import csv
import argparse
import json
import sys


def load_aws_csv(path):
    """
    Load AWS CSV indexed by AWS account ID ('account_id' column).
    """
    aws_map = {}
    with open(path, newline='') as f:
        reader = csv.DictReader(f)
        if 'account_id' not in reader.fieldnames:
            raise ValueError(f"File {path} is not an AWS CSV (missing 'account_id')")
        for row in reader:
            aws_id = row.get('account_id', '').strip()
            if aws_id:
                aws_map[aws_id] = row
    return aws_map


def load_ch_csv(path):
    """
    Load CloudHealth CSV indexed by AWS account ID ('aws_account_id' column).
    """
    ch_map = {}
    with open(path, newline='') as f:
        reader = csv.DictReader(f)
        if 'aws_account_id' not in reader.fieldnames:
            raise ValueError(f"File {path} is not a CloudHealth CSV (missing 'aws_account_id')")
        for row in reader:
            aws_id = row.get('aws_account_id', '').strip()
            if not aws_id:
                continue
            ch_map[aws_id] = {
                'ch_id': row.get('ch_id', '').strip(),
                'old_name': row.get('name', '').strip()
            }
    return ch_map


def detect_and_load(aws_path, ch_path):
    """
    Detects if inputs are swapped and loads accordingly.
    Returns (aws_map, ch_map).
    """
    def peek_header(path):
        with open(path, newline='') as f:
            reader = csv.reader(f)
            return next(reader)

    h1 = peek_header(aws_path)
    h2 = peek_header(ch_path)
    is_aws1 = 'account_id' in h1
    is_ch1 = 'aws_account_id' in h1
    is_aws2 = 'account_id' in h2
    is_ch2 = 'aws_account_id' in h2

    if is_aws1 and is_ch2:
        aws_map = load_aws_csv(aws_path)
        ch_map = load_ch_csv(ch_path)
    elif is_aws2 and is_ch1:
        aws_map = load_aws_csv(ch_path)
        ch_map = load_ch_csv(aws_path)
    else:
        print(f"Error: Unable to detect CSV types. Headers: {aws_path}={h1}, {ch_path}={h2}")
        sys.exit(1)
    return aws_map, ch_map


def generate_plan(aws_csv, ch_csv, out_path):
    try:
        aws_map, ch_map = detect_and_load(aws_csv, ch_csv)
    except Exception as e:
        print(f"[ERROR] {e}")
        sys.exit(1)

    plan = []
    matched = 0
    unmatched_ch = 0
    unmatched_aws = 0

    # Match AWS entries to CH by AWS account ID
    for aws_id, aws in aws_map.items():
        if aws_id in ch_map:
            ch = ch_map[aws_id]
            # Only include if CH old_name is placeholder (same as aws_id)
            if ch['old_name'] == aws_id:
                entry = {
                    'aws_id': aws_id,
                    'ch_id': ch['ch_id'],
                    'old_name': ch['old_name'],
                    'new_name': aws.get('name', '').strip(),
                    'tags': {
                        'ou-level1': aws.get('grandparent_name', '').strip(),
                        'ou-level2': aws.get('parent_name', '').strip()
                    }
                }
                plan.append(entry)
                matched += 1
            # else: human-readable name already present, skip
        else:
            print(f"[INFO] AWS account {aws_id} ({aws.get('name')}) not found in CH CSV")
            unmatched_aws += 1

    # Detect CH entries not present in AWS
    for aws_id, ch in ch_map.items():
        if aws_id not in aws_map:
            print(f"[WARN] CH account {aws_id} ({ch['old_name']}) not found in AWS CSV")
            unmatched_ch += 1

    # Write plan
    with open(out_path, 'w') as f:
        json.dump(plan, f, indent=2)

    print(f"Plan written to {out_path}, {matched} matched, {unmatched_ch} unmatched CH, {unmatched_aws} unmatched AWS")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Generate update plan for CloudHealth')
    parser.add_argument('--aws-csv', required=True, help='AWS accounts CSV file path')
    parser.add_argument('--ch-csv', required=True, help='CloudHealth accounts CSV file path')
    parser.add_argument('--out', default='plan.json', help='Output plan JSON path')
    args = parser.parse_args()
    generate_plan(args.aws_csv, args.ch_csv, args.out)
