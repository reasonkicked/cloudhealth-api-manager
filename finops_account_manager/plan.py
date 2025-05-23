# plan.py
"""
Generates a Terraform-style plan for updating CloudHealth AWS accounts based on AWS Organizations data.
"""
import csv
import argparse
import json

def load_aws_csv(path):
    aws_map = {}
    with open(path, newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            aws_map[row['account_id'].strip()] = row
    return aws_map

def load_ch_csv(path):
    ch_map = {}
    with open(path, newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            aws_id = row.get('aws_account_id', '').strip()
            if aws_id:
                ch_map[aws_id] = row
    return ch_map

def generate_plan(aws_csv, ch_csv, out_path):
    aws_map = load_aws_csv(aws_csv)
    ch_map = load_ch_csv(ch_csv)
    plan = []
    matched = 0
    unmatched_ch = 0
    unmatched_aws = 0

    # Match CH -> AWS
    for aws_id, ch in ch_map.items():
        ch_id = ch.get('ch_id')
        if aws_id in aws_map:
            aws = aws_map[aws_id]
            entry = {
                'ch_id': ch_id,
                'new_name': aws['name'],
                'tags': {
                    'ou-level-1': aws.get('grandparent_name', ''),
                    'ou-level-2': aws.get('parent_name', '')
                }
            }
            plan.append(entry)
            matched += 1
        else:
            print(f"[WARN] CH entry {ch_id} aws_account_id {aws_id} not found in AWS CSV")
            unmatched_ch += 1

    # Optionally, warn for AWS accounts not found in CH
    for aws_id in aws_map:
        if aws_id not in ch_map:
            print(f"[INFO] AWS account {aws_id} ({aws_map[aws_id]['name']}) not found in CH CSV")
            unmatched_aws += 1

    with open(out_path, 'w') as f:
        json.dump(plan, f, indent=2)
    print(f"Plan written to {out_path}, {matched} matched, {unmatched_ch} unmatched CH, {unmatched_aws} unmatched AWS")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Generate update plan for CloudHealth')
    parser.add_argument('--aws-csv', required=True, help='AWS accounts CSV')
    parser.add_argument('--ch-csv', required=True, help='CloudHealth accounts CSV')
    parser.add_argument('--out', default='plan.json', help='Output plan file')
    args = parser.parse_args()
    generate_plan(args.aws_csv, args.ch_csv, args.out)
