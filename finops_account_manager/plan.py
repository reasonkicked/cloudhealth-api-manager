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
    """
    Load CloudHealth CSV by account display name.
    """
    ch_map = {}
    with open(path, newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = row.get('name', '').strip()
            ch_id = row.get('ch_id', '').strip()
            old_name = name
            if name:
                ch_map[name] = {'ch_id': ch_id, 'old_name': old_name}
    return ch_map

def generate_plan(aws_csv, ch_csv, out_path):
    aws_map = load_aws_csv(aws_csv)
    ch_map = load_ch_csv(ch_csv)
    plan = []
    matched = 0
    unmatched_ch = 0
    unmatched_aws = 0

    for aws_id, aws in aws_map.items():
        if aws_id in ch_map:
            ch = ch_map[aws_id]
            entry = {
                'ch_id': ch['ch_id'],
                'old_name': ch['old_name'],
                'new_name': aws['name'],
                'tags': {
                    'ou-level-1': aws.get('grandparent_name', ''),
                    'ou-level-2': aws.get('parent_name', '')
                }
            }
            plan.append(entry)
            matched += 1
        else:
            print(f"[INFO] AWS account {aws_id} ({aws.get('name')}) not found in CH CSV")
            unmatched_aws += 1

    for aws_id in ch_map:
        if aws_id not in aws_map:
            print(f"[WARN] CH account {aws_id} ({ch_map[aws_id]['old_name']}) not found in AWS CSV")
            unmatched_ch += 1

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

