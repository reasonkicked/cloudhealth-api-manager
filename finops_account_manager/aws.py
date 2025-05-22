from dataclasses import dataclass
from typing import Optional, List, Dict

import boto3
import logging
import sys
import csv
import os
from datetime import datetime
from botocore.exceptions import BotoCoreError, ClientError

# Configure a simple logger for this module
logger = logging.getLogger(__name__)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(handler)
logger.setLevel(logging.INFO)

@dataclass
class AWSAccount:
    account_id: str
    name: str
    status: str
    parent_id: str
    parent_name: Optional[str] = ''
    parent_type: Optional[str] = ''
    grandparent_id: Optional[str] = ''
    grandparent_name: Optional[str] = ''
    ou_path: Optional[str] = ''

def get_aws_accounts(profile: Optional[str] = None, limit: Optional[int] = None, verbose: bool = False) -> List[AWSAccount]:
    session_args = {'profile_name': profile} if profile else {}
    session = boto3.Session(**session_args)
    client = session.client('organizations')

    accounts = []
    seen = 0
    paginator = client.get_paginator('list_accounts')

    for page in paginator.paginate():
        for acct in page.get('Accounts', []):
            acct_id = acct['Id']
            acct_name = acct.get('Name', '')
            acct_status = acct.get('Status', '')
            try:
                resp = client.list_parents(ChildId=acct_id)
                parent = resp['Parents'][0]
                parent_id = parent['Id']
                parent_type = parent['Type']
                if parent_type == 'ORGANIZATIONAL_UNIT':
                    desc = client.describe_organizational_unit(OrganizationalUnitId=parent_id)
                    parent_name = desc.get('OrganizationalUnit', {}).get('Name', '')
                else:
                    parent_name = 'Root'
            except Exception as e:
                logger.warning(f"Failed to get parent for {acct_id}: {e}")
                parent_id = ''
                parent_name = ''
                parent_type = ''

            accounts.append(AWSAccount(
                account_id=acct_id,
                name=acct_name,
                status=acct_status,
                parent_id=parent_id,
                parent_name=parent_name,
                parent_type=parent_type
            ))

            seen += 1
            if limit and seen >= limit:
                return accounts

    return accounts

def save_accounts_to_csv(accounts: List[AWSAccount], output_dir: str = '.', filename: Optional[str] = None) -> str:
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = filename or f'aws_accounts_{timestamp}.csv'
    filepath = os.path.join(output_dir, filename)

    with open(filepath, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['account_id', 'name', 'status', 'parent_id', 'parent_name', 'parent_type', 'grandparent_id', 'grandparent_name', 'ou_path'])
        for a in accounts:
            writer.writerow([
                a.account_id, a.name, a.status, a.parent_id,
                a.parent_name or '', a.parent_type or '',
                a.grandparent_id or '', a.grandparent_name or '', a.ou_path or ''
            ])
    logger.info(f"Saved {len(accounts)} accounts to {filepath}")
    return filepath

def enrich_grandparents_from_csv(csv_path: str, profile: str, output_path: Optional[str] = None) -> None:
    session = boto3.Session(profile_name=profile)
    client = session.client('organizations')
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    out_path = output_path or csv_path.replace('.csv', f'_enriched_{timestamp}.csv')

    with open(csv_path, 'r', newline='') as infile, open(out_path, 'w', newline='') as outfile:
        reader = csv.DictReader(infile)
        fieldnames = reader.fieldnames or []
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            account_id = row.get('account_id', '<unknown>')
            parent_id = row.get('parent_id', '')
            if parent_id.startswith('ou-') and not row.get('parent_name'):
                try:
                    ou_resp = client.describe_organizational_unit(OrganizationalUnitId=parent_id)
                    ou = ou_resp.get('OrganizationalUnit', {})
                    row['parent_name'] = ou.get('Name', '')
                    logger.info(f"Enriched parent_name for {account_id}: {row['parent_name']}")
                except Exception as e:
                    logger.warning(f"Failed to describe parent OU for {account_id}: {e}")

            if parent_id.startswith('ou-') and not (row.get('grandparent_id') and row.get('grandparent_name')):
                try:
                    resp = client.list_parents(ChildId=parent_id)
                    parents = resp.get('Parents', [])
                    if parents:
                        gp = parents[0]
                        gp_id = gp.get('Id')
                        row['grandparent_id'] = gp_id
                        if gp['Type'] == 'ORGANIZATIONAL_UNIT':
                            desc = client.describe_organizational_unit(OrganizationalUnitId=gp_id)
                            gp_name = desc.get('OrganizationalUnit', {}).get('Name', '')
                        else:
                            gp_name = 'Root'
                        row['grandparent_name'] = gp_name
                        logger.info(f"Enriched grandparent for {account_id}: {gp_id}, {gp_name}")
                except Exception as e:
                    logger.warning(f"Failed to enrich grandparent for {account_id}: {e}")

            writer.writerow(row)

    logger.info(f"Enriched CSV written to {out_path}")
    analyze_missing_fields(out_path)

def analyze_missing_fields(csv_path: str) -> None:
    with open(csv_path, 'r', newline='') as infile:
        reader = csv.DictReader(infile)
        total = 0
        missing_parents = 0
        missing_grandparents = 0

        for row in reader:
            total += 1
            if not row.get('parent_name'):
                missing_parents += 1
            if not row.get('grandparent_name'):
                missing_grandparents += 1

        logger.info(f"Analyzed {total} accounts")
        logger.info(f"Missing parent_name: {missing_parents}")
        logger.info(f"Missing grandparent_name: {missing_grandparents}")


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Fetch, enrich, and analyze AWS account OU hierarchy')
    parser.add_argument('--profile', required=True, help='AWS CLI profile name')
    parser.add_argument('--output-dir', default='.', help='Directory for output CSV')
    parser.add_argument('--limit', type=int, help='Limit number of AWS accounts to fetch')
    parser.add_argument('--verbose', action='store_true')
    parser.add_argument('--analyze-only', help='Analyze missing fields in existing CSV')
    parser.add_argument('--enrich', help='Enrich parent/grandparent info in existing CSV')

    args = parser.parse_args()

    if args.analyze_only:
        analyze_missing_fields(args.analyze_only)
    elif args.enrich:
        enrich_grandparents_from_csv(args.enrich, args.profile)
    else:
        accounts = get_aws_accounts(profile=args.profile, limit=args.limit, verbose=args.verbose)
        csv_path = save_accounts_to_csv(accounts, output_dir=args.output_dir)
        enrich_grandparents_from_csv(csv_path, args.profile)
