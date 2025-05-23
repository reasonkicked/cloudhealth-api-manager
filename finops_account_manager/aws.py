from dataclasses import dataclass
from typing import List, Optional

import boto3
import logging
import sys
import csv
import os
from datetime import datetime
from botocore.exceptions import BotoCoreError, ClientError

# Configure logger
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
    parent_id: Optional[str]
    parent_name: Optional[str]
    parent_type: Optional[str]
    grandparent_id: Optional[str]
    grandparent_name: Optional[str]


def get_aws_accounts(profile: Optional[str] = None, verbose: bool = False, limit: Optional[int] = None) -> List[AWSAccount]:
    """
    Fetch accounts and resolve parent and grandparent OU names inline, skipping grandparent for root.
    """
    session = boto3.Session(profile_name=profile) if profile else boto3.Session()
    client = session.client('organizations')

    accounts: List[AWSAccount] = []
    count = 0
    paginator = client.get_paginator('list_accounts')

    for page in paginator.paginate():
        for acct in page.get('Accounts', []):
            acct_id = acct['Id']
            acct_name = acct.get('Name', '')
            acct_status = acct.get('Status', '')

            # get direct parent
            try:
                resp = client.list_parents(ChildId=acct_id)
                parents = resp.get('Parents', [])
                if parents:
                    parent = parents[0]
                    parent_id = parent['Id']
                    parent_type = parent['Type']
                else:
                    parent_id = None
                    parent_type = None
            except (BotoCoreError, ClientError) as e:
                logger.warning(f"list_parents failed for {acct_id}: {e}")
                parent_id = None
                parent_type = None

            # resolve parent name
            if parent_type == 'ORGANIZATIONAL_UNIT' and parent_id:
                try:
                    desc = client.describe_organizational_unit(OrganizationalUnitId=parent_id)
                    parent_name = desc['OrganizationalUnit'].get('Name')
                except (ClientError, BotoCoreError) as e:
                    logger.warning(f"describe OU failed for parent {parent_id}: {e}")
                    parent_name = None
            else:
                parent_name = 'Root' if parent_type == 'ROOT' else None

            # get grandparent (skip if parent is root or missing)
            gp_id = None
            gp_name = None
            gp_type = None
            if parent_type == 'ORGANIZATIONAL_UNIT' and parent_id:
                try:
                    resp2 = client.list_parents(ChildId=parent_id)
                    gps = resp2.get('Parents', [])
                    if gps:
                        gp = gps[0]
                        gp_id = gp.get('Id')
                        gp_type = gp.get('Type')
                except (BotoCoreError, ClientError) as e:
                    logger.warning(f"list_parents failed for parent {parent_id}: {e}")
            # resolve grandparent name
            if gp_type == 'ORGANIZATIONAL_UNIT' and gp_id:
                try:
                    desc2 = client.describe_organizational_unit(OrganizationalUnitId=gp_id)
                    gp_name = desc2['OrganizationalUnit'].get('Name')
                except (ClientError, BotoCoreError) as e:
                    logger.warning(f"describe OU failed for grandparent {gp_id}: {e}")
            elif gp_type == 'ROOT':
                gp_name = 'Root'

            accounts.append(AWSAccount(
                account_id=acct_id,
                name=acct_name,
                status=acct_status,
                parent_id=parent_id,
                parent_name=parent_name,
                parent_type=parent_type,
                grandparent_id=gp_id,
                grandparent_name=gp_name
            ))

            count += 1
            if limit and count >= limit:
                if verbose:
                    logger.info(f"Limit {limit} reached, stopping processing")
                return accounts
            if verbose and count % 100 == 0:
                logger.info(f"Processed {count} accounts so far")

    if verbose:
        logger.info(f"Fetched total {len(accounts)} accounts")
    return accounts


def save_accounts_to_csv(accounts: List[AWSAccount], output_dir: str = '.') -> str:
    os.makedirs(output_dir, exist_ok=True)
    fname = f"aws_accounts_{datetime.now():%Y%m%d_%H%M%S}.csv"
    path = os.path.join(output_dir, fname)
    with open(path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['account_id','name','status','parent_id','parent_name','parent_type','grandparent_id','grandparent_name'])
        for a in accounts:
            writer.writerow([
                a.account_id, a.name, a.status,
                a.parent_id or '', a.parent_name or '', a.parent_type or '',
                a.grandparent_id or '', a.grandparent_name or ''
            ])
    logger.info(f"Saved {len(accounts)} accounts to {path}")
    return path

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Export AWS accounts with parent and grandparent OUs')
    parser.add_argument('--profile', help='AWS CLI profile', default=None)
    parser.add_argument('--verbose', action='store_true')
    parser.add_argument('--limit', type=int, help='Limit number of accounts to process')
    parser.add_argument('--output-dir', help='CSV output directory', default='.')
    args = parser.parse_args()

    acct_list = get_aws_accounts(profile=args.profile, verbose=args.verbose, limit=args.limit)
    save_accounts_to_csv(acct_list, output_dir=args.output_dir)
