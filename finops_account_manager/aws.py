from dataclasses import dataclass, asdict
from typing import List, Optional, Dict, Tuple

import boto3
import logging
import sys
import time
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
    """Represents an AWS account in an Organization, including its OU hierarchy and status."""
    account_id: str
    name: str = ""
    status: Optional[str] = None        # ACTIVE | SUSPENDED
    parent_id: Optional[str] = None
    parent_type: Optional[str] = None   # 'ORGANIZATIONAL_UNIT' or 'ROOT'
    ou_path: List[str] = None           # Full path of OU names from root to immediate parent


def _build_ou_maps(client) -> Tuple[Dict[str,str], Dict[str,str]]:
    ou_name_map: Dict[str,str] = {}
    ou_parent_map: Dict[str,str] = {}

    roots = client.list_roots().get('Roots', [])
    queue = [r['Id'] for r in roots]
    while queue:
        parent = queue.pop(0)
        try:
            resp = client.list_organizational_units_for_parent(ParentId=parent)
            ous = resp.get('OrganizationalUnits', [])
        except (BotoCoreError, ClientError) as e:
            logger.warning(f"Error listing OUs for parent {parent}: {e}")
            continue
        for ou in ous:
            ou_id = ou['Id']
            ou_name = ou.get('Name')
            ou_name_map[ou_id] = ou_name
            ou_parent_map[ou_id] = parent
            queue.append(ou_id)
    return ou_name_map, ou_parent_map


def _build_account_parents(client, ou_name_map) -> Dict[str, Tuple[str,str]]:
    account_parents: Dict[str,Tuple[str,str]] = {}
    roots = client.list_roots().get('Roots', [])
    for root in roots:
        root_id = root['Id']
        paginator = client.get_paginator('list_children')
        try:
            for page in paginator.paginate(ParentId=root_id, ChildType='ACCOUNT'):
                for ch in page.get('Children', []):
                    account_parents[ch['Id']] = (root_id, 'ROOT')
        except (BotoCoreError, ClientError) as e:
            logger.warning(f"Error listing accounts under root {root_id}: {e}")
    for ou_id in ou_name_map:
        paginator = client.get_paginator('list_children')
        try:
            for page in paginator.paginate(ParentId=ou_id, ChildType='ACCOUNT'):
                for ch in page.get('Children', []):
                    account_parents[ch['Id']] = (ou_id, 'ORGANIZATIONAL_UNIT')
        except (BotoCoreError, ClientError) as e:
            logger.warning(f"Error listing accounts under OU {ou_id}: {e}")
    return account_parents


def _construct_ou_path(client, ou_id: Optional[str], ou_name_map: Dict[str,str], ou_parent_map: Dict[str,str]) -> List[str]:
    path: List[str] = []
    current = ou_id
    while current:
        if current.startswith("r-"):  # root detected, add and break
            path.insert(0, "Root")
            break
        if current not in ou_name_map:
            try:
                resp = client.describe_organizational_unit(OrganizationalUnitId=current)
                ou = resp.get('OrganizationalUnit', {})
                ou_name_map[current] = ou.get('Name', current)
                ou_parent_map[current] = ou.get('ParentId', '')
            except Exception as e:
                logger.warning(f"Failed to describe OU {current}: {e}")
                break
        path.insert(0, ou_name_map.get(current, current))
        current = ou_parent_map.get(current)
    return path


def get_aws_accounts(profile: Optional[str] = None, verbose: bool = False) -> List[AWSAccount]:
    session_args = {}
    if profile:
        session_args['profile_name'] = profile
    try:
        if verbose:
            logger.info(f"Initializing AWS session (profile={profile})")
        session = boto3.Session(**session_args)
        client = session.client('organizations')
    except Exception as e:
        logger.error(f"Failed to initialize AWS client: {e}")
        sys.exit(1)

    ou_name_map, ou_parent_map = _build_ou_maps(client)
    account_parents = _build_account_parents(client, ou_name_map)
    if verbose:
        logger.info(f"Built hierarchy maps: {len(ou_name_map)} OUs, {len(account_parents)} parent links")

    accounts: List[AWSAccount] = []
    paginator = client.get_paginator('list_accounts')
    page_count = 0
    for page in paginator.paginate():
        page_count += 1
        for acct in page.get('Accounts', []):
            acct_id = acct.get('Id')
            name = acct.get('Name', '')
            status = acct.get('Status')
            parent_info = account_parents.get(acct_id)
            if not parent_info:
                try:
                    resp = client.list_parents(ChildId=acct_id)
                    parents = resp.get('Parents', [])
                    if parents:
                        p0 = parents[0]
                        parent_info = (p0.get('Id'), p0.get('Type'))
                        account_parents[acct_id] = parent_info
                        if verbose:
                            logger.info(f"Fallback parent for {acct_id}: {parent_info}")
                except (BotoCoreError, ClientError):
                    parent_info = (None, None)
            pid, ptype = parent_info if parent_info else (None, None)
            ou_path: List[str] = []
            if ptype == 'ORGANIZATIONAL_UNIT':
                ou_path = _construct_ou_path(client, pid, ou_name_map, ou_parent_map)
            accounts.append(AWSAccount(
                account_id=acct_id,
                name=name,
                status=status,
                parent_id=pid,
                parent_type=ptype,
                ou_path=ou_path
            ))
        if verbose:
            logger.info(f"Page {page_count}: {len(accounts)} accounts so far")
    if verbose:
        logger.info(f"Finished collecting {len(accounts)} accounts")
    return accounts


def save_accounts_to_csv(accounts: List[AWSAccount], directory: str = '.') -> str:
    os.makedirs(directory, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"aws_accounts_{timestamp}.csv"
    filepath = os.path.join(directory, filename)

    with open(filepath, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['account_id','name','status','parent_id','parent_type','ou_path'])
        for a in accounts:
            path = a.ou_path or (['Root'] if a.parent_type=='ROOT' else [])
            writer.writerow([
                a.account_id,
                a.name,
                a.status or '',
                a.parent_id or '',
                a.parent_type or '',
                "/".join(path)
            ])
    logger.info(f"Saved {len(accounts)} accounts to {filepath}")
    return filepath


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Export AWS accounts with OU hierarchy to CSV')
    parser.add_argument('--profile', help='AWS CLI profile', default=None)
    parser.add_argument('--output-dir', help='Directory for CSV', default='.')
    parser.add_argument('--verbose', action='store_true')
    args = parser.parse_args()
    accts = get_aws_accounts(profile=args.profile, verbose=args.verbose)
    save_accounts_to_csv(accts, directory=args.output_dir)
