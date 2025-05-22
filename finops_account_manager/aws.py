from dataclasses import dataclass
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
    """
    Build maps of OU names and parents.
    Returns:
      - ou_name_map: OU_ID -> OU Name
      - ou_parent_map: OU_ID -> Parent OU or Root ID
    """
    ou_name_map: Dict[str,str] = {}
    ou_parent_map: Dict[str,str] = {}

    roots = client.list_roots().get('Roots', [])
    queue = [r['Id'] for r in roots]
    while queue:
        parent = queue.pop(0)
        try:
            response = client.list_organizational_units_for_parent(ParentId=parent)
            ous = response.get('OrganizationalUnits', [])
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
    """
    Build a map of account_id -> (parent_id, parent_type).
    """
    account_parents: Dict[str,Tuple[str,str]] = {}
    roots = client.list_roots().get('Roots', [])
    # Accounts under root
    for root in roots:
        root_id = root['Id']
        paginator = client.get_paginator('list_children')
        try:
            for page in paginator.paginate(ParentId=root_id, ChildType='ACCOUNT'):
                for ch in page.get('Children', []):
                    account_parents[ch['Id']] = (root_id, 'ROOT')
        except (BotoCoreError, ClientError) as e:
            logger.warning(f"Error listing children for root {root_id}: {e}")
    # Accounts under each OU
    for ou_id in ou_name_map:
        paginator = client.get_paginator('list_children')
        try:
            for page in paginator.paginate(ParentId=ou_id, ChildType='ACCOUNT'):
                for ch in page.get('Children', []):
                    account_parents[ch['Id']] = (ou_id, 'ORGANIZATIONAL_UNIT')
        except (BotoCoreError, ClientError) as e:
            logger.warning(f"Error listing children for OU {ou_id}: {e}")
    return account_parents


def _construct_ou_path(ou_id: Optional[str], ou_name_map: Dict[str,str], ou_parent_map: Dict[str,str]) -> List[str]:
    """
    Walk up from OU_ID to root, collecting OU names from top-down.
    """
    path: List[str] = []
    current = ou_id
    while current and current in ou_name_map:
        path.insert(0, ou_name_map[current])
        current = ou_parent_map.get(current)
    return path


def get_aws_accounts(profile: Optional[str] = None, verbose: bool = False) -> List[AWSAccount]:
    """
    List all AWS accounts with full OU path and status.
    """
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

    # Build OU and account-parent maps
    start = time.time()
    ou_name_map, ou_parent_map = _build_ou_maps(client)
    account_parents = _build_account_parents(client, ou_name_map)
    if verbose:
        logger.info(f"Built hierarchy maps in {time.time()-start:.2f}s")

    accounts: List[AWSAccount] = []
    start_all = time.time()
    page_count = 0
    paginator = client.get_paginator('list_accounts')
    for page in paginator.paginate():
        page_count += 1
        for acct in page.get('Accounts', []):
            acct_id = acct.get('Id')
            name = acct.get('Name', '')
            status = acct.get('Status')
            pid, ptype = account_parents.get(acct_id, (None, None))
            ou_path = []
            if ptype == 'ORGANIZATIONAL_UNIT':
                ou_path = _construct_ou_path(pid, ou_name_map, ou_parent_map)
            accounts.append(AWSAccount(
                account_id=acct_id,
                name=name,
                status=status,
                parent_id=pid,
                parent_type=ptype,
                ou_path=ou_path
            ))
        if verbose:
            logger.info(f"Page {page_count}: collected {len(accounts)} accounts")
    total = time.time() - start_all
    logger.info(f"Retrieved {len(accounts)} AWS accounts in {total:.2f}s across {page_count} pages")
    return accounts


def save_accounts_to_csv(accounts: List[AWSAccount], directory: str = '.') -> str:
    """
    Save accounts list to a timestamped CSV.
    """
    os.makedirs(directory, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"aws_accounts_{timestamp}.csv"
    path = os.path.join(directory, filename)
    with open(path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['account_id','name','status','parent_id','parent_type','ou_path'])
        for a in accounts:
            writer.writerow([a.account_id,a.name,a.status or '',a.parent_id or '',a.parent_type or '',"/".join(a.ou_path or [])])
    logger.info(f"Saved {len(accounts)} accounts to {path}")
    return path


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Export AWS accounts with OU hierarchy to CSV')
    parser.add_argument('--profile', help='AWS CLI profile', default=None)
    parser.add_argument('--output-dir', help='Directory for CSV', default='.')
    parser.add_argument('--verbose', action='store_true')
    args = parser.parse_args()
    accts = get_aws_accounts(profile=args.profile, verbose=args.verbose)
    save_accounts_to_csv(accts, directory=args.output_dir)