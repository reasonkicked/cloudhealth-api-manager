from dataclasses import dataclass
from typing import List, Optional, Dict

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
    name: str = ""
    status: Optional[str] = None
    parent_id: Optional[str] = None
    parent_type: Optional[str] = None
    parent_name: Optional[str] = None
    grandparent_id: Optional[str] = None
    grandparent_name: Optional[str] = None
    ou_path: List[str] = None


def get_aws_accounts(profile: Optional[str] = None, verbose: bool = False) -> List[AWSAccount]:
    """
    Retrieves all AWS accounts with full OU hierarchy and correct parent names.
    """
    session_args = {'profile_name': profile} if profile else {}
    try:
        if verbose:
            logger.info(f"Initializing AWS session (profile={profile})")
        session = boto3.Session(**session_args)
        client = session.client('organizations')
    except Exception as e:
        logger.error(f"Failed to initialize AWS client: {e}")
        sys.exit(1)

    # Discover root and OUs
    roots = client.list_roots().get('Roots', [])
    if not roots:
        logger.error("No organization root found")
        sys.exit(1)
    root_id = roots[0]['Id']

    ou_name_map: Dict[str, str] = {root_id: 'Root'}
    ou_parent_map: Dict[str, str] = {root_id: None}
    ou_path_map: Dict[str, List[str]] = {root_id: ['Root']}

    # BFS to build OU maps
    queue = [root_id]
    while queue:
        parent = queue.pop(0)
        try:
            resp = client.list_organizational_units_for_parent(ParentId=parent)
            ous = resp.get('OrganizationalUnits', [])
        except (BotoCoreError, ClientError) as e:
            logger.warning(f"Error listing OUs under {parent}: {e}")
            continue
        for ou in ous:
            ou_id = ou['Id']
            ou_name = ou['Name']
            ou_name_map[ou_id] = ou_name
            ou_parent_map[ou_id] = parent
            ou_path_map[ou_id] = ou_path_map[parent] + [ou_name]
            queue.append(ou_id)
    if verbose:
        logger.info(f"Discovered {len(ou_name_map)} OUs (incl. root)")

    # Map accounts to parents via list_children
    account_parents: Dict[str, Tuple[str,str]] = {}
    paginator = client.get_paginator('list_children')
    # under root
    for page in paginator.paginate(ParentId=root_id, ChildType='ACCOUNT'):
        for ch in page.get('Children', []):
            account_parents[ch['Id']] = (root_id, 'ROOT')
    # under each OU
    for ou_id in list(ou_name_map.keys()):
        if ou_id == root_id:
            continue
        for page in paginator.paginate(ParentId=ou_id, ChildType='ACCOUNT'):
            for ch in page.get('Children', []):
                account_parents[ch['Id']] = (ou_id, 'ORGANIZATIONAL_UNIT')
    if verbose:
        logger.info(f"Mapped {len(account_parents)} accounts to parents via list_children")

    # Fetch account metadata and enrich
    accounts: List[AWSAccount] = []
    acct_paginator = client.get_paginator('list_accounts')
    for page in acct_paginator.paginate():
        for acct in page.get('Accounts', []):
            acct_id = acct['Id']
            name = acct.get('Name', '')
            status = acct.get('Status')
            # parent lookup
            pid, ptype = account_parents.get(acct_id, (None, None))
            if not pid:
                # fallback list_parents
                try:
                    resp = client.list_parents(ChildId=acct_id)
                    parents = resp.get('Parents', [])
                    if parents:
                        pid = parents[0]['Id']
                        ptype = parents[0]['Type']
                        account_parents[acct_id] = (pid, ptype)
                        if verbose:
                            logger.info(f"Fallback list_parents for {acct_id}: {pid}, {ptype}")
                except (BotoCoreError, ClientError) as e:
                    logger.warning(f"Fallback list_parents failed for {acct_id}: {e}")
            # parent name and path
            if ptype == 'ORGANIZATIONAL_UNIT' and pid:
                parent_name = ou_name_map.get(pid)
                if not parent_name:
                    try:
                        resp = client.describe_organizational_unit(OrganizationalUnitId=pid)
                        ou = resp.get('OrganizationalUnit', {})
                        parent_name = ou.get('Name')
                        ou_name_map[pid] = parent_name
                        ou_parent_map[pid] = ou.get('ParentId')
                        ou_path_map[pid] = ou_path_map.get(ou_parent_map[pid], ['Root']) + [parent_name]
                        if verbose:
                            logger.info(f"Described OU {pid}: {parent_name}")
                    except Exception as e:
                        logger.warning(f"Describe OU failed for {pid}: {e}")
                ou_path = ou_path_map.get(pid, ['Root'])
            else:
                parent_name = 'Root'
                ptype = 'ROOT'
                ou_path = ['Root']
            # grandparent
            if ptype == 'ORGANIZATIONAL_UNIT':
                gpid = ou_parent_map.get(pid)
                grandparent_name = ou_name_map.get(gpid) if gpid else None
            else:
                gpid = None
                grandparent_name = None
            accounts.append(AWSAccount(
                account_id=acct_id,
                name=name,
                status=status,
                parent_id=pid,
                parent_type=ptype,
                parent_name=parent_name,
                grandparent_id=gpid,
                grandparent_name=grandparent_name,
                ou_path=ou_path
            ))
    if verbose:
        logger.info(f"Collected {len(accounts)} AWS accounts total")
    return accounts


def save_accounts_to_csv(accounts: List[AWSAccount], directory: str = '.') -> str:
    os.makedirs(directory, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"aws_accounts_{timestamp}.csv"
    filepath = os.path.join(directory, filename)

    with open(filepath, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([
            'account_id','name','status',
            'parent_id','parent_name','parent_type',
            'grandparent_id','grandparent_name','ou_path'
        ])
        for a in accounts:
            writer.writerow([
                a.account_id,
                a.name,
                a.status or '',
                a.parent_id or '',
                a.parent_name or '',
                a.parent_type or '',
                a.grandparent_id or '',
                a.grandparent_name or '',
                "/".join(a.ou_path or [])
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
