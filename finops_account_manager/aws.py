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
    """Represents an AWS account in an Organization, including its parent unit and status."""
    account_id: str
    name: str = ""
    status: Optional[str] = None        # ACTIVE | SUSPENDED
    parent_id: Optional[str] = None
    parent_type: Optional[str] = None   # 'ORGANIZATIONAL_UNIT' or 'ROOT'
    parent_name: Optional[str] = None   # e.g. 'Security'


def _build_parent_map(client) -> Dict[str, Tuple[str, str, str]]:
    """
    Build a map: account_id -> (parent_id, parent_type, parent_name) by traversing root and OUs.
    """
    parent_map: Dict[str, Tuple[str, str, str]] = {}

    # List roots
    roots = client.list_roots().get('Roots', [])
    for root in roots:
        root_id = root['Id']
        root_name = root.get('Name', 'Root')
        # Accounts directly under root
        try:
            paginator = client.get_paginator('list_children')
            for page in paginator.paginate(ParentId=root_id, ChildType='ACCOUNT'):
                for child in page.get('Children', []):
                    acct_id = child['Id']
                    parent_map[acct_id] = (root_id, 'ROOT', root_name)
        except (BotoCoreError, ClientError) as e:
            logger.warning(f'Error listing accounts under root {root_id}: {e}')

        # Traverse organizational units
        queue = [root_id]
        while queue:
            parent = queue.pop(0)
            try:
                ous = client.list_organizational_units_for_parent(ParentId=parent).get('OrganizationalUnits', [])
            except (BotoCoreError, ClientError) as e:
                logger.warning(f'Error listing OUs for parent {parent}: {e}')
                ous = []

            for ou in ous:
                ou_id = ou['Id']
                ou_name = ou.get('Name')
                # Map accounts under this OU
                try:
                    pag = client.get_paginator('list_children')
                    for pg in pag.paginate(ParentId=ou_id, ChildType='ACCOUNT'):
                        for child in pg.get('Children', []):
                            acct_id = child['Id']
                            parent_map[acct_id] = (ou_id, 'ORGANIZATIONAL_UNIT', ou_name)
                except (BotoCoreError, ClientError) as e:
                    logger.warning(f'Error listing accounts under OU {ou_id}: {e}')
                # Enqueue nested OUs
                queue.append(ou_id)
    return parent_map


def get_aws_accounts(profile: Optional[str] = None, verbose: bool = False) -> List[AWSAccount]:
    """
    List all AWS accounts in the current Organization with status, parent_id, parent_type, and parent_name.

    :param profile: AWS CLI profile to use
    :param verbose: if True, logs page-by-page progress
    :return: List of AWSAccount instances
    """
    session_args = {}
    if profile:
        session_args['profile_name'] = profile

    try:
        if verbose:
            logger.info(f'Initializing AWS session (profile={profile})')
        session = boto3.Session(**session_args)
        client = session.client('organizations')
    except Exception as e:
        logger.error(f'Failed to initialize AWS client: {e}')
        sys.exit(1)

    # Build parent mapping
    start = time.time()
    parent_map = _build_parent_map(client)
    if verbose:
        logger.info(f'Built parent map for {len(parent_map)} accounts in {time.time()-start:.2f}s')

    accounts: List[AWSAccount] = []
    start_all = time.time()

    try:
        paginator = client.get_paginator('list_accounts')
        page_count = 0
        for page in paginator.paginate():
            page_count += 1
            for acct in page.get('Accounts', []):
                acct_id = acct.get('Id')
                acct_name = acct.get('Name', '')
                acct_status = acct.get('Status')
                pid, ptype, pname = parent_map.get(acct_id, (None, None, None))
                accounts.append(
                    AWSAccount(
                        account_id=acct_id,
                        name=acct_name,
                        status=acct_status,
                        parent_id=pid,
                        parent_type=ptype,
                        parent_name=pname
                    )
                )
            if verbose:
                logger.info(f'Page {page_count}: collected {len(accounts)} accounts')
    except Exception as e:
        logger.error(f'Error listing AWS accounts: {e}')
        sys.exit(1)

    total = time.time() - start_all
    logger.info(f'Retrieved {len(accounts)} AWS accounts in {total:.2f}s across {page_count} pages')
    return accounts


def save_accounts_to_csv(accounts: List[AWSAccount], directory: str = '.') -> str:
    """
    Save a list of AWSAccount objects to a timestamped CSV file.

    :param accounts: list of AWSAccount instances
    :param directory: directory to write the CSV into
    :return: path to the created CSV file
    """
    # Ensure output directory exists
    os.makedirs(directory, exist_ok=True)

    # Generate filename with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"aws_accounts_{timestamp}.csv"
    filepath = os.path.join(directory, filename)

    # Write CSV
    with open(filepath, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        # Header row
        writer.writerow([
            'account_id', 'name', 'status',
            'parent_id', 'parent_type', 'parent_name'
        ])
        # Data rows
        for acct in accounts:
            writer.writerow([
                acct.account_id,
                acct.name,
                acct.status or '',
                acct.parent_id or '',
                acct.parent_type or '',
                acct.parent_name or ''
            ])

    logger.info(f"Saved {len(accounts)} accounts to {filepath}")
    return filepath


if __name__ == '__main__':
    # Example usage
    import argparse

    parser = argparse.ArgumentParser(description='Fetch AWS accounts and save to CSV')
    parser.add_argument('--profile', help='AWS CLI profile name', default=None)
    parser.add_argument('--output-dir', help='Directory to write CSV', default='.')
    parser.add_argument('--verbose', action='store_true', help='Show progress logs')
    args = parser.parse_args()

    aws_accts = get_aws_accounts(profile=args.profile, verbose=args.verbose)
    save_accounts_to_csv(aws_accts, directory=args.output_dir)
