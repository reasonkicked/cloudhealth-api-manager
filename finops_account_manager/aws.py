from dataclasses import dataclass
from typing import List, Optional, Dict, Tuple

import boto3
import logging
import sys
import time
from botocore.exceptions import BotoCoreError, ClientError

# Configure a simple logger for this module
logger = logging.getLogger(__name__)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(handler)
logger.setLevel(logging.INFO)


@dataclass
class AWSAccount:
    """Represents an AWS account in an Organization, including its parent unit."""
    account_id: str
    name: str = ""
    parent_id: Optional[str] = None
    parent_type: Optional[str] = None  # 'ORGANIZATIONAL_UNIT' or 'ROOT'


def _build_parent_map(client) -> Dict[str, Tuple[str, str]]:
    """
    Build a map: account_id -> (parent_id, parent_type) by traversing root and OUs.
    """
    parent_map: Dict[str, Tuple[str, str]] = {}

    # 1) Handle root
    roots = client.list_roots().get('Roots', [])
    if not roots:
        logger.warning('No roots found in Organization')
        return parent_map
    for root in roots:
        root_id = root['Id']
        # Accounts directly under root
        try:
            paginator = client.get_paginator('list_children')
            for page in paginator.paginate(ParentId=root_id, ChildType='ACCOUNT'):
                for child in page.get('Children', []):
                    acct_id = child['Id']
                    parent_map[acct_id] = (root_id, 'ROOT')
        except Exception as e:
            logger.warning(f'Error listing accounts under root {root_id}: {e}')

        # Recursively traverse OUs under root
        queue = [root_id]
        while queue:
            parent = queue.pop(0)
            # List OUs under this parent
            try:
                ous_resp = client.list_organizational_units_for_parent(ParentId=parent)
                ous = ous_resp.get('OrganizationalUnits', [])
            except Exception as e:
                logger.warning(f'Error listing OUs for parent {parent}: {e}')
                ous = []
            for ou in ous:
                ou_id = ou['Id']
                # Get accounts under this OU
                try:
                    pag = client.get_paginator('list_children')
                    for pg in pag.paginate(ParentId=ou_id, ChildType='ACCOUNT'):
                        for child in pg.get('Children', []):
                            acct_id = child['Id']
                            parent_map[acct_id] = (ou_id, 'ORGANIZATIONAL_UNIT')
                except Exception as e:
                    logger.warning(f'Error listing accounts under OU {ou_id}: {e}')
                # Enqueue nested OUs
                queue.append(ou_id)
    return parent_map


def get_aws_accounts(profile: Optional[str] = None, verbose: bool = False) -> List[AWSAccount]:
    """
    List all AWS accounts in the current Organization with parent info via efficient traversal.

    :param profile: AWS CLI profile to use
    :param verbose: log progress if True
    :return: List of AWSAccount
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

    # Build parent mapping once
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
                parent_info = parent_map.get(acct_id, (None, None))
                accounts.append(AWSAccount(
                    account_id=acct_id,
                    name=acct_name,
                    parent_id=parent_info[0],
                    parent_type=parent_info[1]
                ))
            if verbose:
                logger.info(f'Page {page_count}: {len(accounts)} accounts collected so far')
    except Exception as e:
        logger.error(f'Error listing AWS accounts: {e}')
        sys.exit(1)

    total = time.time() - start_all
    logger.info(f'Retrieved {len(accounts)} AWS accounts in {total:.2f}s across {page_count} pages')
    return accounts

# Note:
# - The paginator 'list_accounts' returns for each account keys: Id, Arn, Email, Name, Status, JoinedMethod, JoinedTimestamp
# - You can access these via acct.get('<Key>').
# - Parent mapping uses list_roots, list_children (ACCOUNT), and list_organizational_units_for_parent to minimize per-account calls.
