from dataclasses import dataclass
from typing import List, Optional

import boto3
import logging
import sys
from botocore.exceptions import BotoCoreError, ClientError

# Configure a simple logger for this module
logger = logging.getLogger(__name__)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(handler)
logger.setLevel(logging.INFO)


@dataclass
class AWSAccount:
    """Represents an AWS account in an Organization."""
    account_id: str
    name: str = ""


def get_aws_accounts(profile: Optional[str] = None) -> List[AWSAccount]:
    """
    List all AWS accounts in the current Organization.

    :param profile: (optional) AWS CLI profile name to load credentials from
    :return: list of AWSAccount(account_id, name)
    :raises SystemExit: on any boto3/client error
    """
    session_args = {}
    if profile:
        session_args["profile_name"] = profile

    # Initialize session and client, catching any errors
    try:
        session = boto3.Session(**session_args)
        client = session.client("organizations")
    except Exception as e:
        logger.error(f"Failed to initialize AWS session/client: {e}")
        sys.exit(1)

    accounts: List[AWSAccount] = []
    try:
        paginator = client.get_paginator("list_accounts")
        for page in paginator.paginate():
            for acct in page.get("Accounts", []):
                acct_id = acct.get("Id")
                acct_name = acct.get("Name", "")
                accounts.append(AWSAccount(account_id=acct_id, name=acct_name))
    except Exception as e:
        logger.error(f"Error listing AWS accounts: {e}")
        sys.exit(1)

    logger.info(f"Retrieved {len(accounts)} AWS accounts")
    return accounts
