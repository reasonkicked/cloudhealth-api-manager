import logging
import sys
import requests
import os
import csv
from datetime import datetime
from dataclasses import dataclass, asdict
from typing import List, Dict, Optional, Union

# Configure logger
logger = logging.getLogger(__name__)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(handler)
logger.setLevel(logging.INFO)


@dataclass
class CHAccount:
    """
    Represents an AWS account entry in CloudHealth.
    """
    ch_id: int
    aws_account_id: str
    name: str
    tags: Dict[str, str]


def get_cloudhealth_accounts(api_key: str, client_api_id: int) -> List[CHAccount]:
    """
    Fetch all AWS accounts from CloudHealth for a given client.

    :param api_key: CloudHealth API key
    :param client_api_id: CloudHealth client ID
    :return: list of CHAccount
    """
    url = 'https://chapi.cloudhealthtech.com/v1/aws_accounts'
    params = {
        'api_key': api_key,
        'client_api_id': client_api_id
    }
    try:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        logger.error(f"Error fetching CloudHealth accounts: {e}")
        sys.exit(1)

    # Parse JSON, handle possible dict-wrapper
    raw: Union[List, Dict] = resp.json()
    if isinstance(raw, dict):
        if 'aws_accounts' in raw and isinstance(raw['aws_accounts'], list):
            items = raw['aws_accounts']
        elif 'data' in raw and isinstance(raw['data'], list):
            items = raw['data']
        else:
            lists = [v for v in raw.values() if isinstance(v, list)]
            items = lists[0] if lists else []
    elif isinstance(raw, list):
        items = raw
    else:
        logger.error("Unexpected JSON structure from CloudHealth API")
        sys.exit(1)

    accounts: List[CHAccount] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        tags = {t['key']: t['value'] for t in item.get('tags', []) if isinstance(t, dict) and 'key' in t and 'value' in t}
        try:
            ch_id = int(item.get('id', 0))
        except (ValueError, TypeError):
            logger.warning(f"Invalid id field: {item.get('id')}")
            continue
        accounts.append(
            CHAccount(
                ch_id=ch_id,
                aws_account_id=str(item.get('aws_account_number', '')),
                name=str(item.get('name', '')),
                tags=tags
            )
        )
    logger.info(f"Retrieved {len(accounts)} CloudHealth accounts")
    return accounts


def save_cloudhealth_accounts_to_csv(accounts: List[CHAccount], directory: str = '.') -> str:
    """
    Save a list of CHAccount objects to a timestamped CSV file.

    :param accounts: list of CHAccount instances
    :param directory: directory to write the CSV into
    :return: path to the created CSV file
    """
    os.makedirs(directory, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"cloudhealth_accounts_{timestamp}.csv"
    filepath = os.path.join(directory, filename)

    with open(filepath, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        # Header
        writer.writerow(['ch_id', 'aws_account_id', 'name', 'tags'])
        for acct in accounts:
            writer.writerow([
                acct.ch_id,
                acct.aws_account_id,
                acct.name,
                ';'.join(f"{k}={v}" for k, v in acct.tags.items())
            ])
    logger.info(f"Saved {len(accounts)} CloudHealth accounts to {filepath}")
    return filepath


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Fetch CloudHealth accounts and save to CSV')
    parser.add_argument('--api-key', required=True, help='CloudHealth API key')
    parser.add_argument('--client-api-id', type=int, required=True, help='CloudHealth client ID')
    parser.add_argument('--output-dir', default='.', help='Directory to write CSV')
    args = parser.parse_args()

    ch_accounts = get_cloudhealth_accounts(args.api_key, args.client_api_id)
    save_cloudhealth_accounts_to_csv(ch_accounts, directory=args.output_dir)
