import logging
import sys
import requests
import os
import csv
from datetime import datetime
from dataclasses import dataclass
from typing import List, Dict, Union

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


def get_cloudhealth_accounts(api_key: str,
                             client_api_id: int,
                             per_page: int = 100) -> List[CHAccount]:
    """
    Fetch all AWS accounts from CloudHealth for a given client, handling pagination.

    Uses API key and client_api_id as query params (legacy auth).

    :param api_key: CloudHealth API key
    :param client_api_id: CloudHealth client ID
    :param per_page: Number of results per page (max 100)
    :return: list of CHAccount
    """
    base_url = 'https://chapi.cloudhealthtech.com/v1/aws_accounts'
    page = 1
    all_items: List[Dict] = []

    # Pagination loop
    while True:
        params = {
            'api_key': api_key,
            'client_api_id': client_api_id,
            'page': page,
            'per_page': per_page
        }
        try:
            resp = requests.get(base_url, params=params, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.error(f"Error fetching CloudHealth accounts page {page}: {e}")
            sys.exit(1)

        data = resp.json()
        # unwrap
        if isinstance(data, dict):
            items = data.get('aws_accounts') or data.get('data') or []
        elif isinstance(data, list):
            items = data
        else:
            logger.error(f"Unexpected JSON structure on page {page}")
            break

        count = len(items)
        logger.info(f"Fetched {count} accounts from page {page}")
        if count == 0:
            break
        all_items.extend(items)
        if count < per_page:
            break
        page += 1

    logger.info(f"Total CloudHealth accounts retrieved: {len(all_items)}")

    # Parse into CHAccount
    accounts: List[CHAccount] = []
    for item in all_items:
        if not isinstance(item, dict):
            continue
        try:
            ch_id = int(item.get('id', 0))
        except (ValueError, TypeError):
            logger.warning(f"Invalid id: {item.get('id')}")
            continue
        # Prefer 'aws_account_number', fallback to 'owner_id'
        aws_acc = str(item.get('aws_account_number') or item.get('owner_id', '')).strip()
        name = str(item.get('name') or '').strip()
        tags = {}
        for t in item.get('tags', []):
            if isinstance(t, dict) and 'key' in t and 'value' in t:
                tags[t['key']] = t['value']
        accounts.append(CHAccount(ch_id=ch_id, aws_account_id=aws_acc, name=name, tags=tags))

    return accounts


def save_cloudhealth_accounts_to_csv(accounts: List[CHAccount], directory: str = '.') -> str:
    os.makedirs(directory, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"cloudhealth_accounts_{timestamp}.csv"
    filepath = os.path.join(directory, filename)

    with open(filepath, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
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
    parser.add_argument('--client-api-id', type=int, required=True, help='CloudHealth client API ID')
    parser.add_argument('--per-page', type=int, default=100, help='Results per page (max 100)')
    parser.add_argument('--output-dir', default='.', help='Directory to write CSV')
    args = parser.parse_args()

    ch_accounts = get_cloudhealth_accounts(
        args.api_key, args.client_api_id, per_page=args.per_page
    )
    save_cloudhealth_accounts_to_csv(ch_accounts, directory=args.output_dir)
