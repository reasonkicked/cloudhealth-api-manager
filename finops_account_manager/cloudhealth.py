import logging
import sys
import requests
from dataclasses import dataclass
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
        # unwrap common envelope keys
        if 'aws_accounts' in raw and isinstance(raw['aws_accounts'], list):
            items = raw['aws_accounts']
        elif 'data' in raw and isinstance(raw['data'], list):
            items = raw['data']
        else:
            # fallback: first list found
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
        tags = {}
        for t in item.get('tags', []):
            # skip malformed entries
            if isinstance(t, dict) and 'key' in t and 'value' in t:
                tags[t['key']] = t['value']
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


def update_cloudhealth_account(ch_id: int,
                               new_name: Optional[str] = None,
                               new_tags: Optional[Dict[str, str]] = None,
                               api_key: str = None,
                               client_api_id: int = None,
                               dry_run: bool = False) -> None:
    """
    Update a single AWS account record in CloudHealth.

    :param ch_id: CloudHealth record ID
    :param new_name: desired name (if provided)
    :param new_tags: desired tags dict (if provided)
    :param api_key: CloudHealth API key
    :param client_api_id: CloudHealth client ID
    :param dry_run: if True, only log without sending
    """
    url = f"https://chapi.cloudhealthtech.com/v1/aws_accounts/{ch_id}"
    params = {
        'api_key': api_key,
        'client_api_id': client_api_id
    }
    payload: Dict = {}
    if new_name is not None:
        payload['name'] = new_name
    if new_tags is not None:
        payload['tags'] = [{'key': k, 'value': v} for k, v in new_tags.items()]

    if dry_run:
        logger.info(f"DRY RUN: PUT {url} params={params} payload={payload}")
        return

    headers = {'Content-Type': 'application/json'}
    try:
        resp = requests.put(url, params=params, json=payload, headers=headers, timeout=30)
        resp.raise_for_status()
        logger.info(f"Updated CloudHealth account {ch_id}")
    except requests.RequestException as e:
        logger.error(f"Failed to update CloudHealth account {ch_id}: {e}")
        sys.exit(1)
