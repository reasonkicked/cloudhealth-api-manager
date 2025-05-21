import logging
import pytest
import responses
from finops_account_manager.cloudhealth import get_cloudhealth_accounts, update_cloudhealth_account, CHAccount

# Use logging capture for dry_run output

def _api_url():
    return 'https://chapi.cloudhealthtech.com/v1/aws_accounts'

@responses.activate
def test_get_cloudhealth_accounts_success(caplog):
    sample = [
        {
            'id': '1',
            'aws_account_number': '111111111111',
            'name': 'First',
            'tags': [{'key': 'env', 'value': 'prod'}]
        },
        {
            'id': '2',
            'aws_account_number': '222222222222',
            'name': 'Second',
            'tags': []
        }
    ]
    responses.add(
        responses.GET,
        _api_url(),
        json=sample,
        status=200
    )
    caplog.set_level(logging.INFO)
    accounts = get_cloudhealth_accounts('fake_key', 12345)
    assert len(accounts) == 2
    assert accounts[0] == CHAccount(
        ch_id=1,
        aws_account_id='111111111111',
        name='First',
        tags={'env': 'prod'}
    )
    assert accounts[1].tags == {}

@responses.activate
def test_get_cloudhealth_accounts_error():
    # Simulate server error
    responses.add(
        responses.GET,
        _api_url(),
        status=500
    )
    with pytest.raises(SystemExit):
        get_cloudhealth_accounts('fake_key', 12345)

@responses.activate
def test_update_cloudhealth_account_dry_run(caplog):
    caplog.set_level(logging.INFO)
    # Should not raise, just log
    update_cloudhealth_account(
        ch_id=99,
        new_name='NameX',
        new_tags={'k': 'v'},
        api_key='fake_key',
        client_api_id=123,
        dry_run=True
    )
    assert 'DRY RUN' in caplog.text

@responses.activate
def test_update_cloudhealth_account_put(caplog):
    caplog.set_level(logging.INFO)
    # Mock PUT without caring about query string
    responses.add(
        responses.PUT,
        f"{_api_url()}/99",
        status=200
    )
    update_cloudhealth_account(
        ch_id=99,
        new_name='NameY',
        new_tags={'a': 'b'},
        api_key='fake_key',
        client_api_id=123,
        dry_run=False
    )
    assert 'Updated CloudHealth account 99' in caplog.text

@responses.activate
def test_update_cloudhealth_account_put_error():
    # Simulate failure status
    responses.add(
        responses.PUT,
        f"{_api_url()}/100",
        status=404
    )
    with pytest.raises(SystemExit):
        update_cloudhealth_account(
            ch_id=100,
            new_name=None,
            new_tags=None,
            api_key='fake',
            client_api_id=0,
            dry_run=False
        )