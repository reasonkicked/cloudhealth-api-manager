import pytest
from finops_account_manager.aws import get_aws_accounts, AWSAccount

# Dummy classes to simulate boto3 behavior
class DummyPaginator:
    def __init__(self, pages):
        self._pages = pages

    def paginate(self):
        yield from self._pages

class DummyClient:
    def __init__(self, pages):
        self._pages = pages

    def get_paginator(self, name):
        assert name == "list_accounts"
        return DummyPaginator(self._pages)

class DummySession:
    def __init__(self, pages):
        self._pages = pages

    def client(self, svc):
        assert svc == "organizations"
        return DummyClient(self._pages)

def test_get_aws_accounts_success(monkeypatch):
    # Prepare fake pages
    pages = [
        {"Accounts": [{"Id": "111111111111", "Name": "FirstAcct"}]},
        {"Accounts": [{"Id": "222222222222"}]}  # missing Name should default to ""
    ]

    # Monkey-patch boto3.Session to return our dummy
    monkeypatch.setattr("boto3.Session", lambda **kwargs: DummySession(pages))

    accounts = get_aws_accounts(profile="anything")
    assert isinstance(accounts, list)
    assert accounts == [
        AWSAccount(account_id="111111111111", name="FirstAcct"),
        AWSAccount(account_id="222222222222", name=""),
    ]

def test_get_aws_accounts_org_error(monkeypatch):
    # Simulate an exception when creating the client
    def bad_session(**kwargs):
        raise Exception("Unable to load credentials")

    monkeypatch.setattr("boto3.Session", bad_session)

    with pytest.raises(SystemExit):
        get_aws_accounts(profile=None)
