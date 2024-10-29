import datetime as dt
import json
from typing import Dict

import pytest
import pytz
from aleph_message.models import ItemType

from aleph.schemas.pending_messages import (
    BasePendingMessage,
    PendingAggregateMessage,
    PendingForgetMessage,
    PendingPostMessage,
    PendingProgramMessage,
    PendingStoreMessage,
    parse_message,
)
from aleph.types.message_status import InvalidMessageException


def check_basic_message_fields(pending_message: BasePendingMessage, message_dict: Dict):
    assert pending_message.chain == message_dict["chain"]
    assert pending_message.item_hash == message_dict["item_hash"]
    assert pending_message.sender == message_dict["sender"]
    assert pending_message.type == message_dict["type"]
    assert pending_message.channel == message_dict["channel"]
    assert pending_message.signature == message_dict["signature"]
    assert pending_message.channel == message_dict["channel"]
    assert pending_message.time == pytz.utc.localize(
        dt.datetime.utcfromtimestamp(message_dict["time"])
    )


def test_parse_aggregate_inline_message():
    message_dict = {
        "chain": "ETH",
        "item_hash": "6127637a9415444e62843f62c81a9dda708363b3bb830a5b10fcc212cd586fa9",
        "sender": "0x51A58800b26AA1451aaA803d1746687cB88E0501",
        "type": "AGGREGATE",
        "channel": "UNSLASHED",
        "item_content": '{"address":"0x51A58800b26AA1451aaA803d1746687cB88E0501","key":"0x93463a4f3af42de28f6840e59de6111b4192cf8bbinance","content":{"1652716953956":{"version":"x25519-xsalsa20-poly1305","nonce":"eohzj0i+fiaduqOcRnKyHVoTN19Gdv1N","ephemPublicKey":"eAWtQ7A0qA1b/VpnuexR098LFzQhw4/wbneri+XuBgA=","ciphertext":"qsWudj1UrcC5qbdZgzks3h8OF+kOD/CpB5og7zZXNj3zDYoXA+0CLYErX8gsN7yuJQNd+MciEHoxfQZWKtulR9+RMxQrD7DnNUE4y0ick3aFKjXAcJLcbCKOXllo5p9hxq1o/VONJor4uiHc97UhRTK1RXQUNdKz+V+RknAPrlamDcv3LJopm9zdMoxw5hRYIpF3fKH5natLkvc7EzmeQ3Bvo3mUtcyHWZZqmWpmtjVNFf1I+OHgFz4SK4O8nLq2fDPW8EtJBwTTfKeUIhj9B34V4+gl4O+822e4Tnbi4sTFGREc2lSqwDME4u4qyYMaM7omFYcfVvLwBHtIOoSl21xtaPh7g5q8z9lqlWJQTE9ZE2z0kKUuDLPCcrLMm1ooc69pSKPn2W87ycAWyN1v88i5KIU8vIBraWlFY76ROlHScw1L05PKI+1S03demsgNaT+hNTxGwSBOuEq7P25aJjiL3eKXZZ+lZONtSbegxTfFBg==","sha256":"99b9660c915e6ba26fb52cc7ae2735e73b1aeebd167db762e6e4b3d6ec85235f"}},"time":1652716955.776}',
        "item_type": "inline",
        "signature": "0xb0b97e102f7f75091306ec3ac39639a9560783a1652ed9f1c79138cafc77b9a1519a5cccabf7eb64a8cf4de11394757d95f9d7218833f85010ee847b77716c201c",
        "time": 1652716955.776,
    }

    message = parse_message(message_dict)
    assert isinstance(message, PendingAggregateMessage)

    check_basic_message_fields(message, message_dict)

    assert message.content is not None
    content = json.loads(message_dict["item_content"])
    assert message.content.address == content["address"]
    assert message.content.time == content["time"]
    assert message.content.key == content["key"]
    assert message.content.content == content["content"]


def test_parse_post_message_storage_content():
    message_dict = {
        "chain": "ETH",
        "item_hash": "QmWx3j1gSQUrBkYnA8wiuhmE5wGuVNKv5wW6L7RHgLi4H4",
        "sender": "0x06DE0C46884EbFF46558Cd1a9e7DA6B1c3E9D0a8",
        "type": "POST",
        "channel": None,
        "item_content": None,
        "item_type": "ipfs",
        "signature": "0xa1d9fadcf5e6613f6929aa18720c216763a4c04d1462c6e10b81b37d8b2b7fd42618f7889fd2b29d4940d5cb68b6eb24243b51fa932dec6d96de9bbb7e64f91d1c",
        "time": 1608297192.085,
    }

    message = parse_message(message_dict)
    assert isinstance(message, PendingPostMessage)

    check_basic_message_fields(message, message_dict)
    assert message.content is None


def test_parse_store_message_inline_content():
    message_dict = {
        "chain": "NULS2",
        "item_hash": "4bbcfe7c4775492c2e602d322d68f558891468927b5e0d6cb89ff880134f323e",
        "sender": "NULSd6Hgbhr42Dm5nEgf6foEUT5bgwHesZQJB",
        "type": "STORE",
        "channel": "MYALEPH",
        "item_content": '{"address":"NULSd6Hgbhr42Dm5nEgf6foEUT5bgwHesZQJB","item_type":"ipfs","item_hash":"QmUDS8mpQmpPyptyUEedHxHMkxo7ueRRiAvrpgvJMpjXwW","time":1577325086.513}',
        "item_type": "inline",
        "signature": "G7/xlWoMjjOr1NBN4SiZ8USYYVM9Q3JHXChR9hPw9/YSItfAplshWysqYDkvmBZiwbICG0IVB3ilMPJ/ZVgPNlk=",
        "time": 1608297193.717,
    }

    message = parse_message(message_dict)
    assert isinstance(message, PendingStoreMessage)

    check_basic_message_fields(message, message_dict)

    assert message.content is not None
    content = json.loads(message_dict["item_content"])
    assert message.content.address == content["address"]
    assert message.content.time == content["time"]
    assert message.content.item_hash == content["item_hash"]
    assert message.content.item_type == content["item_type"]


def test_parse_store_message_storage_content():
    message_dict = {
        "chain": "ETH",
        "item_hash": "30cc40533aa3ccf16a7c7c8a40da5633f64a83e4b89dcc7815f3a0af2149e1ac",
        "sender": "0x7332eA1229c11C627C10eB24c1A6F77BceD1D5c1",
        "type": "STORE",
        "channel": "EVIDENZ",
        "item_content": None,
        "item_type": "storage",
        "signature": "23d1d099dd111ae3251efea537f57767cf43b2ae3611bf9051760e0a9bc2bd4429563a130e3e391668086d101f8a197f55377f50b15d4c0303ff957d90a258a31b",
        "time": 1616021679.055,
    }

    message = parse_message(message_dict)
    assert isinstance(message, PendingStoreMessage)

    check_basic_message_fields(message, message_dict)
    assert message.content is None
    assert message.item_content is None


def test_parse_forget_message():
    message_dict = {
        "chain": "ETH",
        "item_hash": "884dd713e94fa0350239b67e65eecaa54361df8af0e3f6d0e42e0f8de059e15a",
        "sender": "0xB68B9D4f3771c246233823ed1D3Add451055F9Ef",
        "type": "FORGET",
        "channel": "TEST",
        "item_content": '{"address":"0xB68B9D4f3771c246233823ed1D3Add451055F9Ef","time":1639058312.376,"hashes":["e3b24727335e34016247c0d37e2b0203bb8c2d76deddafc1700b4cf0e13845c5"],"reason":"None"}',
        "item_type": "inline",
        "signature": "0x7dc7a45aab12d78367c085799d06ef2e98fce31f76ca06975ce570fe4d92008f66f307bf68ed3ca450d04d4e779776ca13a1e7851cb48915bd390389ae4afd1b1c",
        "time": 1639058312.376,
    }

    message = parse_message(message_dict)
    assert isinstance(message, PendingForgetMessage)

    assert message.content is not None
    check_basic_message_fields(message, message_dict)
    content = json.loads(message_dict["item_content"])
    assert message.content.address == content["address"]
    assert message.content.time == content["time"]
    assert message.content.hashes == content["hashes"]


def test_parse_program_message():
    message_dict = {
        "chain": "ETH",
        "item_hash": "2feafebd2dcc023851cbe461ba09000c6ea7ddf2db6dbb31ae8b627556382ba7",
        "sender": "0x101d8D16372dBf5f1614adaE95Ee5CCE61998Fc9",
        "type": "PROGRAM",
        "channel": "TEST",
        "item_content": '{"address":"0x101d8D16372dBf5f1614adaE95Ee5CCE61998Fc9","time":1627465647.9127016,"type":"vm-function","allow_amend":false,"code":{"encoding":"zip","entrypoint":"main:app","ref":"3631866c6237ff84c546e43b5679111b419c7044e0c367f357dbc7dd8ad21a5a","use_latest":true},"on":{"http":true},"environment":{"reproducible":false,"internet":true,"aleph_api":true,"shared_cache":false},"resources":{"vcpus":1,"memory":128,"seconds":30},"runtime":{"ref":"bd79839bf96e595a06da5ac0b6ba51dea6f7e2591bb913deccded04d831d29f4","use_latest":true,"comment":"Aleph Alpine Linux with Python 3.8"},"volumes":[]}',
        "item_type": "inline",
        "signature": "0x167b4558fd2f806bab7ef14d1f92723dd1616d5806075ba95e5ebbe4860a47b2613a2205c507525e8e5f8c7251e1a5c5963a12f7f2343e93c4b9b6e402fbb9bf1b",
        "time": 1627465978.121,
    }

    message = parse_message(message_dict)
    assert isinstance(message, PendingProgramMessage)

    check_basic_message_fields(message, message_dict)

    assert message.content is not None
    content = json.loads(message_dict["item_content"])
    assert message.content.address == content["address"]
    assert message.content.time == content["time"]
    assert message.content.code.model_dump(exclude_none=True) == content["code"]
    assert message.content.type == content["type"]


def test_default_item_type_inline():
    # Note: we reuse the fixture of test_parse_program_message here
    message_dict = {
        "chain": "ETH",
        "item_hash": "2feafebd2dcc023851cbe461ba09000c6ea7ddf2db6dbb31ae8b627556382ba7",
        "sender": "0x101d8D16372dBf5f1614adaE95Ee5CCE61998Fc9",
        "type": "PROGRAM",
        "channel": "TEST",
        "item_content": '{"address":"0x101d8D16372dBf5f1614adaE95Ee5CCE61998Fc9","time":1627465647.9127016,"type":"vm-function","allow_amend":false,"code":{"encoding":"zip","entrypoint":"main:app","ref":"3631866c6237ff84c546e43b5679111b419c7044e0c367f357dbc7dd8ad21a5a","use_latest":true},"on":{"http":true},"environment":{"reproducible":false,"internet":true,"aleph_api":true,"shared_cache":false},"resources":{"vcpus":1,"memory":128,"seconds":30},"runtime":{"ref":"bd79839bf96e595a06da5ac0b6ba51dea6f7e2591bb913deccded04d831d29f4","use_latest":true,"comment":"Aleph Alpine Linux with Python 3.8"},"volumes":[]}',
        "signature": "0x167b4558fd2f806bab7ef14d1f92723dd1616d5806075ba95e5ebbe4860a47b2613a2205c507525e8e5f8c7251e1a5c5963a12f7f2343e93c4b9b6e402fbb9bf1b",
        "time": 1627465978.121,
    }

    message = parse_message(message_dict)
    assert message.item_type == ItemType.inline


def test_default_item_type_ipfs():
    # Note: we reuse the fixture of test_parse_post_message_storage_content here
    message_dict = {
        "chain": "ETH",
        "item_hash": "QmcS6md3AHR62rbmrnjy6SzJkunTsqtc6XhAuzYkYV66m4",
        "sender": "0x06DE0C46884EbFF46558Cd1a9e7DA6B1c3E9D0a8",
        "type": "POST",
        "channel": None,
        "item_content": None,
        "signature": "0xc728c7ddc9d8ec930465915d866c7bcc7b304fb15e95b753aa89f8c0ca143bad5b6e7cbec66a098f4c8435e1bb65c0d48913241a2b95285b12c12eb2ea8f12971b",
        "time": 1608297192.104,
    }

    message = parse_message(message_dict)
    assert message.item_type == ItemType.ipfs


def test_invalid_item_type():
    """
    Tests a message with an uppercase item type instead of lower case.
    """

    message_dict = {
        "_id": None,
        "chain": "ETH",
        "channel": "ANIMA_MAINNET",
        "confirmed": False,
        "item_hash": "c6c9df8d5b5dcd5cf74562c9d383308c354a5238d3b0d9db10d931b250490600",
        "item_type": "STORAGE",
        "sender": "0x989Cfa25243C07b90Bedc673Fe6Df69B5B0D675C",
        "signature": "0x63e9dd351ff6735bb41c2658d95828bd0dbc14fc64eee119114fd7e84fa737f157c3b38ae7c5ca30a73c03e3794f35c94282b5fc9379d31b43f913d1c18300ca01",
        "time": 1653989834,
        "type": "AGGREGATE",
    }

    with pytest.raises(InvalidMessageException):
        _ = parse_message(message_dict)


def test_invalid_chain():
    """
    Tests a message sent on a chain that does not exist.
    """

    message_dict = {
        "_id": None,
        "chain": "MEGA_CHAIN",
        "channel": "ANIMA_MAINNET",
        "confirmed": False,
        "item_hash": "c6c9df8d5b5dcd5cf74562c9d383308c354a5238d3b0d9db10d931b250490600",
        "item_type": "storage",
        "sender": "0x989Cfa25243C07b90Bedc673Fe6Df69B5B0D675C",
        "signature": "0x63e9dd351ff6735bb41c2658d95828bd0dbc14fc64eee119114fd7e84fa737f157c3b38ae7c5ca30a73c03e3794f35c94282b5fc9379d31b43f913d1c18300ca01",
        "time": 1653989834,
        "type": "AGGREGATE",
    }

    with pytest.raises(InvalidMessageException):
        _ = parse_message(message_dict)


def test_invalid_hash():
    """
    Tests a message sent on a chain that does not exist.
    """

    message_dict = {
        "_id": None,
        "chain": "ETH",
        "channel": "ANIMA_MAINNET",
        "confirmed": False,
        "item_hash": "deadbeef",
        "item_type": "storage",
        "sender": "0x989Cfa25243C07b90Bedc673Fe6Df69B5B0D675C",
        "signature": "0x63e9dd351ff6735bb41c2658d95828bd0dbc14fc64eee119114fd7e84fa737f157c3b38ae7c5ca30a73c03e3794f35c94282b5fc9379d31b43f913d1c18300ca01",
        "time": 1653989834,
        "type": "AGGREGATE",
    }

    with pytest.raises(InvalidMessageException):
        _ = parse_message(message_dict)


def test_invalid_time_field():
    message_dict = {
        "_id": None,
        "chain": "ETH",
        "channel": "ANIMA_MAINNET",
        "confirmed": False,
        "item_hash": "c6c9df8d5b5dcd5cf74562c9d383308c354a5238d3b0d9db10d931b250490600",
        "item_type": "storage",
        "sender": "0x989Cfa25243C07b90Bedc673Fe6Df69B5B0D675C",
        "signature": "0x63e9dd351ff6735bb41c2658d95828bd0dbc14fc64eee119114fd7e84fa737f157c3b38ae7c5ca30a73c03e3794f35c94282b5fc9379d31b43f913d1c18300ca01",
        "time": 255402210800,
        "type": "AGGREGATE",
    }
    with pytest.raises(InvalidMessageException) as exc_info:
        _ = parse_message(message_dict)
    exc_details = exc_info.value.details()
    assert "time" in exc_details["errors"][0]["loc"]


def test_parse_none():
    with pytest.raises(InvalidMessageException):
        _ = parse_message(None)


def test_parse_empty_dict():
    with pytest.raises(InvalidMessageException):
        _ = parse_message({})


def test_parse_storage_with_item_content():
    with pytest.raises(InvalidMessageException):
        _ = parse_message({})
