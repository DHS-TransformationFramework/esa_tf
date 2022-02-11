import pytest

from esa_tf_restapi.odata import parse_qs


def test_parse_qs_empty():
    parsed = parse_qs(filter=None)
    assert parsed.filter == []
    assert parsed.count == False


def test_parse_qs():
    parsed = parse_qs(filter="name eq 'John'")
    assert parsed.filter[0]._asdict() == dict(name="name", operator="eq", value="John")
    assert parsed.count == False


def test_or_not_supported():
    with pytest.raises(NotImplementedError):
        parse_qs(filter="name eq 'John' or name eq 'Jack'")


def test_some_operators_not_supported():
    with pytest.raises(NotImplementedError):
        parse_qs(filter="name ne 'John'")


def test_parse_and_separator():
    parsed = parse_qs(
        filter="name eq 'John' and surname eq 'Smith' and middle_name eq 'JJ' and birthday eq '1980-01-01'"
    )
    assert parsed.filter[0]._asdict() == dict(name="name", operator="eq", value="John")
    assert parsed.filter[1]._asdict() == dict(
        name="surname", operator="eq", value="Smith"
    )
    assert parsed.filter[2]._asdict() == dict(
        name="middle_name", operator="eq", value="JJ"
    )
    assert parsed.filter[3]._asdict() == dict(
        name="birthday", operator="eq", value="1980-01-01"
    )
    assert parsed.count == False


def test_ltgt_operators():
    parsed = parse_qs(filter="value gt '1980-01-01' and value lt '1980-06-01'")
    assert parsed.filter[0]._asdict() == dict(
        name="value", operator="gt", value="1980-01-01"
    )
    assert parsed.filter[1]._asdict() == dict(
        name="value", operator="lt", value="1980-06-01"
    )
    assert parsed.count == False


def test_count():
    parsed = parse_qs(filter="name eq 'John'", count=True)
    assert parsed.filter[0]._asdict() == dict(name="name", operator="eq", value="John")
    assert parsed.count == True
