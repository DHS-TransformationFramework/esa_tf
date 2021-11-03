import pytest

from esa_tf_restapi.odata import ODataFilterExpr, ODataParams, parse_qs


def test_parse_qs_empty():
    assert parse_qs(filter=None) == ODataParams(
        filter=ODataFilterExpr(None, None, None)
    )


def test_parse_qs():
    filter = parse_qs(filter="name eq 'test'")
    assert filter == ODataParams(
        filter=ODataFilterExpr(name="name", value="test", operator="eq")
    )
