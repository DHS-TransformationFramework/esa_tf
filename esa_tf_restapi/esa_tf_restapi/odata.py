from collections import namedtuple

from odata_query.ast import And, BoolOp, Eq, Gt, GtE, Lt, LtE, Or
from odata_query.grammar import ODataLexer, ODataParser

ODataParams = namedtuple("OData", ["filter", "count"], defaults=[[], False])
ODataFilterExpr = namedtuple("ODataFilter", ["name", "operator", "value"])

lexer = ODataLexer()
parser = ODataParser()


def parse_qs(filter: str = None, count: bool = False):
    odata_params = ODataParams(count=count)
    if filter:
        odata_filter = parser.parse(lexer.tokenize(filter))
        odata_params = odata_params._replace(
            filter=[*_get_inner_expr([], odata_filter)]
        )

    return odata_params


def _get_operator(op_type):
    types = {
        "Eq()": "eq",
        "Lt()": "lt",
        "Gt()": "gt",
        "LtE()": "le",
        "GtE()": "ge",
    }
    operator = types.get(str(op_type))
    if operator is None:
        raise NotImplementedError(f"Operator {str(op_type)} not supported")
    return operator


def _get_inner_expr(all_params: list, expr: BoolOp):
    if hasattr(expr, "op"):
        return [
            *_get_inner_expr(all_params, expr.left),
            *_get_inner_expr(all_params, expr.right),
        ]
    else:
        return [
            *all_params,
            ODataFilterExpr(
                name=expr.left.name,
                operator=_get_operator(expr.comparator),
                value=expr.right.val,
            ),
        ]
