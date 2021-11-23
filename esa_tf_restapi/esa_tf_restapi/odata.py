from collections import namedtuple

from odata_query.grammar import ODataLexer, ODataParser

ODataParams = namedtuple("OData", ["filter"])
ODataFilterExpr = namedtuple("ODataFilterEQ", ["name", "value", "operator"])

lexer = ODataLexer()
parser = ODataParser()


def parse_qs(filter: str = None):
    od_filter = ODataFilterExpr(None, None, None)
    if filter:
        odata_filter = parser.parse(lexer.tokenize(filter))
        od_filter = ODataFilterExpr(
            name=odata_filter.left.name, value=odata_filter.right.val, operator="eq"
        )
    return ODataParams(filter=od_filter)
