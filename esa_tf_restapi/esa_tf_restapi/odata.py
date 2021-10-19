from collections import namedtuple

from odata_query.grammar import ODataLexer, ODataParser

ODataParams = namedtuple("OData", ["filter",])
ODataFilterExpr = namedtuple("ODataFilterEQ", ["name", "value", "operator"])

lexer = ODataLexer()
parser = ODataParser()


def parseQS(filter: str = None):
    odata_filter = parser.parse(lexer.tokenize(filter))
    filter = ODataFilterExpr(
        name=odata_filter.left.name, value=odata_filter.right.val, operator="eq"
    )
    print(odata_filter)
    return ODataParams(filter=filter)
