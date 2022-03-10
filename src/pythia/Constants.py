from src.pythia.Attribute import Attribute
## ATTRIBUTE TYPE
NUMERICAL = "numerical"
CATEGORICAL = "categorical"

## AMBIGUITIES TYPE
TYPE_ROW = 'row'
TYPE_ATTRIBUTE = 'attribute'
TYPE_FULL = 'full'
TYPE_FD = 'fd'
TYPE_FUNC = 'func'

## PANDAS INDEX
#INDEX = ['index']
INDEX = Attribute("index")
INDEX.type = NUMERICAL

## STRATEGIES AMBIGUITY ATTRIBUTES
STRATEGY_SCHEMA = "schema"
STRATEGY_SCHEMA_WITH_DATA_SAMPLE = "schemaWithDataSample"
STRATEGY_PAIRWISE_COMBINATION = "combination"
STRATEGY_PAIRWISE_PERMUTATION = "permutation"

## MATCH TYPE TEMPLATES
MATCH_TYPE_CONTRADICTING = 'contradicting'
MATCH_TYPE_UNIFORM_FALSE = 'uniform_false'
MATCH_TYPE_UNIFORM_TRUE = 'uniform_true'