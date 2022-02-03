import pandas
from itertools import chain, combinations

from src.pythia.Constants import NUMERICAL
from src.pythia.T5EngineMock import T5EngineMock
from src.pythia.Tane import getTaneFDs
from src.pythia.Constants import STRATEGY_SCHEMA, STRATEGY_SCHEMA_WITH_DATA_SAMPLE, STRATEGY_PAIRWISE_COMBINATION, STRATEGY_PAIRWISE_PERMUTATION

t5Engine = T5EngineMock()  ## TODO: in the web app this shuold be a singleton

def _key_options(items):
    return chain.from_iterable(combinations(items, r) for r in range(1, len(items)+1) )

def _toAttrName(tuple, mappings, isRHS):
    listAttrName = []
    if isRHS:
        listAttrName.append(mappings[tuple])
    else:
        for t in tuple:
            listAttrName.append(mappings[t])
    return listAttrName

def _isCategorical(LHS, RHS, attributes):
    attributesType = {}
    for attr in attributes:
        attributesType[attr[0]] = attr[1]
    for attr in LHS:
        if attributesType[attr] == NUMERICAL:
            return False
    for attr in RHS:
        if attributesType[attr] == NUMERICAL:
            return False
    return True

def _toList(tuple):
    list = []
    for e in tuple:
        list.append(e)
    return list

def getCompositeKeys(file, maxSizeKey):
    df = pandas.read_csv(file)
    candidates = []
    # iterate over all combos of headings, excluding ID for brevity
    for candidate in _key_options(list(df)[1:]):
        deduped = df.drop_duplicates(candidate)
        if len(deduped.index) == len(df.index) and len(candidate) <= maxSizeKey:
            candidates.append(_toList(candidate))
    return candidates

def getFDs(file, attributes):
    taneFDs = getTaneFDs(file)
    df = pandas.read_csv(file)
    index = 0
    mappings = {}
    for col in df.columns:
        mappings[index] = col
        index+=1
    fds = []
    for LHS, RHS in taneFDs:
        LHSAttr = _toAttrName(LHS, mappings, False)
        RHSAttr = _toAttrName(RHS, mappings, True)
        if _isCategorical(LHSAttr, RHSAttr, attributes):
            fd = LHSAttr + RHSAttr
            fds.append(fd)

def getAmbiguousAttribute(prefix, attribute1, attribute2, strategy):
    requestString = prefix + " attr1: " + attribute1[0] + " attr2: " + attribute2[0]
    label = None
    if (strategy == STRATEGY_SCHEMA):
        ## TODO: request to the deployed task1
        label = t5Engine.makePrediction(requestString)
    if (strategy == STRATEGY_SCHEMA_WITH_DATA_SAMPLE):
        ## TODO: request to the deployed task3
        pass
    if (label == None or label == "None" or label == "none"):
        label = None
    return label
