import pandas
from itertools import chain, combinations
from functools import cmp_to_key
from pandas.core.dtypes.common import is_numeric_dtype
from src.pythia.Constants import NUMERICAL
from src.pythia.T5EngineMock import T5EngineMock
from src.pythia.Tane import getTaneFDs
from src.pythia.Constants import STRATEGY_SCHEMA, STRATEGY_SCHEMA_WITH_DATA_SAMPLE, STRATEGY_PAIRWISE_COMBINATION, STRATEGY_PAIRWISE_PERMUTATION

t5Engine = T5EngineMock()  ## TODO: in the web app this shuold be a singleton

def _key_options(items, dtypes, useNumerical=False):
    if useNumerical == False:
        newItems = []
        for attrName, attrType in dtypes.items():
            if is_numeric_dtype(attrType) == False:
                newItems.append(attrName)
        items = newItems
    return chain.from_iterable(combinations(items, r) for r in range(1, len(items)+1) )

def _toAttrName(tuple, mappings, isRHS):
    listAttrName = []
    if isRHS:
        #listAttrName.append(mappings[tuple])
        listAttrName.append(mappings.get(tuple))
    else:
        for t in tuple:
            #listAttrName.append(mappings[t])
            listAttrName.append(mappings.get(t))
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

def _notIn(candidate, listOfCandidates):
    if (len(listOfCandidates) == 0):
        return True
    for candidateInList in listOfCandidates:
        if (all(x in candidate for x in candidateInList)):
            return False
    return True

def getCompositeKeys(file, maxSizeKey):
    df = pandas.read_csv(file)
    dtypes = df.infer_objects().dtypes
    candidates = []
    # iterate over all combos of headings, excluding ID for brevity
    for candidate in _key_options(list(df)[1:], dtypes):
        deduped = df.drop_duplicates(candidate)
        if len(deduped.index) == len(df.index) and len(candidate) <= maxSizeKey:
            candidates.append(_toList(candidate))
    ## sort the candidates such as the first element is the ones with the highest number of unique values
    def compare(item1, item2):
        listItem1 = df[item1].unique().tolist()
        listItem2 = df[item2].unique().tolist()
        if len(listItem1) < len(listItem2):
            return 1
        elif len(listItem1) > len(listItem2):
            return -1
        else:
            return 0
    sortedCandidates = []
    for candidate in candidates:
        sortedCandidate = sorted(candidate, key=cmp_to_key(compare))
        sortedCandidates.append(sortedCandidate)
    sortedMinimal = []
    ## prune not minimal keys
    for candidate in sortedCandidates:
        if _notIn(candidate, sortedMinimal):
            sortedMinimal.append(candidate)
    return sortedMinimal

def getFDs(file, attributes):
    df = pandas.read_csv(file)
    dtypes = df.infer_objects().dtypes
    columnToDrop = []
    for attrName, attrType in dtypes.items():
        if is_numeric_dtype(attrType):
            columnToDrop.append(attrName)
    dfExport = df.copy()
    dfExport = dfExport.drop(columns=columnToDrop)
    fileExport = file + "_tanefd"
    dfExport.to_csv(fileExport, index=False)
    #taneFDs = getTaneFDs(file)
    taneFDs = getTaneFDs(fileExport)
    index = 0
    mappings = {}
    #for col in df.columns:
    for col in dfExport.columns:
        mappings[index] = col
        index+=1
    fds = []
    for LHS, RHS in taneFDs:
        LHSAttr = _toAttrName(LHS, mappings, False)
        RHSAttr = _toAttrName(RHS, mappings, True)
        #if _isCategorical(LHSAttr, RHSAttr, attributes):
        #    #fd = LHSAttr + RHSAttr
        #    fd = LHS + RHS
        #    fds.append(fd)
        fd = LHSAttr + RHSAttr
        isNone = False
        for attr in fd:
            if attr is None:
                isNone = True
        if not isNone:
            fds.append(fd)
    return fds

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
