from PrintFunctions import printf, printo, prinfFunction
from DBUtils import executeQueryBatch
from Constants import CATEGORICAL, NUMERICAL, TYPE_ROW, TYPE_ATTRIBUTE, TYPE_FD, TYPE_FULL, TYPE_FUNC, MATCH_TYPE_CONTRADICTING, MATCH_TYPE_UNIFORM_TRUE, MATCH_TYPE_UNIFORM_FALSE
from StringUtils import findTokens
from src.pythia.Constants import INDEX
## UTILS FUCTIONS

def toListCompositeKeys(compositeKeys):
    tmpSet = set()
    for ck in compositeKeys:
        for a in ck:
            tmpSet.add(a)
    return list(tmpSet)

def checkAQueryComplete(a_query):
    if ("$" in a_query):
        return False
    return True

## NEG OPERATOR
## TODO: add more operators
def negOperator(operator):
    if operator == "=":
        return "<>"
    if operator == ">":
        return "<"
    if operator == "<":
        return ">"
    print("**** KEY ERROR: ", operator)
    return None

def negFunctions(function):
    if function == "min":
        return ">"
    if function == "max":
        return "<"
    print("**** KEY ERROR: ", function)
    return None

def checkOperatorWithType(attribute1, attribute2, operator):
    type1 = attribute1[1]
    type2 = None
    if (attribute2 is not None):
        type2 = attribute2[1]
    if (type2 is not None) and (type1 != type2):
        return False
    if type1 == CATEGORICAL and \
            ((operator == '>') or (operator == '<')
            or (operator == "min") or (operator == "max") or (operator == "avg")):
        return False
    return True

def attributeStrategyTemplate(template, ambiguities, pk,tableName, operator, mt, printConfig):
    a_queries = []
    type = template[1]
    if type != TYPE_ATTRIBUTE:
        print("*** ERROR. Not an ATTRIBUTE template ", template)
        return None
    for a1, a2, label in ambiguities:
        if checkOperatorWithType(a1, a2, operator) == False:
            continue
        a_query = template[0]
        a_query = a_query.replace('$PK$', ('"'+pk[0]+'"'))
        a_query = a_query.replace('$TABLE$', tableName)
        a_query = a_query.replace('$OPERATOR$', operator)
        a_query = a_query.replace('$AMB_1$', '"'+a1[0]+'"')
        a_query = a_query.replace('$AMB_2$', '"'+a2[0]+'"')
        printf_string = printf(operator, label, printConfig).strip()
        if mt == MATCH_TYPE_CONTRADICTING:
            a_query = a_query.replace('$MT_OPERATOR$', negOperator(operator))
        else:
            a_query = a_query.replace('$MT_OPERATOR$', operator)
            if mt == MATCH_TYPE_UNIFORM_FALSE:
                printf_string = printf(negOperator(operator), label, printConfig).strip()
        a_query = a_query.replace('$PRINT_F$', "' "+printf_string+" '")
        if checkAQueryComplete(a_query):
            a_queries.append(a_query)
    return a_queries

def fullStrategyTemplate(template, ambiguities, ck,tableName, operator, mt, printConfig):
    a_queries = []
    type = template[1]
    if type != TYPE_FULL:
        print("*** ERROR. Not a FULL template ", template)
        return None
    subpk = ck[0]
    a_query_original = template[0]
    a_query_updated = updatePKsFromCk(a_query_original, ck)
    for a1, a2, label in ambiguities:
        if checkOperatorWithType(a1, a2, operator) == False:
            continue
        a_query = a_query_updated
        a_query = a_query.replace('$SUB_PK$', ('"' + subpk + '"'))
        a_query = replacePKs(a_query, ck, subpk)
        a_query = a_query.replace('$TABLE$', tableName)
        a_query = a_query.replace('$OPERATOR$', operator)
        a_query = a_query.replace('$AMB_1$', '"'+a1[0]+'"')
        a_query = a_query.replace('$AMB_2$', '"'+a2[0]+'"')
        printf_string = printf(operator, label, printConfig).strip()
        if mt == MATCH_TYPE_CONTRADICTING:
            a_query = a_query.replace('$MT_OPERATOR$', negOperator(operator))
            a_query = a_query.replace("$B_TABLE$", "b1")
        else:
            a_query = a_query.replace("$B_TABLE$", "b2")
            a_query = a_query.replace('$MT_OPERATOR$', operator)
            if mt == MATCH_TYPE_UNIFORM_FALSE:
                printf_string = printf(negOperator(operator), label, printConfig).strip()
        a_query = a_query.replace('$PRINT_F$', "' "+printf_string+" '")
        if checkAQueryComplete(a_query):
            a_queries.append(a_query)
    return a_queries

def rowStrategyTemplate(template, ck, tableName, attributes, operator, mt, printConfig):
    type = template[1]
    if type != TYPE_ROW:
        print("*** ERROR. Not a ROW template ", template)
        return None
    a_queries = []
    listAttr = toListCompositeKeys(ck)
    attrList = [x for x in attributes if x not in listAttr]
    if (INDEX[0], CATEGORICAL) in attrList: attrList.remove((INDEX[0], CATEGORICAL))
    subpk = ck[0]
    a_query_original = template[0]
    a_query_updated = updatePKsFromCk(a_query_original, ck)
    #key = ck[1]
    for a in attrList:
        if checkOperatorWithType(a, None, operator) == False:
            continue
        a_query = a_query_updated
        a_query = a_query.replace('$SUB_PK$', ('"' + subpk + '"'))
        a_query = replacePKs(a_query, ck, subpk)
        a_query = a_query.replace('$A1$', ('"' + a[0] + '"'))
        a_query = a_query.replace('$A1_NAME$', ("' " + a[0] + "'"))
        a_query = a_query.replace('$TABLE$', tableName)
        printo_string = printo(operator, printConfig).strip()
        if mt == MATCH_TYPE_CONTRADICTING:
            a_query = a_query.replace('$MT_OPERATOR$', negOperator(operator))
        else:
            a_query = a_query.replace('$MT_OPERATOR$', operator) ## uniform_true
            if mt == MATCH_TYPE_UNIFORM_FALSE:
                printo_string = printo(negOperator(operator), printConfig).strip()
        a_query = a_query.replace('$PRINT_O$', " ' " + printo_string + " '")
        if checkAQueryComplete(a_query):
            a_queries.append(a_query)
    return a_queries

def funcStrategyTemplate(template, ck, tableName, attributes, func, mt, printConfig):
    type = template[1]
    if type != TYPE_FUNC:
        print("*** ERROR. Not a FUNC template ", template)
        return None
    a_queries = []
    listAttr = toListCompositeKeys(ck)
    attrList = [x for x in attributes if x not in listAttr]
    if (INDEX[0], CATEGORICAL) in attrList: attrList.remove((INDEX[0], CATEGORICAL))
    subpk = ck[0]
    a_query_original = template[0]
    a_query_updated = updatePKsFromCk(a_query_original, ck)
    #key = ck[1]
    for a in attrList:
        if checkOperatorWithType(a, None, func) == False:
            continue
        a_query = a_query_updated
        a_query = a_query.replace('$SUB_PK$', ('"' + subpk + '"'))
        a_query = replacePKs(a_query, ck, subpk)
        a_query = a_query.replace('$A1$', ('"' + a[0] + '"'))
        a_query = a_query.replace('$A1_NAME$', ("' " + a[0] + "'"))
        a_query = a_query.replace('$TABLE$', tableName)
        a_query = a_query.replace('$FUNC$', func)
        printo_string = prinfFunction(func, printConfig).strip()
        if mt == MATCH_TYPE_CONTRADICTING:
            a_query = a_query.replace('$OP_FUNC$', "=")
            a_query = a_query.replace('$MT_OP_FUNC$', negOperator("="))
        else:
            a_query = a_query.replace('$OP_FUNC$', "=") ## uniform_true
            a_query = a_query.replace('$MT_OP_FUNC$', "=")  ## uniform_true
            if mt == MATCH_TYPE_UNIFORM_FALSE:
                a_query = a_query.replace('$OP_FUNC$', negFunctions(func))  ## uniform_true
                a_query = a_query.replace('$MT_OP_FUNC$', negFunctions(func))  ## uniform_true
                printo_string = prinfFunction(negFunctions(func), printConfig).strip()
        a_query = a_query.replace('$PRINT_FUNC$', " ' " + printo_string + " '")
        #print("*****", a_query)
        if checkAQueryComplete(a_query):
            a_queries.append(a_query)
    return a_queries

def replacePKs(a_query, ck, subpk):
    for pk_index in range(1, len(ck)):
        key = ck[pk_index]
        a_query = a_query.replace('$PK_' + str(pk_index) + '$', ('"' + key + '"'))
    return a_query

def updatePKsFromCk(a_query_original, ck):
    tokensReplace = findTokens(a_query_original, "$PK$")
    newTokens = []
    for pk_index in range(1, len(ck)):
        for token in tokensReplace:
            new_token = token.replace("PK", "PK_" + str(pk_index))
            newTokens.append(new_token)
    a_query = a_query_original.replace(' , '.join(tokensReplace), ' , '.join(newTokens))
    return a_query

def updateLHSFromLKS(a_query_original, lhs):
    tokensReplace = findTokens(a_query_original, "$LHS_NAME$")
    a_query = a_query_original
    for token in tokensReplace:
        newTokens = []
        for lhs_attr in range(0, len(lhs)):
            new_token = token.replace("LHS_NAME", "LHS_NAME_" + str(lhs_attr))
            newTokens.append(new_token)
        a_query = a_query.replace(token, ' , '.join(newTokens), 1)
    tokensReplace = findTokens(a_query, "$LHS$")
    #a_query = a_query_original
    for token in tokensReplace:
        newTokens = []
        for lhs_attr in range(0, len(lhs)):
            new_token = token.replace("LHS", "LHS_" + str(lhs_attr))
            newTokens.append(new_token)
        a_query = a_query.replace(token, ' , '.join(newTokens), 1)
    return a_query

def fdStrategyTemplate(template, fd, pk, tableName):
    type = template[1]
    if type != TYPE_FD:
        print("*** ERROR. Not an FD ", fd)
        return None
    fdNormalForm = fd[0]
    values = fd[1]
    lhs = fdNormalForm[:-1]
    rhs = fdNormalForm[-1]
    #lhs = fd[0]
    #rhs = fd[1]
    #values = fd[2]
    a_query_original = template[0]
    a_query = updateLHSFromLKS(a_query_original, lhs)
    attrNum = 0
    for attr in lhs:
        a_query = a_query.replace('$LHS_NAME_'+ str(attrNum)+'$',("' " + attr + "'"))
        attrNum+=1
    attrNum = 0
    for attr in lhs:
        a_query = a_query.replace('$LHS_'+ str(attrNum)+'$',('"' + attr + '"'))
        attrNum+=1
    #a_query = a_query.replace('$LHS_NAME$', ("' " + lhs + "'"))
    #a_query = a_query.replace('$LHS$', ('"' + lhs + '"'))
    a_query = a_query.replace('$RHS$', ('"' + rhs + '"'))
    a_query = a_query.replace('$PK$', ('"' + pk[0] + '"'))
    a_query = a_query.replace('$TABLE$', tableName)
    for value in values:
        a_query = a_query.replace('$PRINT_FD$', "' " + value + " '", 1)
    if checkAQueryComplete(a_query):
        return a_query
    return None

def checkWithData(a_query, type, connection, a_queries_with_data, stored_results, template, fd, limitQueryResults):
    if limitQueryResults > 0:
        a_query += "\nLIMIT "+ str(limitQueryResults)
    results = executeQueryBatch(a_query, connection)
    if len(results) > 0:
        a_queries_with_data.append(a_query)
        t_stored = (a_query, type, results, template, fd)
        stored_results.append(t_stored)

###################
## MAIN ALGORITHM
###################
## TODO: change to make the algorithm (see T.R. alg 2) robust wrt any Template given a predefined syntax
#def find_a_queries(table, templates, matchType, connection,
def find_a_queries(dataset, templates, matchType, connection,
                   #operatorPrintConfigRow, operatorPrintConfigAttribute, operators=["=", ">", "<"], executeQuery=True,
                   operators=["=", ">", "<"], functions=["max", "min"], executeQuery=True,
                   limitQueryResults = -1, shuffleQuery = True):
    ## collect metainformation from table
    ##ambiguities = table[0]
    ##pk = table[1]
    ##fds = table[2]
    ##compositeKeys = table[3]
    ##tableName = table[4]
    ##attributes = table[5]
    ## collect metainformation from dataset
    ambiguities = dataset.getAmbiguousAttributeNormalized()
    pk = dataset.getPk()
    fds = dataset.getNormalizedFDs()
    compositeKeys = dataset.getNormalizedCompositeKeys()
    tableName = dataset.getDatasetName()
    attributes = dataset.getNormalizedAttributes()
    a_queries_with_data = []
    stored_results = [] ## list of <a_query, type, result, template>
    for template in templates:
        type = template[1]
        print_f = template[2]
        if (type == TYPE_FD) and (len(fds) > 0):
            print("*** GENERATING A-QUERIES for FDS")
            for fd in fds:
                a_query = fdStrategyTemplate(template, fd, pk, tableName)
                #print(a_query)
                if (a_query is not None and executeQuery):
                    if shuffleQuery:
                        a_query += " ORDER BY random()"
                    checkWithData(a_query, type, connection, a_queries_with_data, stored_results, template, fd, limitQueryResults)
        if (type == TYPE_FUNC) and (len(functions) > 0):
            print("*** GENERATING A-QUERIES for FUNCs")
            for ck in compositeKeys:
                for func in functions:
                    a_queries = funcStrategyTemplate(template, ck, tableName, attributes, func, matchType, print_f)
                    for a_query in a_queries:
                        # print(a_query)
                        if (a_query is not None and executeQuery):
                            if shuffleQuery:
                                a_query += " ORDER BY random()"
                            print(a_query)
                            checkWithData(a_query, type, connection, a_queries_with_data, stored_results, template, None,
                                          limitQueryResults)
        if (type == TYPE_ROW) and (len(compositeKeys) > 0):
            print("*** GENERATING A-QUERIES for ROWs")
            for ck in compositeKeys:
                for operator in operators:
                    #a_queries = rowStrategyTemplate(template, ck, tableName, attributes, operator, matchType, operatorPrintConfigRow)
                    a_queries = rowStrategyTemplate(template, ck, tableName, attributes, operator, matchType, print_f)
                    for a_query in a_queries:
                        #print(a_query)
                        if (a_query is not None and executeQuery):
                            if shuffleQuery:
                                a_query += " ORDER BY random()"
                            checkWithData(a_query, type, connection, a_queries_with_data, stored_results, template, None, limitQueryResults)
        if (type == TYPE_ATTRIBUTE) and (len(ambiguities) > 0):
            print("*** GENERATING A-QUERIES for ATTRIBUTEs")
            for operator in operators:
                #a_queries = attributeStrategyTemplate(template, ambiguities, pk, tableName, operator, matchType, operatorPrintConfigAttribute)
                a_queries = attributeStrategyTemplate(template, ambiguities, pk, tableName, operator, matchType, print_f)
                for a_query in a_queries:
                    #print(a_query)
                    if (a_query is not None and executeQuery):
                        if shuffleQuery:
                            a_query += " ORDER BY random()"
                        checkWithData(a_query, type, connection, a_queries_with_data, stored_results, template, None, limitQueryResults)
        if (type == TYPE_FULL) and (len(ambiguities) > 0) and (len(compositeKeys) > 0):
            print("*** GENERATING A-QUERIES for FULL")
            for ck in compositeKeys:
                subpk = ck[0]
                for operator in operators:
                    #a_queries = fullStrategyTemplate(template, ambiguities, ck, tableName, operator, matchType, operatorPrintConfigAttribute)
                    a_queries = fullStrategyTemplate(template, ambiguities, ck, tableName, operator, matchType, print_f)
                    for a_query in a_queries:
                        #print(a_query)
                        if (a_query is not None and executeQuery):
                            if shuffleQuery:
                                a_query += " ORDER BY random()"
                            checkWithData(a_query, type, connection, a_queries_with_data, stored_results, template, None, limitQueryResults)

    return a_queries_with_data, stored_results