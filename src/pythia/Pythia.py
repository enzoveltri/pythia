from PrintFunctions import printf, printo
from DBUtils import executeQueryBatch
from Constants import CATEGORICAL, NUMERICAL, TYPE_ROW, TYPE_ATTRIBUTE, TYPE_FD, TYPE_FULL
from StringUtils import findTokens

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

def checkOperatorWithType(attribute, operator):
    type = attribute[1]
    if type == CATEGORICAL and ((operator == '>') or (operator == '<')):
        return False
    return True

def attributeStrategyTemplate(template, ambiguities, pk,tableName, operator, mt, printConfig):
    a_queries = []
    type = template[1]
    if type != TYPE_ATTRIBUTE:
        print("*** ERROR. Not an ATTRIBUTE template ", template)
        return None
    for a1, a2, label in ambiguities:
        if checkOperatorWithType(a1, operator) == False:
            continue
        a_query = template[0]
        a_query = a_query.replace('$PK$', ('"'+pk+'"'))
        a_query = a_query.replace('$TABLE$', tableName)
        a_query = a_query.replace('$OPERATOR$', operator)
        a_query = a_query.replace('$AMB_1$', '"'+a1[0]+'"')
        a_query = a_query.replace('$AMB_2$', '"'+a2[0]+'"')
        printf_string = printf(operator, label, printConfig).strip()
        if mt == 'contradicting':
            a_query = a_query.replace('$MT_OPERATOR$', negOperator(operator))
        else:
            a_query = a_query.replace('$MT_OPERATOR$', operator)
            if mt == 'uniform_false':
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
        if checkOperatorWithType(a1, operator) == False:
            continue
        a_query = a_query_updated
        a_query = a_query.replace('$SUB_PK$', ('"' + subpk + '"'))
        a_query = replacePKs(a_query, ck, subpk)
        a_query = a_query.replace('$TABLE$', tableName)
        a_query = a_query.replace('$OPERATOR$', operator)
        a_query = a_query.replace('$AMB_1$', '"'+a1[0]+'"')
        a_query = a_query.replace('$AMB_2$', '"'+a2[0]+'"')
        printf_string = printf(operator, label, printConfig).strip()
        if mt == 'contradicting':
            a_query = a_query.replace('$MT_OPERATOR$', negOperator(operator))
        else:
            a_query = a_query.replace('$MT_OPERATOR$', operator)
            if mt == 'uniform_false':
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
    subpk = ck[0]
    a_query_original = template[0]
    a_query_updated = updatePKsFromCk(a_query_original, ck)
    #key = ck[1]
    for a in attrList:
        if checkOperatorWithType(a, operator) == False:
            continue
        a_query = a_query_updated
        a_query = a_query.replace('$SUB_PK$', ('"' + subpk + '"'))
        a_query = replacePKs(a_query, ck, subpk)
        a_query = a_query.replace('$A1$', ('"' + a[0] + '"'))
        a_query = a_query.replace('$A1_NAME$', ("' " + a[0] + "'"))
        a_query = a_query.replace('$TABLE$', tableName)
        printo_string = printo(operator, printConfig).strip()
        if mt == 'contradicting':
            a_query = a_query.replace('$MT_OPERATOR$', negOperator(operator))
        else:
            a_query = a_query.replace('$MT_OPERATOR$', operator) ## uniform_true
            if mt == 'uniform_false':
                printo_string = printo(negOperator(operator), printConfig).strip()
        a_query = a_query.replace('$PRINT_O$', " ' " + printo_string + " '")
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

def fdStrategyTemplate(template, fd, pk, tableName):
    type = template[1]
    if type != TYPE_FD:
        print("*** ERROR. Not an FD ", fd)
        return None
    lhs = fd[0]
    rhs = fd[1]
    values = fd[2]
    a_query = template[0]
    a_query = a_query.replace('$LHS_NAME$', ("' " + lhs + "'"))
    a_query = a_query.replace('$LHS$', ('"' + lhs + '"'))
    a_query = a_query.replace('$RHS$', ('"' + rhs + '"'))
    a_query = a_query.replace('$PK$', ('"' + pk + '"'))
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
def find_a_queries(table, templates, matchType, connection,
                   operatorPrintConfigRow, operatorPrintConfigAttribute, operators=["=", ">", "<"], executeQuery=True,
                   limitQueryResults = -1, shuffleQuery = True):
    ## collect metainformation from table
    ambiguities = table[0]
    pk = table[1]
    fds = table[2]
    compositeKeys = table[3]
    tableName = table[4]
    attributes = table[5]
    a_queries_with_data = []
    stored_results = [] ## list of <a_query, type, result, template>
    for template in templates:
        type = template[1]
        if (type == TYPE_FD) and (len(fds) > 0):
            print("*** GENERATING A-QUERIES for FDS")
            for fd in fds:
                a_query = fdStrategyTemplate(template, fd, pk, tableName)
                #print(a_query)
                if (a_query is not None and executeQuery):
                    if shuffleQuery:
                        a_query += " ORDER BY random()"
                    checkWithData(a_query, type, connection, a_queries_with_data, stored_results, template, fd, limitQueryResults)
        if (type == TYPE_ROW) and (len(compositeKeys) > 0):
            print("*** GENERATING A-QUERIES for ROWs")
            for ck in compositeKeys:
                for operator in operators:
                    a_queries = rowStrategyTemplate(template, ck, tableName, attributes, operator, matchType, operatorPrintConfigRow)
                    for a_query in a_queries:
                        #print(a_query)
                        if (a_query is not None and executeQuery):
                            if shuffleQuery:
                                a_query += " ORDER BY random()"
                            checkWithData(a_query, type, connection, a_queries_with_data, stored_results, template, None, limitQueryResults)
        if (type == TYPE_ATTRIBUTE) and (len(ambiguities) > 0):
            print("*** GENERATING A-QUERIES for ATTRIBUTEs")
            for operator in operators:
                a_queries = attributeStrategyTemplate(template, ambiguities, pk, tableName, operator, matchType, operatorPrintConfigAttribute)
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
                    a_queries = fullStrategyTemplate(template, ambiguities, ck, tableName, operator, matchType, operatorPrintConfigAttribute)
                    #a_queries = attributeStrategyTemplate(template, ambiguities, subpk, tableName, operator, matchType, operatorPrintConfigAttribute)
                    #rowStrategyTemplate(template, ck, tableName, attributes, operator, matchType, operatorPrintConfigRow)
                    for a_query in a_queries:
                        #print(a_query)
                        if (a_query is not None and executeQuery):
                            if shuffleQuery:
                                a_query += " ORDER BY random()"
                            checkWithData(a_query, type, connection, a_queries_with_data, stored_results, template, None, limitQueryResults)

    return a_queries_with_data, stored_results