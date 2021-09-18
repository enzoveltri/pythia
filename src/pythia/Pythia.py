from PrintFunctions import printf, printo
from DBUtils import executeQueryBatch
from Constants import CATEGORICAL, NUMERICAL, TYPE_ROW, TYPE_ATTRIBUTE, TYPE_FD, TYPE_FULL

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

######## TO DEL
def fdStrategy(templates, fds, pk, tableName):
    a_queries = []
    for template, type in templates:
        if type != 'fd':
            continue
        for fd in fds:
            a_query = fdStrategyTemplate(template, fd, pk, tableName)
            if a_query is not None:
                a_queries.append(a_query)
        # for lhs, rhs, values in fds:
        #     a_query = template
        #     a_query = a_query.replace('$LHS_NAME$', ("' " + lhs + "'"))
        #     a_query = a_query.replace('$LHS$', ('"' + lhs + '"'))
        #     a_query = a_query.replace('$RHS$', ('"' + rhs + '"'))
        #     a_query = a_query.replace('$PK$', ('"' + pk + '"'))
        #     a_query = a_query.replace('$TABLE$', tableName)
        #     for value in values:
        #         a_query = a_query.replace('$PRINT_FD$',"' " +value +" '", 1)
        #     if checkAQueryComplete(a_query):
        #         a_queries.append(a_query)
    return a_queries

def attributeStrategy(templates, ambiguities, pk, tableName, operators, mt, printConfig):
    a_queries = []
    for template, type in templates:
        if type != TYPE_ATTRIBUTE:
            continue
        for operator in operators:
            for a1, a2, label in ambiguities:
                if checkOperatorWithType(a1, operator) == False:
                    continue
                a_query = template
                a_query = a_query.replace('$PK$', ('"'+pk+'"'))
                a_query = a_query.replace('$TABLE$', tableName)
                a_query = a_query.replace('$OPERATOR$', operator)
                a_query = a_query.replace('$AMB_1$', '"'+a1+'"')
                a_query = a_query.replace('$AMB_2$', '"'+a2+'"')
                printf_string = printf(operator, label, printConfig).strip()
                a_query = a_query.replace('$PRINT_F$', "' "+printf_string+" '")
                if mt == 'contradicting':
                    a_query = a_query.replace('$MT_OPERATOR$', negOperator(operator))
                else:
                    a_query = a_query.replace('$MT_OPERATOR$', operator)
                if checkAQueryComplete(a_query):
                    a_queries.append(a_query)
    return a_queries

def attributeStrategy(template, ambiguities, pk,tableName, operator, mt, printConfig):
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
        a_query = a_query.replace('$PRINT_F$', "' "+printf_string+" '")
        if mt == 'contradicting':
            a_query = a_query.replace('$MT_OPERATOR$', negOperator(operator))
        else:
            a_query = a_query.replace('$MT_OPERATOR$', operator)
        if checkAQueryComplete(a_query):
            a_queries.append(a_query)
    return a_queries

def rowStrategy(templates, compositeKeys, tableName, attributes, operators, mt, printConfig):
    a_queries = []
    for template, type in templates:
        if type != TYPE_ROW:
            continue
        for operator in operators:
            listAttr = toListCompositeKeys(compositeKeys)
            attrList = [x for x in attributes if x not in listAttr]
            for ck in compositeKeys:
                subpk = ck[0]
                key = ck[1]
                for a in attrList:
                    if checkOperatorWithType(a, operator) == False:
                        continue
                    a_query = template
                    a_query = a_query.replace('$SUB_PK$', ('"'+subpk+'"'))
                    a_query = a_query.replace('$PK$', ('"' + key + '"'))
                    a_query = a_query.replace('$A1$', ('"' + a + '"'))
                    a_query = a_query.replace('$A1_NAME$', ("' " + a + "'"))
                    a_query = a_query.replace('$TABLE$', tableName)
                    printo_string = printo(operator, printConfig).strip()
                    a_query = a_query.replace('$PRINT_O$', " ' " + printo_string + " '")
                    if mt == 'contradicting':
                        a_query = a_query.replace('$MT_OPERATOR$', negOperator(operator))
                    else:
                        a_query = a_query.replace('$MT_OPERATOR$', operator)
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
    key = ck[1]
    for a in attrList:
        if checkOperatorWithType(a, operator) == False:
            continue
        a_query = template[0]
        a_query = a_query.replace('$SUB_PK$', ('"'+subpk+'"'))
        a_query = a_query.replace('$PK$', ('"' + key + '"'))
        a_query = a_query.replace('$A1$', ('"' + a[0] + '"'))
        a_query = a_query.replace('$A1_NAME$', ("' " + a[0] + "'"))
        a_query = a_query.replace('$TABLE$', tableName)
        printo_string = printo(operator, printConfig).strip()
        a_query = a_query.replace('$PRINT_O$', " ' " + printo_string + " '")
        if mt == 'contradicting':
            a_query = a_query.replace('$MT_OPERATOR$', negOperator(operator))
        else:
            a_query = a_query.replace('$MT_OPERATOR$', operator)
        if checkAQueryComplete(a_query):
            a_queries.append(a_query)
    return a_queries

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

def checkWithData(a_query, type, connection, a_queries_with_data, stored_results, template, fd):
    results = executeQueryBatch(a_query, connection)
    if len(results) > 0:
        a_queries_with_data.append(a_query)
        t_stored = (a_query, type, results, template, fd)
        stored_results.append(t_stored)

def find_a_queries(table, templates, mathcType, connection,
                   operatorPrintConfigRow, operatorPrintConfigAttribute, operators=["=", ">", "<"]):
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
                if (a_query is not None):
                    checkWithData(a_query, type, connection, a_queries_with_data, stored_results, template, fd)
        if (type == TYPE_ROW) and (len(compositeKeys) > 0):
            print("*** GENERATING A-QUERIES for ROWs")
            for ck in compositeKeys:
                for operator in operators:
                    a_queries = rowStrategyTemplate(template, ck, tableName, attributes, operator, mathcType, operatorPrintConfigRow)
                    for a_query in a_queries:
                        if (a_query is not None):
                            checkWithData(a_query, type, connection, a_queries_with_data, stored_results, template, None)
        if (type == TYPE_ATTRIBUTE) and (len(ambiguities) > 0):
            print("*** GENERATING A-QUERIES for ATTRIBUTEs")
            for operator in operators:
                a_queries = attributeStrategy(template, ambiguities, pk, tableName, operator, mathcType, operatorPrintConfigAttribute)
                for a_query in a_queries:
                    if (a_query is not None):
                        checkWithData(a_query, type, connection, a_queries_with_data, stored_results, template, None)
        if (type == TYPE_FULL) and (len(ambiguities) > 0) and (len(compositeKeys) > 0):
            print("*** GENERATING A-QUERIES for FULL")
            for ck in compositeKeys:
                subpk = ck[0]
                for operator in operator:
                    a_queries = attributeStrategy(template, ambiguities, subpk, tableName, operator, mathcType, operatorPrintConfigAttribute)
                    for a_query in a_queries:
                        if (a_query is not None):
                            checkWithData(a_query, type, connection, a_queries_with_data, stored_results, template)

    return a_queries_with_data, stored_results


def run(table, templates, matchType, connection):
    ambiguities = table[0]
    pk = table[1]
    fds = table[2]
    compositeKeys = table[3]
    tableName = table[4]
    attributes = table[5]
    a_queries_with_data = []
    if len(fds) > 0:
        print("***********************")
        print("FDS sentences and data")
        print("***********************")
        a_queries, to_totto_list = fdStrategy(templates,fds, pk, tableName)
        for aq in a_queries:
            results = executeQueryBatch(aq, connection)
            if len(results) > 0:
                a_queries_with_data.append(aq)
        for l in to_totto_list:
            count = 0
            for sentence, data in l:
                distinctSentences.add(sentence)
                #print("SENTENCE: ", sentence)
                #for row in data:
                #    print("ROW:", row)
                #print("**********************")
                example = to_totto_input(tableName, tableName, data, sentence)
                count += 1
                if (maxSentencesPerType != -1) and count > maxSentencesPerType:
                    break
                dataset.append(example)
    if len(compositeKeys) > 0:
        ## implement for CK
        a_queries = rowStrategy(templates, table, compositeKeys, tableName,attributes,operators, matchType, ['has', 'has more than', 'has less than'])
        to_tottos = []
        for aq in a_queries:
            results = executeQueryBatch(aq, connection)
            if len(results) > 0:
                columnsQuery = getColumnsName(aq, connection)
                a_queries_with_data.append(aq)
                to_totto = to_totto_row(results, columnsQuery)
                to_tottos.append(to_totto)
        print("***********************")
        print("ROW sentences and data")
        print("***********************")
        for to_totto in to_tottos:
            count = 0
            for sentence, data in to_totto:
                distinctSentences.add(sentence)
                #print("SENTENCE: ", sentence)
                #for row in data:
                #    print("ROW:", row)
                #print("**********************")
                example = to_totto_input(tableName, tableName, data, sentence)
                count += 1
                if (maxSentencesPerType != -1) and count > maxSentencesPerType:
                    break
                dataset.append(example)
    if len(ambiguities) > 0:
        a_queries = attributeStrategy(templates, table, ambiguities, pk, tableName, operators, matchType, [('has the same', 'as'), ('has higher', 'than'), ('has lower', 'than')])
        to_tottos = []
        for aq in a_queries:
            results = executeQueryBatch(aq, connection)
            if len(results) > 0:
                columnsQuery = getColumnsName(aq, connection)
                a_queries_with_data.append(aq)
                to_totto = to_totto_attribute(results,columnsQuery)
                to_tottos.append(to_totto)
        print("*****************************")
        print("ATTRIBUTE sentences and data")
        print("****************************")
        for to_totto in to_tottos:
            count = 0
            for sentence, data in to_totto:
                distinctSentences.add(sentence)
                #print("SENTENCE: ", sentence)
                #for row in data:
                #    print("ROW:", row)
                #print("**********************")
                example = to_totto_input(tableName, tableName, data, sentence)
                count += 1
                if (maxSentencesPerType != -1) and count > maxSentencesPerType:
                    break
                dataset.append(example)
