from DBUtils import executeQueryBatch, getDBConnection, getColumnsName
from DatasetProfiling import loadTable
from Pythia import find_a_queries
from Constants import TYPE_FULL,TYPE_ROW,TYPE_ATTRIBUTE,TYPE_FD
import random

### DB CONNECTION POSTGRESQL
dialect = "postgresql"
user_uenc = "postgres"
pw_uenc = "postgres"
host = "localhost"
port = "5432"
dbname = "ambiguities"

##############################
## TO_TOTTO INPUT
#############################
def to_table(selectedData):
    rowSentences = []
    columns = selectedData[0]
    for row in selectedData[1:]:
        text = ""
        for pos in range(0, len(columns)):
            text += str(columns[pos])+ " is " + str(row[pos])
            if pos + 1 < len(columns):
                text += " ; "
            else:
                text += "."
        rowSentences.append(text)
    return rowSentences

def to_feverous_input(selectedData, sentence):
    sequence = [sentence]
    sequence += to_table(selectedData)
    return ' </s> '.join(sequence)

def to_data_row(results, cols):
    to_totto = []
    for row in results:
        sentence = row[0]
        t0 = (cols[1], cols[3], cols[5])
        t1 = (row[1], row[3], row[5])
        t2 = (row[2], row[4], row[6])
        data = [t0, t1, t2]
        to_totto.append((sentence, data))
    return to_totto

def to_data_attribute(results, cols):
    to_totto = []
    for row in results:
        sentence = row[0]
        t0 = (cols[1], cols[3], cols[5])
        t1 = (row[1], row[3], row[5])
        t2 = (row[2], row[4], row[6])
        data = [t0, t1, t2]
        to_totto.append((sentence, data))
    return to_totto

def to_data_fd(results, fdTemplateQuery, tableName , pk, connection, fd):
    to_totto = []
    lhsAttr = fd[0]
    rhsAttr = fd[1]
    for row in results:
        sentence = row[0]
        lhs = row[1]
        rhs = row[2]
        #print(sentence, lhs, rhs)
        queryData = fdTemplateQuery
        queryData = queryData.replace('$LHS$', ('"' + lhsAttr + '"'))
        queryData = queryData.replace('$RHS$', ('"' + rhsAttr + '"'))
        queryData = queryData.replace('$TABLE$', tableName)
        queryData = queryData.replace('$VALUE$', "'"+rhs+"'")
        queryData = queryData.replace('$PK$', ('"' + pk + '"'))
        dataTable = executeQueryBatch(queryData, connection)
        cQ = getColumnsName(queryData, connection)
        tupleColumn = tuple(cQ)
        dataTable.insert(0, tupleColumn)
        to_totto.append((sentence, dataTable))
    return to_totto

#######################################
attributeTemplate = "SELECT CONCAT(b1.$PK$, $PRINT_F$, b2.$PK$),b1.$PK$, b2.$PK$,b1.$AMB_1$,b2.$AMB_1$, b1.$AMB_2$, b2.$AMB_2$ FROM $TABLE$ b1, $TABLE$ b2 WHERE b1.$PK$ <> b2.$PK$ AND b1.$AMB_1$ $OPERATOR$ b2.$AMB_1$ AND b1.$AMB_2$ $MT_OPERATOR$ b2.$AMB_2$"
rowTemplate = "SELECT CONCAT(b1.$SUB_PK$, $PRINT_O$, b2.$A1$, $A1_NAME$),b1.$SUB_PK$,b2.$SUB_PK$,b1.$A1$,b2.$A1$,b1.$PK$,b2.$PK$ FROM $TABLE$ b1, $TABLE$ b2 WHERE b1.$SUB_PK$ = b2.$SUB_PK$ AND b1.$A1$ $MT_OPERATOR$ b2.$A1$"
fdTemplate = "SELECT CONCAT($LHS_NAME$ , $PRINT_FD$, b1.$RHS$ , $PRINT_FD$, Count(b1.$PK$), $PRINT_FD$),b1.$LHS$,b1.$RHS$  FROM $TABLE$ b1 GROUP BY b1.$RHS$, b1.$LHS$ HAVING COUNT(b1.$LHS$) > 1"
fdTemplateQuery = "SELECT b1.$LHS$,b1.$RHS$, b1.$PK$ FROM $TABLE$ b1 WHERE b1.$RHS$ = $VALUE$"
fdTemplate2 = "SELECT CONCAT($LHS_NAME$, $PRINT_FD$, b1.$RHS$ , $PRINT_FD$, Count(b1.$PK$), $PRINT_FD$),b1.$LHS$,b1.$RHS$ FROM $TABLE$ b1 WHERE b1.$RHS$ in (select $RHS$ from  (select $RHS$, $LHS$ from $TABLE$ group by $RHS$, $LHS$) as Nested group by $RHS$ having count(*) > 1) GROUP BY b1.$RHS$, b1.$LHS$"

templates = [(attributeTemplate, TYPE_ATTRIBUTE), (rowTemplate,TYPE_ROW), (fdTemplate2,TYPE_FD)]
#templates = [(attributeTemplate, TYPE_ATTRIBUTE)]


if __name__ == '__main__':
    saveFile = True
    sample = True
    sampleSize = 1000
    limitResults = 10 ## - 1 for no limit
    connection = getDBConnection(user_uenc, pw_uenc, host, port, dbname)
    #table = loadTable('iris')
    #table = loadTable('basket_full')
    #table = loadTable('soccer')
    #table = loadTable('basket_acronyms')
    #table = loadTable('abalone')
    #table = loadTable('abalone_short')
    #table = loadTable('adult_short')
    #table = loadTable('adult')
    #table = loadTable('mushroom_short')
    table = loadTable('mushroom')
    #matchType = 'contradicting'
    #matchType = 'uniform_false'
    matchType = 'uniform_true'
    operatorsPrintConfigAttribute = [('has the same', 'as'), ('has higher', 'than'), ('has lower', 'than'), ('has different', 'as')]
    operatorsPrintConfigRow = ['has', 'has more than', 'has less than', 'has not']
    a_queries, a_queries_with_data = find_a_queries(table, templates, matchType, connection, operatorsPrintConfigRow,
                                                    operatorsPrintConfigAttribute, operators=["=", ">", "<"],
                                                    executeQuery=True, limitQueryResults=limitResults)
    #for aq in a_queries:
    #    print(aq)
    print("Total A-Queries Generated:", len(a_queries))
    print("Distinct Sentences: ", len(set(a_queries)))

    to_feverous_list = []
    tName = table[4]
    for a_query, type, results, template, fd in a_queries_with_data:
        if (type == TYPE_FD):
            pk = table[1]
            to_feverous = to_data_fd(results, fdTemplateQuery, tName, pk, connection, fd)
            to_feverous_list.extend(to_feverous)
        if (type == TYPE_ROW):
            columnsQuery = getColumnsName(a_query, connection)
            to_feverous = to_data_row(results, columnsQuery)
            to_feverous_list.extend(to_feverous)
        if (type == TYPE_ATTRIBUTE):
            columnsQuery = getColumnsName(a_query, connection)
            to_feverous = to_data_attribute(results, columnsQuery)
            to_feverous_list.extend(to_feverous)
    feverous_examples = set()
    for sentence, data in to_feverous_list:
        example = to_feverous_input( data, sentence)
        feverous_examples.add(example)
    if sample:
        feverous_examples = list(feverous_examples)
        random.shuffle(feverous_examples)
        feverous_examples = feverous_examples[0:sampleSize]
    lines = []
    for x in feverous_examples:
        #print(x, matchType)
        #print(x)
        line = x + "\t" + matchType +"\n"
        lines.append(line)
    if saveFile:
        #f = open("../../data/generated/" + tName + "_" + matchType + "_train.tsv", "w")
        f = open("../../data/generated/" + tName + "_" + matchType + "_test.tsv", "w")
        print("Save to: ", f.name)
        try:
            f.writelines(lines)
        except:
            print("Exception")
        f.close()



