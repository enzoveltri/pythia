from DBUtils import executeQueryBatch, getDBConnection, getColumnsName
from DatasetProfiling import loadTable
from Pythia import find_a_queries
from Constants import TYPE_FULL,TYPE_ROW,TYPE_ATTRIBUTE,TYPE_FD
import random
import time

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
    table = "<table> "
    columns = selectedData[0]
    for row in selectedData[1:]:
        for pos in range(0, len(columns)):
            cellRow = "<cell> " + str(row[pos]) + " <col_header> " + str(columns[pos]) +" </col_header> </cell> "
            table += cellRow
    table += "</table>"
    return table

def to_totto_input(title, section, selectedData, sentence):
    totto_x = ""
    totto_x += "<page_title> " + title + " </page_title> "
    totto_x += "<section_title> " + section + " </section_title> "
    totto_x += to_table(selectedData)
    return (totto_x, sentence)

def to_totto_full(results, cols):
    to_totto = []
    for row in results:
        sentence = row[0]
        colList = []
        row1List = []
        row2List = []
        for i in range(1, len(cols), 2):
            colList.append(cols[i])
            row1List.append(row[i])
            row2List.append(row[i+1])
        data = [tuple(colList), tuple(row1List), tuple(row2List)]
        to_totto.append((sentence, data))
    return to_totto

def to_totto_row(results, cols):
    to_totto = []
    for row in results:
        sentence = row[0]
        t0 = (cols[1], cols[3], cols[5])
        t1 = (row[1], row[3], row[5])
        t2 = (row[2], row[4], row[6])
        data = [t0, t1, t2]
        to_totto.append((sentence, data))
    return to_totto

def to_totto_attribute(results, cols):
    to_totto = []
    for row in results:
        sentence = row[0]
        t0 = (cols[1], cols[3], cols[5])
        t1 = (row[1], row[3], row[5])
        t2 = (row[2], row[4], row[6])
        data = [t0, t1, t2]
        to_totto.append((sentence, data))
    return to_totto

def to_totto_fd(results, fdTemplateQuery, tableName , pk, connection, fd):
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
## TODO: introduce a grammar otherwise define rules: comma (,) is preceded by a space and followed by a space, atoms are all space devided
attributeTemplate = "SELECT CONCAT(b1.$PK$, $PRINT_F$, b2.$PK$),b1.$PK$, b2.$PK$,b1.$AMB_1$,b2.$AMB_1$, b1.$AMB_2$, b2.$AMB_2$ FROM $TABLE$ b1, $TABLE$ b2 WHERE b1.$PK$ <> b2.$PK$ AND b1.$AMB_1$ $OPERATOR$ b2.$AMB_1$ AND b1.$AMB_2$ $MT_OPERATOR$ b2.$AMB_2$"
rowTemplate = "SELECT CONCAT( b1.$SUB_PK$ , $PRINT_O$, b2.$A1$, $A1_NAME$ ) , b1.$SUB_PK$ , b2.$SUB_PK$ , b1.$A1$ , b2.$A1$ , b1.$PK$ , b2.$PK$ FROM $TABLE$ b1 , $TABLE$ b2 WHERE b1.$SUB_PK$ = b2.$SUB_PK$ AND b1.$A1$ $MT_OPERATOR$ b2.$A1$"
fdTemplate = "SELECT CONCAT($LHS_NAME$ , $PRINT_FD$, b1.$RHS$ , $PRINT_FD$, Count(b1.$PK$), $PRINT_FD$),b1.$LHS$,b1.$RHS$  FROM $TABLE$ b1 GROUP BY b1.$RHS$, b1.$LHS$ HAVING COUNT(b1.$LHS$) > 1"
fdTemplateQuery = "SELECT b1.$LHS$,b1.$RHS$, b1.$PK$ FROM $TABLE$ b1 WHERE b1.$RHS$ = $VALUE$"
fdTemplate2 = "SELECT CONCAT($LHS_NAME$, $PRINT_FD$, b1.$RHS$ , $PRINT_FD$, Count(b1.$PK$), $PRINT_FD$),b1.$LHS$,b1.$RHS$ FROM $TABLE$ b1 WHERE b1.$RHS$ in (select $RHS$ from  (select $RHS$, $LHS$ from $TABLE$ group by $RHS$, $LHS$) as Nested group by $RHS$ having count(*) > 1) GROUP BY b1.$RHS$, b1.$LHS$"
fullTemplate = "SELECT CONCAT( b1.$SUB_PK$ , $PRINT_F$ , b2.$SUB_PK$ ) , b1.$SUB_PK$ , b2.$SUB_PK$ , b1.$PK$ , b2.$PK$ , b1.$AMB_1$ , b2.$AMB_1$ , b1.$AMB_2$ , b2.$AMB_2$ FROM $TABLE$ b1 , $TABLE$ b2 WHERE b1.$SUB_PK$ = b2.$SUB_PK$ AND b1.$AMB_1$ $OPERATOR$ b2.$AMB_1$ AND b1.$AMB_2$ $MT_OPERATOR$ b2.$AMB_2$"



templates = [(attributeTemplate, TYPE_ATTRIBUTE), (rowTemplate,TYPE_ROW), (fdTemplate2,TYPE_FD), (fullTemplate, TYPE_FULL)]

#templates = [(attributeTemplate, TYPE_ATTRIBUTE)]
#templates = [(rowTemplate,TYPE_ROW)]
#templates = [(fdTemplate2,TYPE_FD)]
#templates = [(fullTemplate, TYPE_FULL)]


if __name__ == '__main__':
    saveFile = False
    limitResults = -1 #-1 for unlimit results
    sample = False
    sampleSize = 40000
    connection = getDBConnection(user_uenc, pw_uenc, host, port, dbname)
    ######################
    ### train
    ######################
    #table = loadTable('iris')
    #table = loadTable('basket_full')
    #table = loadTable('soccer')
    ######################
    ### test
    ######################
    #table = loadTable('basket_acronyms')
    #table = loadTable('abalone')
    #table = loadTable('abalone_short')
    #table = loadTable('adult')
    #table = loadTable('adult_short')
    #table = loadTable('mushroom')
    table = loadTable('mushroom_short')
    matchType = 'contradicting'
    #matchType = 'uniform_false'
    #matchType = 'uniform_true'
    operatorsPrintConfigAttribute = [('has the same', 'as'), ('has higher', 'than'), ('has lower', 'than'), ('has different', 'as')]
    operatorsPrintConfigRow = ['has', 'has more than', 'has less than', 'has not']
    start_time = time.time()
    a_queries, a_queries_with_data = find_a_queries(table, templates, matchType, connection, operatorsPrintConfigRow,
                                                    operatorsPrintConfigAttribute, operators=["=", ">", "<"],
                                                    executeQuery=True, limitQueryResults=limitResults)
    end_time = time.time()
    #for aq in a_queries:
    #    print(aq)
    print("Total A-Queries Generated:", len(a_queries))
    print("Differents A-Queries: ", len(set(a_queries)))
    print("Time (s): ", (end_time-start_time))
    to_totto_list = []
    tName = table[4]
    type = ""
    for a_query, type, results, template, fd in a_queries_with_data:
        if (type == TYPE_FD):
            pk = table[1]
            to_totto = to_totto_fd(results, fdTemplateQuery, tName, pk, connection, fd)
            to_totto_list.extend(to_totto)
        if (type == TYPE_ROW):
            columnsQuery = getColumnsName(a_query, connection)
            to_totto = to_totto_row(results, columnsQuery)
            to_totto_list.extend(to_totto)
        if (type == TYPE_ATTRIBUTE):
            columnsQuery = getColumnsName(a_query, connection)
            to_totto = to_totto_attribute(results, columnsQuery)
            to_totto_list.extend(to_totto)
        if (type == TYPE_FULL):
            columnsQuery = getColumnsName(a_query, connection)
            to_totto = to_totto_full(results, columnsQuery)
            to_totto_list.extend(to_totto)
    totto_examples = set()
    for sentence, data in to_totto_list:
        example = to_totto_input(tName, tName, data, sentence)
        totto_examples.add(example)
    lines = []
    if sample:
        totto_examples = list(totto_examples)
        random.shuffle(totto_examples)
        totto_examples = totto_examples[0:sampleSize]
    for x, y in totto_examples:
        #print(x, y)
        line = x + "\t" + y +"\n"
        lines.append(line)
    print("# Sentences: ", len(totto_examples))
    if saveFile:
        f = open("../../data/pythia_ambtotto-vldb/" + tName + "_" + matchType + "_" + type + "_train_40000.tsv", "w")
        #f = open("../../data/pythia_ambtotto-vldb/" + tName + "_" + matchType + "_" + type + "_test.tsv", "w")
        try:
            f.writelines(lines)
        except:
            print("Exception")
        f.close()



