from src.pythia.Constants import TYPE_ATTRIBUTE, TYPE_ROW, TYPE_FULL, TYPE_FD, TYPE_FUNC

attributeTemplate = "SELECT CONCAT( b1.$PK$, $PRINT_F$, b2.$PK$ ), b1.$PK$, b2.$PK$, b1.$AMB_1$, b2.$AMB_1$, b1.$AMB_2$, b2.$AMB_2$ " \
                    "FROM $TABLE$ b1, $TABLE$ b2 " \
                    "WHERE b1.$PK$ <> b2.$PK$ AND b1.$AMB_1$ $OPERATOR$ b2.$AMB_1$ AND b1.$AMB_2$ $MT_OPERATOR$ b2.$AMB_2$"

rowTemplate = "SELECT CONCAT( b1.$SUB_PK$ , $PRINT_O$, b2.$A1$, $A1_NAME$ ) , b1.$SUB_PK$ , b2.$SUB_PK$ , b1.$A1$ , b2.$A1$ , b1.$PK$ , b2.$PK$ " \
              "FROM $TABLE$ b1 , $TABLE$ b2 " \
              "WHERE b1.$SUB_PK$ = b2.$SUB_PK$ AND b1.$A1$ $MT_OPERATOR$ b2.$A1$"

fullTemplate = "SELECT CONCAT( b1.$SUB_PK$ , $PRINT_F$ , b3.$SUB_PK$ ) , b1.$SUB_PK$ , b2.$SUB_PK$ , b1.$PK$ , b2.$PK$ , b1.$AMB_1$ , b2.$AMB_1$ , b1.$AMB_2$ , b2.$AMB_2$ " \
               "FROM $TABLE$ b1 , $TABLE$ b2 , $TABLE$ b3 " \
               "WHERE b1.$SUB_PK$ = b2.$SUB_PK$ AND b1.$AMB_1$ $OPERATOR$ b2.$AMB_1$ AND b1.$AMB_2$ $MT_OPERATOR$ b2.$AMB_2$ AND b1.$AMB_1$ $OPERATOR$ b3.$AMB_1$ AND b1.$AMB_2$ $MT_OPERATOR$ b3.$AMB_2$ "

fdTemplate = "SELECT CONCAT( $LHS_NAME$, $PRINT_FD$, b1.$RHS$ , $PRINT_FD$, Count(b1.$PK$), $PRINT_FD$ ), b1.$LHS$, b1.$RHS$ " \
              "FROM $TABLE$ b1 " \
              "WHERE b1.$RHS$ in (select $RHS$ from  (select $RHS$, $LHS$ from $TABLE$ group by $RHS$, $LHS$) as Nested group by $RHS$ having count(*) > 1) GROUP BY b1.$RHS$, b1.$LHS$"

functionTemplate = "SELECT CONCAT( b1.$SUB_PK$, $PRINT_O$, b1.$A1_NAME$ " \
                   "FROM $TABLE$ b1, $TABLE$ b2 " \
                   "WHERE b1.$SUB_PK$ = b2.$SUB_PK$ AND b1.$A1_NAME$ = (SELECT $FUNC$( $A1_NAME$ ) FROM $TABLE$) AND b2.$A1_NAME$ $MT_FUNC$ (SELECT $FUNC$( $A1_NAME$ ) FROM $TABLE$)"

## MORE TEMPLATES HERE ##

class TemplateFactory:
    def __init__(self):
        self._initTemplates()

    def _initTemplates(self):
        self.templates = [
            (attributeTemplate, TYPE_ATTRIBUTE),
            (rowTemplate, TYPE_ROW),
            (fullTemplate, TYPE_FULL),
            (fdTemplate, TYPE_FD),
            (functionTemplate, TYPE_FUNC),
            ## MORE DEFINITION HERE ##
        ]

    def getTemplates(self):
        return self.templates

    def getTemplatesByType(self, type):
        templatesByType = []
        for template, templateType in self.templates:
            if (templateType == type):
                templatesByType.append((template, templateType))
        return templatesByType