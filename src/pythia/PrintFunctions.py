def printf(operator, label, values):
    #['has the same', 'has higher', 'has lower']
    labelString = "";
    if(label != None):
        labelString = " " + label + " "
    if operator == '=':
        valComparison = values[0]
        if isinstance(valComparison, str):
            return valComparison + labelString
        else:
            return valComparison[0] + labelString + valComparison[1] + " "
    if operator == '>':
        valComparison = values[1]
        if isinstance(valComparison, str):
            return valComparison + labelString
        else:
            return valComparison[0] + labelString + valComparison[1] + " "
    if operator == '<':
        valComparison = values[2]
        if isinstance(valComparison, str):
            return valComparison + labelString
        else:
            return valComparison[0] + labelString + valComparison[1] + " "
    if operator == '<>':
        valComparison = values[3]
        if isinstance(valComparison, str):
            return valComparison + labelString
        else:
            return valComparison[0] + labelString + valComparison[1] + " "
    print("*** ERROR in printf:", operator, label, values)

def printo(operator, values):
    if operator == '=':
        return str(values[0]) + " "
    if operator == '>':
        return str(values[1]) + " "
    if operator == '<':
        return str(values[2]) + " "
    if operator == '<>':
        return str(values[3]) + " "
    print("*** ERROR in printf:", operator, values)

def prinfFunction(function, values):
    if function == "max":
        return values[0] + " "
    if function == "min":
        return values[1] + " "
    if function == "<":
        return values[2] + " "
    if function == ">":
        return values[3] + " "
    print("*** ERROR in prinfFunction:", function, values)
