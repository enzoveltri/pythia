def printf(operator, label, values):
    #['has the same', 'has higher', 'has lower']
    if operator == '=':
        valComparison = values[0]
        if len(valComparison) == 1:
            return valComparison[0] +" " + label + " "
        else:
            return valComparison[0] +" " + label + " " + valComparison[1] + " "
    if operator == '>':
        valComparison = values[1]
        if len(valComparison) == 1:
            return valComparison[0] +" " + label + " "
        else:
            return valComparison[0] +" " + label + " " + valComparison[1] + " "
    if operator == '<':
        valComparison = values[2]
        if len(valComparison) == 1:
            return valComparison[0] +" " + label + " "
        else:
            return valComparison[0] +" " + label + " " + valComparison[1] + " "
    if operator == '<>':
        valComparison = values[3]
        if len(valComparison) == 1:
            return valComparison[0] +" " + label + " "
        else:
            return valComparison[0] +" " + label + " " + valComparison[1] + " "
    print("*** ERROR in printf:", operator, label, values)

def printo(operator, values):
    if operator == '=':
        return values[0] + " "
    if operator == '>':
        return values[1] + " "
    if operator == '<':
        return values[2] + " "
    if operator == '<>':
        return values[3] + " "
    print("*** ERROR in printf:", operator, values)
