import pandas as pd

def updateDFColumns(df, attributes):
    colNames = []
    for attr in attributes:
        colNames.append(attr.name)
    df.set_axis(colNames, axis=1, inplace=True)
    return df