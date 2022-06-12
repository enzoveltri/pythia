import copy
import json

import pandas as pd
from pandas.core.dtypes.common import is_numeric_dtype

from src.pythia.Attribute import Attribute
from src.pythia.Constants import STRATEGY_SCHEMA, STRATEGY_SCHEMA_WITH_DATA_SAMPLE, STRATEGY_PAIRWISE_COMBINATION, \
    STRATEGY_PAIRWISE_PERMUTATION, CATEGORICAL, NUMERICAL, INDEX
from src.pythia.Profiling import getCompositeKeys, getFDs, getAmbiguousAttribute
from src.pythia.StringUtils import normalizeString
from itertools import combinations, permutations

class Dataset:

    def __init__(self, datasetName, name):
        self.name = name
        self.datasetName = datasetName
        self.pk = None
        self.fds = []
        self.compositeKeys = []
        self.ambiguousAttribute = []

    def initDataframe(self, file):
        self.dataframe = self._loadDataFrame(file)
        self.attributes = self._getAttributesFromDF(self.dataframe)
        self.nameToAttribute = self._dictNameToAttribute()

    def findCompositeKeys(self, maxSizeKeys, ckMinimal):
        return self.findCompositeKeys(self, self.dataframe, maxSizeKeys, ckMinimal)

    def findCompositeKeys(self, dataframe, maxSizeKeys, ckMinimal):
        compositeKeysName = getCompositeKeys(dataframe, maxSizeKeys)
        self.compositeKeys = []
        for ck in compositeKeysName:
            ckAttr = []
            for attrName in ck:
                if attrName == self.pk.name:
                    break
                attr = self.nameToAttribute[attrName]
                ckAttr.append(attr)
            self.compositeKeys.append(ckAttr)
        if ckMinimal and len(self.compositeKeys) > 0:
            self.compositeKeys = [self.compositeKeys[0]]

    def findPK(self):
        rows = self.dataframe.shape[0]
        for column in self.dataframe.columns:
            uniqueValuesInColumn = len(self.dataframe[column].unique())
            if uniqueValuesInColumn == rows:
                self.pk = self.nameToAttribute[column]
                break
        if self.pk is None:
            #self.pk = INDEX
            #self.dataframe[self.pk.name] = self.dataframe.index
            _pk = INDEX
            self.dataframe.insert(0, _pk.name, range(rows))
            self.attributes = self._getAttributesFromDF(self.dataframe)
            self.nameToAttribute = self._dictNameToAttribute()
            self.pk = self.nameToAttribute[_pk.name]

        ## update also the dataframe
        self.dataframe.set_index(self.pk.name, inplace=True)

    def allCategorical(self, attributes):
        for attr in attributes:
            if attr.type != CATEGORICAL:
                return False
        return True

    def pruneCKsByType(self, useNumerical=False):
        if useNumerical == True:
            return
        compositeKeysCategorical = []
        for ck in self.compositeKeys:
            if self.allCategorical(ck):
                compositeKeysCategorical.append(ck)
        self.compositeKeys = compositeKeysCategorical

    def findFDs(self, df, dropFiles, tempFolder = "./taneFolder/"):
        fdsName = getFDs(df, self.attributes, dropFiles, tempFolder)
        self.fds = []
        for fd in fdsName:
            fdAttr = []
            for attrName in fd:
                #attrName = [x for x in self.attributes if x.normalizedName == attrName][0].name
                attr = self.nameToAttribute[attrName]
                fdAttr.append(attr)
            self.fds.append(fdAttr)

    def extendFDs(self, rowMeaning):
        extendedFDs = []
        for fd in self.fds:
            efd = (fd, rowMeaning)
            extendedFDs.append(efd)
        self.fds = extendedFDs

    def getCompositeKeys(self):
        return self.compositeKeys

    def getPk(self):
        return self.pk

    def getDatasetName(self):
        return self.datasetName

    def getAttributes(self):
        return self.attributes

    def getNormalizedAttributes(self):
        normalizedAttributes = []
        for attr in self.attributes:
            normalizedAttributes.append(attr.normalizedName)
        return normalizedAttributes

    def getNormalizedFDs(self):
        normalizedFDs = []
        for fd, label in self.fds:
            normFD = []
            for attr in fd:
                normFD.append(attr.normalizedName)
            normalizedFDs.append((normFD, label))
        return normalizedFDs

    def getNormalizedCompositeKeys(self):
        return self._normalizeList(self.compositeKeys)

    def getFDs(self):
        return self.fds

    def getDataFrame(self):
        return self.dataframe

    def getAmbiguousAttribute(self):
        return self.ambiguousAttribute

    def getAmbiguousAttributeNormalized(self):
        normalizedAttributes = []
        for attr1, attr2, label in self.ambiguousAttribute:
            normalizedAttributes.append((attr1.normalizedName, attr2.normalizedName, label))
        ##return self.ambiguousAttributeNormalized
        return normalizedAttributes

    def findAmbiguousAttributes(self, strategy, t5Engine, usePermutation=False):
        ## usePermutation = False to avoid to compare A-B and B-A
        self.ambiguousAttribute = []
        pairwiseAttributes = []
        if usePermutation:
            pairwiseAttributes = list(permutations(self.attributes, 2))
        else:
            pairwiseAttributes = list(combinations(self.attributes, 2))
        if (strategy == STRATEGY_SCHEMA):
            linearizedSchema = self._linearizeSchema()
            for attr1, attr2 in pairwiseAttributes:
                label = getAmbiguousAttribute(linearizedSchema, attr1, attr2, strategy, t5Engine)
                if label is not None:
                    self.ambiguousAttribute.append((attr1, attr2, label))
        if (strategy == STRATEGY_SCHEMA_WITH_DATA_SAMPLE):
            schemaWithData = self._schemaWithDataSample()  ## TODO code in colab task3
            for attr1, attr2 in pairwiseAttributes:
                label = getAmbiguousAttribute(schemaWithData, attr1, attr2, strategy, t5Engine)
                if label is not None:
                    self.ambiguousAttribute.append((attr1, attr2, label))

    def findNonAmbiguousAttributes(self):
        notAmbiguousAttr = []
        if (self.ambiguousAttribute is None): return None
        for attribute in self.attributes:
            toAdd = True
            for a1, a2, label in self.ambiguousAttribute:
                if attribute == a1 or attribute == a2:
                    toAdd = False
                    break
            if toAdd: notAmbiguousAttr.append(attribute)
        return notAmbiguousAttr

    def storeInDB(self, engine, if_exists='replace'):
        ## if_exists {'fail', 'replace', 'append'}
        dfDB = self.dataframe.copy()
        dictRename = dict()
        for name, attr in self.nameToAttribute.items():
            dictRename[name] = attr.normalizedName
        dfDB.rename(columns=dictRename, inplace=True)
        dfDB.to_sql(self.datasetName, engine, if_exists=if_exists, index_label=self.pk.normalizedName)
        dataframeRows = self.dataframe.shape[0]
        rowsInDB = engine.execute("SELECT count(*) from "+self.datasetName).fetchall()[0][0]
        return dataframeRows == rowsInDB

    def _dictNameToAttribute(self):
        mappings = dict()
        for attr in self.attributes:
            mappings[attr.name] = attr
        return mappings

    def _getAttributesFromDF(self, df):
        dtypes = df.infer_objects().dtypes
        attributes = []
        for attrName, attrType in dtypes.items():
            type = CATEGORICAL
            if is_numeric_dtype(attrType):
                type = NUMERICAL
            attribute = Attribute(attrName)
            attribute.setType(type)
            attributes.append(attribute)
        return attributes

    def _getAttributes(self, file):
        df = pd.read_csv(file)
        dtypes = df.infer_objects().dtypes
        attributes = []
        if self.pk == INDEX:
            attributes.append((INDEX[0], CATEGORICAL))
        for attrName, attrType in dtypes.items():
            type = CATEGORICAL
            if is_numeric_dtype(attrType):
                type = NUMERICAL
            attributes.append((attrName, type))
        return attributes

    def _normalizeList(self, listOfList):
        normalizedNames = []
        for list in listOfList:
            normalized = []
            for attr in list:
                normalized.append(attr.normalizedName)
            normalizedNames.append(normalized)
        return normalizedNames

    def _loadDataFrame(self, file):
        reader = pd.read_csv(file, sep=None, iterator=True, engine='python')
        inferred_sep = reader._engine.data.dialect.delimiter
        df = pd.read_csv(file, sep=inferred_sep)
        return df

    def _linearizeSchema(self):
        attributeNames = [attr.name for attr in self.attributes]
        return "|".join(attributeNames)

    def _schemaWithDataSample(self):
        ## TODO: implement from task 3
        return ""

    def toJSON(self):
        if hasattr(self, "dataframe"):
            delattr(self, "dataframe")
        return json.dumps(self, default=lambda o: o.__dict__, indent=4)

