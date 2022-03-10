import pandas as pd
from pandas.core.dtypes.common import is_numeric_dtype

from src.pythia.Attribute import Attribute
from src.pythia.Constants import STRATEGY_SCHEMA, STRATEGY_SCHEMA_WITH_DATA_SAMPLE, STRATEGY_PAIRWISE_COMBINATION, STRATEGY_PAIRWISE_PERMUTATION
from src.pythia.DBUtils import getDBConnection, getEngine
from src.pythia.Profiling import getCompositeKeys, getFDs, getAmbiguousAttribute
from Constants import CATEGORICAL, NUMERICAL, INDEX
from src.pythia.StringUtils import normalizeString
from itertools import combinations, permutations

class Dataset:

    # def __init__(self, file, name, maxSizeKeys, ckMinimal, rowMeaning):
    #     self.datasetName= name
    #     self.file = file
    #     self.compositeKeys = getCompositeKeys(file, maxSizeKeys)
    #     if ckMinimal and len(self.compositeKeys) > 0:
    #         self.compositeKeys = [self.compositeKeys[0]]
    #     self.pk = self._getPk()
    #     self._updateCKs()
    #     self.attributes = self._getAttributes(file)
    #     self._pruneCKsByType()
    #     #self._pruneCKsByType(True) ## to keep numerical attr
    #     self.fds = self._getFDs(file)
    #     self.normalizedAttributes = self._normalize(self.attributes)
    #     self.normalizedCompositeKeys = self._normalizeNames(self.compositeKeys)
    #     self.normalizedFDs = self._normalizeNames(self.fds)
    #     self.normalizedFDs = self._extendFDs(self.normalizedFDs, rowMeaning)
    #     self.mappigsAttributes = self._getDictionaryAttributes()
    #     self.dataframe = self._loadDataFrame(file)

    def __init__(self, file, name):
        self.datasetName= name
        self.file = file
        self.dataframe = self._loadDataFrame(file)
        self.pk = None
        self.attributes = self._getAttributesFromDF(self.dataframe)
        self.nameToAttribute = self._dictNameToAttribute()
        self.fds = []
        self.compositeKeys = []
        self.ambiguousAttribute = []

    def findCompositeKeys(self, maxSizeKeys, ckMinimal):
        #compositeKeysName = getCompositeKeys(self.file, maxSizeKeys)
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
            self.pk = INDEX
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

    def getMappingsAttributes(self):
        return self.mappigsAttributes

    def getAmbiguousAttribute(self):
        return self.ambiguousAttribute

    def getAmbiguousAttributeNormalized(self):
        normalizedAttributes = []
        for attr1, attr2, label in self.ambiguousAttribute:
            normalizedAttributes.append((attr1.normalizedName, attr2.normalizedName, label))
        ##return self.ambiguousAttributeNormalized
        return normalizedAttributes

    def findAmbiguousAttributes(self, strategy, t5Engine):
        self.ambiguousAttribute = []
        pairwiseAttributes = []
        #pairwiseAttributes = list(combinations(self.attributes, 2))
        pairwiseAttributes = list(permutations(self.attributes, 2))
        if (strategy == STRATEGY_SCHEMA):
            linearizedSchema = self._linearizeSchema()
            for attr1, attr2 in pairwiseAttributes:
                label = getAmbiguousAttribute(linearizedSchema, attr1, attr2, strategy, t5Engine)
                if label is not None:
                    self.ambiguousAttribute.append((attr1, attr2, label))
        if (strategy == STRATEGY_SCHEMA_WITH_DATA_SAMPLE):
            schemaWithData = self._schemaWithDataSample() ## TODO code in colab task3
            for attr1, attr2 in pairwiseAttributes:
                label = getAmbiguousAttribute(schemaWithData, attr1, attr2, strategy, t5Engine)
                if label is not None:
                    self.ambiguousAttribute.append((attr1, attr2, label))
        # self.ambiguousAttributeNormalized = []
        # for attr1, attr2, label in self.ambiguousAttribute:
        #     normalizedName1 = self.mappigsAttributes[attr1[0]]
        #     normalizedName2 = self.mappigsAttributes[attr2[0]]
        #     attr1Norm = (normalizedName1, attr1[1])
        #     attr2Norm = (normalizedName2, attr2[1])
        #     self.ambiguousAttributeNormalized.append((attr1Norm, attr2Norm, label))

    def storeInDB(self, user_uenc, pw_uenc, host, port, dbname, if_exists='replace'):
        ## if_exists {'fail', 'replace', 'append'}
        #connection = getDBConnection(user_uenc, pw_uenc, host, port, dbname)
        engine = getEngine(user_uenc, pw_uenc, host, port, dbname)
        self.dataframe.to_sql(self.datasetName, engine, if_exists=if_exists)
        dataframeRows = self.dataframe.shape[0]
        rowsInDB = engine.execute("SELECT count(*) from "+self.datasetName).fetchall()[0][0]
        return dataframeRows == rowsInDB

    def _dictNameToAttribute(self):
        mappings = dict()
        for attr in self.attributes:
            mappings[attr.name] = attr
        return mappings

    def _getPk(self):
        for ck in self.compositeKeys:
            if (len(ck) == 1):
                return ck
        return INDEX

    def _updateCKs(self):
        if (self.pk in self.compositeKeys):
            self.compositeKeys.remove(self.pk)

    # def _getAttributesFromDF(self, df):
    #     dtypes = df.infer_objects().dtypes
    #     attributes = []
    #     if self.pk == INDEX:
    #         attributes.append((INDEX[0], CATEGORICAL))
    #     for attrName, attrType in dtypes.items():
    #         type = CATEGORICAL
    #         if is_numeric_dtype(attrType):
    #             type = NUMERICAL
    #         attributes.append((attrName, type))
    #     return attributes

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

    def _normalize(self, attrList):
        normalized = []
        for attrName, type in attrList:
            attrName = normalizeString(attrName, "_")
            newAttribute = (attrName, type)
            normalized.append(newAttribute)
        return normalized

    def _normalizeNames(self, listOfList):
        listOfListNormalized = []
        if listOfList is None:
            return listOfListNormalized
        for list in listOfList:
            listNormalized = []
            for name in list:
                listNormalized.append(normalizeString(name, "_"))
            listOfListNormalized.append(listNormalized)
        return listOfListNormalized

    def _getFDs(self, file):
        fds = getFDs(file, self.attributes)
        return fds

    def _normalizeList(self, listOfList):
        normalizedNames = []
        for list in listOfList:
            normalized = []
            for attr in list:
                normalized.append(attr.normalizedName)
            normalizedNames.append(normalized)
        return normalizedNames

    # def _loadDataFrame(self, file):
    #     df = pd.read_csv(file)
    #     normalizedColumns = []
    #     for colName, type in self.normalizedAttributes:
    #         if colName != INDEX[0]:
    #             normalizedColumns.append(colName)
    #     df.columns = normalizedColumns
    #     return df

    def _loadDataFrame(self, file):
        df = pd.read_csv(file)
        return df

    def _getDictionaryAttributes(self):
        mappings = {}
        for attr, attrNorm in zip(self.attributes, self.normalizedAttributes):
            mappings[attr[0]] = attrNorm[0]
        return mappings

    def _pruneCKsByType(self, useNumerical=False):
        if useNumerical == True:
            return
        attrType = {}
        for attr, type in self.attributes:
            attrType[attr] = type
        compositeKeysCategorical = []
        for ck in self.compositeKeys:
            if (self._isCategorical(ck, attrType)):
                compositeKeysCategorical.append(ck)
        self.compositeKeys = compositeKeysCategorical

    def _isCategorical(self, attrList, attrType):
        for attr in attrList:
            if (attrType[attr] != CATEGORICAL):
                return False
        return True

    def _extendFDs(self, FDs, rowMeaning):
        extendedFDs = []
        for fd in FDs:
            efd = (fd, rowMeaning)
            extendedFDs.append(efd)
        return extendedFDs

    # def _linearizeSchema(self):
    #     attributeNames = [attr[0] for attr in self.attributes]
    #     return "|".join(attributeNames)

    def _linearizeSchema(self):
        attributeNames = [attr.name for attr in self.attributes]
        return "|".join(attributeNames)

    def _schemaWithDataSample(self):
        ## TODO: implement from task 3
        return ""
