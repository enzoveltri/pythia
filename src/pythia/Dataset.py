import pandas as pd
from pandas.core.dtypes.common import is_numeric_dtype

from src.pythia.Constants import STRATEGY_SCHEMA, STRATEGY_SCHEMA_WITH_DATA_SAMPLE, STRATEGY_PAIRWISE_COMBINATION, STRATEGY_PAIRWISE_PERMUTATION
from src.pythia.DBUtils import getDBConnection, getEngine
from src.pythia.Profiling import getCompositeKeys, getFDs, getAmbiguousAttribute
from Constants import CATEGORICAL, NUMERICAL, INDEX
from src.pythia.StringUtils import normalizeString
from itertools import combinations, permutations

class Dataset:

    def __init__(self, file, name, maxSizeKeys, ckMinimal, rowMeaning):
        self.datasetName= name
        self.file = file
        self.compositeKeys = getCompositeKeys(file, maxSizeKeys)
        if ckMinimal and len(self.compositeKeys) > 0:
            self.compositeKeys = [self.compositeKeys[0]]
        self.pk = self._getPk()
        self._updateCKs()
        self.attributes = self._getAttributes(file)
        self._pruneCKsByType()
        #self._pruneCKsByType(True) ## to keep numerical attr
        self.fds = self._getFDs(file)
        self.normalizedAttributes = self._normalize(self.attributes)
        self.normalizedCompositeKeys = self._normalizeNames(self.compositeKeys)
        self.normalizedFDs = self._normalizeNames(self.fds)
        self.normalizedFDs = self._extendFDs(self.normalizedFDs, rowMeaning)
        self.mappigsAttributes = self._getDictionaryAttributes()
        self.dataframe = self._loadDataFrame(file)

    def getCompositeKeys(self):
        return self.compositeKeys

    def getPk(self):
        return self.pk

    def getDatasetName(self):
        return self.datasetName

    def getAttributes(self):
        return self.attributes

    def getNormalizedAttributes(self):
        return self.normalizedAttributes

    def getNormalizedFDs(self):
        return self.normalizedFDs

    def getNormalizedCompositeKeys(self):
        return self.normalizedCompositeKeys

    def getFDs(self):
        return self.fds

    def getDataFrame(self):
        return self.dataframe

    def getMappingsAttributes(self):
        return self.mappigsAttributes

    def getAmbiguousAttribute(self):
        return self.ambiguousAttribute

    def getAmbiguousAttributeNormalized(self):
        return self.ambiguousAttributeNormalized

    def findAmbiguousAttributes(self, strategy):
        self.ambiguousAttribute = []
        pairwiseAttributes = []
        #pairwiseAttributes = list(combinations(self.attributes, 2))
        pairwiseAttributes = list(permutations(self.attributes, 2))
        if (strategy == STRATEGY_SCHEMA):
            linearizedSchema = self._linearizeSchema()
            for attr1, attr2 in pairwiseAttributes:
                label = getAmbiguousAttribute(linearizedSchema, attr1, attr2, strategy)
                if (label is not None):
                    self.ambiguousAttribute.append((attr1, attr2, label))
        if (strategy == STRATEGY_SCHEMA_WITH_DATA_SAMPLE):
            schemaWithData = self._schemaWithDataSample() ## TODO code in colab task3
            for attr1, attr2 in pairwiseAttributes:
                label = getAmbiguousAttribute(schemaWithData, attr1, attr2, strategy)
                if (label is not None):
                    self.ambiguousAttribute.append((attr1, attr2, label))
        self.ambiguousAttributeNormalized = []
        for attr1, attr2, label in self.ambiguousAttribute:
            normalizedName1 = self.mappigsAttributes[attr1[0]]
            normalizedName2 = self.mappigsAttributes[attr2[0]]
            attr1Norm = (normalizedName1, attr1[1])
            attr2Norm = (normalizedName2, attr2[1])
            self.ambiguousAttributeNormalized.append((attr1Norm, attr2Norm, label))


    def storeInDB(self, user_uenc, pw_uenc, host, port, dbname, if_exists='replace'):
        ## if_exists {'fail', 'replace', 'append'}
        #connection = getDBConnection(user_uenc, pw_uenc, host, port, dbname)
        engine = getEngine(user_uenc, pw_uenc, host, port, dbname)
        self.dataframe.to_sql(self.datasetName, engine, if_exists=if_exists)
        dataframeRows = self.dataframe.shape[0]
        rowsInDB = engine.execute("SELECT count(*) from "+self.datasetName).fetchall()[0][0]
        return dataframeRows == rowsInDB

    def _getPk(self):
        for ck in self.compositeKeys:
            if (len(ck) == 1):
                return ck
        return INDEX

    def _updateCKs(self):
        if (self.pk in self.compositeKeys):
            self.compositeKeys.remove(self.pk)

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

    def _loadDataFrame(self, file):
        df = pd.read_csv(file)
        normalizedColumns = []
        for colName, type in self.normalizedAttributes:
            if colName != INDEX[0]:
                normalizedColumns.append(colName)
        df.columns = normalizedColumns
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

    def _linearizeSchema(self):
        attributeNames = [attr[0] for attr in self.attributes]
        return "|".join(attributeNames)

    def _schemaWithDataSample(self):
        ## TODO: implement from task 3
        return ""
