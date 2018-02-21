__author__ = 'Ma Haoxiang'

class DataFactory(object):

    @staticmethod
    def initDataContainer(dataName, dataDict, dataLockDict, dbNameSet):
        for dbName in dbNameSet:
            dataName[dbName] = set()
            dataDict[dbName] = dict()
            dataLockDict[dbName] = dict()
        return dataName, dataDict, dataLockDict

    @staticmethod
    def getDataContainer(dbNameSet):
        dataNameContainer = dict()
        dataDictContainer = dict()
        dataLockDictContainer = dict()
        return DataFactory.initDataContainer(dataNameContainer, dataDictContainer,
                                             dataLockDictContainer, dbNameSet)

    @staticmethod
    def getTTLContainer(dbNameSet):
        ttlContainer = dict()
        for dbName in dbNameSet:
            ttlContainer[dbName] = dict()
        return ttlContainer

    @staticmethod
    def getInvertedTypeContainer(dbNameSet):
        invertedTypeContainer = dict()
        for dbName in dbNameSet:
            invertedTypeContainer[dbName] = dict()
        return invertedTypeContainer
