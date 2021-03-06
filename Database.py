__author__ = 'Ma Haoxiang'

# import
import re
import os
import json
import logging
import random
from ZSet import ZSet
from AOFLoader import AOFLoader
from TTLTool import *
from DataFactory import *

class NoSqlDb(object):

    DB_PASSWORD_MAX_LENGTH = 12
    DB_PASSWORD_MIN_LENGTH = 6

    def __init__(self, config):
        self.dbNameSet = {"db0", "db1", "db2", "db3", "db4"}  # initialize databases
        self.saveTrigger = config["SAVE_TRIGGER"]
        self.adminKey = config["ADMIN_KEY"]
        self.saveSchema = config["SAVE_SCHEMA"]
        self.aofLogPath = config["AOF_LOG_PATH"]
        self.opCount = 0
        self.aofLoader = AOFLoader(self.aofLogPath, self)

        self.initDb()
        self.initLog(config)
        self.rdbLoad()

    def initDb(self):

        self.invertedTypeDict = DataFactory.getInvertedTypeContainer(self.dbNameSet)

        # initialize data containers
        self.elemName, self.elemDict, self.elemLockDict = DataFactory.getDataContainer(self.dbNameSet)
        self.listName, self.listDict, self.listLockDict = DataFactory.getDataContainer(self.dbNameSet)
        self.hashName, self.hashDict, self.hashLockDict = DataFactory.getDataContainer(self.dbNameSet)
        self.setName, self.setDict, self.setLockDict = DataFactory.getDataContainer(self.dbNameSet)
        self.zsetName, self.zsetDict, self.zsetLockDict = DataFactory.getDataContainer(self.dbNameSet)

        # TTL structure
        self.elemTTL = DataFactory.getTTLContainer(self.dbNameSet)
        self.listTTL = DataFactory.getTTLContainer(self.dbNameSet)
        self.hashTTL = DataFactory.getTTLContainer(self.dbNameSet)
        self.setTTL = DataFactory.getTTLContainer(self.dbNameSet)
        self.zsetTTL = DataFactory.getTTLContainer(self.dbNameSet)

        self.lockDicts = DataFactory.getLockDictContainer(self.elemLockDict, self.listLockDict,
                                                          self.hashLockDict, self.setLockDict,
                                                          self.zsetLockDict)

        self.dataNameSets = DataFactory.getNameSetContainer(self.elemName, self.listName,
                                                            self.hashName, self.setName,
                                                            self.zsetName)

        self.ttlDicts = DataFactory.getTTLDictContainer(self.elemTTL, self.listTTL,
                                                        self.hashTTL, self.setTTL,
                                                        self.zsetTTL)

        self.valueDicts = DataFactory.getValueDictContainer(self.elemDict, self.listDict,
                                                            self.hashDict, self.setDict,
                                                            self.zsetDict)

        self.dbPassword = dict()
        self.saveLock = False

    def initLog(self, config):
        # check log directory
        if os.path.exists(config["LOG_PATH"]) is False:
            os.mkdir(config["LOG_PATH"])

        # register a rdb logger
        self.rdbLogger = logging.getLogger('rdbLogger')
        self.rdbLogger.setLevel(logging.INFO)
        rdbHandler = logging.FileHandler(config["LOG_PATH"] + "rdb.log", mode="a")
        formatter = logging.Formatter('[%(asctime)s] %(name)s:'
                                      '%(levelname)s: %(message)s')
        rdbHandler.setFormatter(formatter)
        self.rdbLogger.addHandler(rdbHandler)

        # register a aof logger
        self.aofLogger = logging.getLogger('aofLogger')
        self.aofLogger.setLevel(logging.INFO)
        aofHandler = logging.FileHandler(config["LOG_PATH"] + "aof.log", mode="a")
        formatter = logging.Formatter('[%(asctime)s] %(message)s')
        aofHandler.setFormatter(formatter)
        self.aofLogger.addHandler(aofHandler)

    def translateType(self, dataType):
        typeCode = responseCode.ELEM_TYPE_ERROR
        if dataType == "ELEM":
            typeCode = responseCode.ELEM_TYPE
        elif dataType == "LIST":
            typeCode = responseCode.LIST_TYPE
        elif dataType == "HASH":
            typeCode = responseCode.HASH_TYPE
        elif dataType == "SET":
            typeCode = responseCode.SET_TYPE
        elif dataType == "ZSET":
            typeCode = responseCode.ZSET_TYPE
        return typeCode

    # get the data type of the given key name
    def getType(self, dbName, keyName):
        try:
            type = self.invertedTypeDict[dbName][keyName]
        except:
            type = responseCode.ELEM_TYPE_ERROR

        message = {
            "msg": responseCode.detail[type],
            "typeCode": type,
            "data": keyName
        }
        return message

    def getLockDict(self, type):
        try:
            lockDict = self.lockDicts[type]
            return lockDict
        except:
            return None

    def getNameSet(self, type):
        try:
            nameSet = self.dataNameSets[type]
            return nameSet
        except:
            return None

    def getTTLDict(self, type):
        try:
            ttlDict = self.ttlDicts[type]
            return ttlDict
        except:
            return None

    def getValueDict(self, type):
        try:
            valueDict = self.valueDicts[type]
            return valueDict
        except:
            return None

    def lock(self, type, dbName, keyName):
        lockDict = self.getLockDict(type)
        lockDict[dbName][keyName] = True

    def unlock(self, type, dbName, keyName):
        lockDict = self.getLockDict(type)
        lockDict[dbName][keyName] = False

    def isDbExist(self, dbName):
        return dbName in self.dbNameSet

    def isExist(self, type, dbName, *keyNames):
        if self.isDbExist(dbName) is False:
            return False
        nameSet = self.getNameSet(type)
        for keyName in keyNames:
            if keyName not in nameSet[dbName]:
                return False
        return True

    def verifyPassword(self, dbName, password):
        if dbName not in self.dbPassword:
            if password is None:
                return True
            return False
        return password == self.dbPassword[dbName]

    @passwordCheck
    def searchByRE(self, dbName, expression, dataType, password=None):
        if self.isDbExist(dbName) is False:
            return []
        searchResult = set()
        expression = re.sub("\*",".*",expression)   # convert expression to regular expression
        names = self.getNameSet(dataType)[dbName]
        logInfo = "Search Success {0} {1}"

        for name in names:
            try:
                if len(re.findall("({})".format(expression),name)) > 0:
                    searchResult.add(name)
            except:
                continue
        self.rdbLogger.info(logInfo.format(dbName, expression))
        return list(searchResult)

    @passwordCheck
    def showTTL(self, dbName, keyName, dataType, password=None):
        ttlDict = self.getTTLDict(dataType)[dbName]

        if keyName in ttlDict.keys():
            if ttlDict[keyName]["status"] is False:
                return responseCode.TTL_EXPIRED, None
            else:
                curTime = int(time.time())
                ttl = ttlDict[keyName]["ttl"]
                restTime = ttl - (curTime-ttlDict[keyName]["createAt"])
                if restTime <= 0:
                    ttlDict[keyName]["status"] = False
                    return responseCode.TTL_EXPIRED, None
                else:
                    return responseCode.TTL_SHOW_SUCCESS, restTime
        else:
            return responseCode.TTL_NO_RECORD, None

    def isExpired(self, dataType, dbName, *keyNames):
        ttlDict = self.getTTLDict(dataType)[dbName]

        for keyName in keyNames:
            if keyName not in ttlDict.keys():
                return False
            else:
                curTime = int(time.time())
                if(ttlDict[keyName]["status"] is False):
                    return True
                createAt = ttlDict[keyName]["createAt"]
                ttl = ttlDict[keyName]["ttl"]
                if curTime - createAt >= ttl:
                    ttlDict[keyName]["status"] = False
                    return True
                else:
                    return False

    def writeLog(self, log):
        if self.saveSchema == "RDB":
            self.rdbLogger.info(log)
        elif self.saveSchema == "AOF":
            self.aofLogger.info(log)

    @keyNameValidity
    @saveTrigger
    @passwordCheck
    def createElem(self, dbName, keyName, value, password=None):
        self.lock("ELEM", dbName, keyName) # lock this element avoiding r/w implements
        self.elemName[dbName].add(keyName)
        self.elemDict[dbName][keyName] = value
        self.invertedTypeDict[dbName][keyName] = responseCode.ELEM_TYPE
        self.unlock("ELEM", dbName, keyName)
        self.writeLog("CREATE_ELEM\t{}\t{}\t{}".format(dbName, keyName, value))
        return responseCode.ELEM_CREATE_SUCCESS

    @saveTrigger
    @passwordCheck
    def updateElem(self, dbName, keyName, value, password=None):
        if self.elemLockDict[dbName][keyName] is True: # element is locked
            self.rdbLogger.warning("Update Element Locked "
                                "{0}->{1}->{2}".format(dbName, keyName, value))
            return responseCode.ELEM_IS_LOCKED

        else:   # update the value
            self.lock("ELEM", dbName, keyName)
            self.elemDict[dbName][keyName] = value
            self.unlock("ELEM", dbName, keyName)
            self.writeLog("UPDATE_ELEM\t{}\t{}\t{}".format(dbName, keyName, value))
            return responseCode.ELEM_UPDATE_SUCCESS

    @passwordCheck
    def getElem(self, dbName, keyName, password=None):
        try:
            elemValue = self.elemDict[dbName][keyName]
        except:
            elemValue = None
        self.rdbLogger.info("Get Element Success "
                         "{0}->{1}".format(dbName, keyName))
        return (responseCode.ELEM_GET_SUCCESS, elemValue)

    @passwordCheck
    def searchAllElem(self, dbName, password=None):
        if self.isDbExist(dbName) is False:
            return []
        self.rdbLogger.info("Search All Elements in {0}".format(dbName))
        return list(self.elemName[dbName])

    @saveTrigger
    @passwordCheck
    def increaseElem(self, dbName, keyName, value=1, password=None):
        if self.elemLockDict[dbName][keyName] is True: # element is locked
            self.rdbLogger.warning("Increase Element Locked "
                                "{0}->{1}".format(dbName, keyName))
            return responseCode.ELEM_IS_LOCKED
        else:
            self.lock("ELEM", dbName, keyName)
            self.elemDict[dbName][keyName] += value
            self.unlock("ELEM", dbName, keyName)
            self.writeLog("INCREASE_ELEM\t{}\t{}\t{}"
                          .format(dbName, keyName, value))
            return responseCode.ELEM_INCR_SUCCESS

    @saveTrigger
    @passwordCheck
    def decreaseElem(self, dbName, keyName, value=1, password=None):
        if self.elemLockDict[dbName][keyName] is True: # element is locked
            self.rdbLogger.warning("Decrease Element Locked "
                                "{0}->{1}".format(dbName, keyName))
            return responseCode.ELEM_IS_LOCKED
        else:
            self.lock("ELEM", dbName, keyName)
            self.elemDict[dbName][keyName] -= value
            self.unlock("ELEM", dbName, keyName)
            self.writeLog("DECREASE_ELEM\t{}\t{}\t{}"
                          .format(dbName, keyName, value))
            return responseCode.ELEM_DECR_SUCCESS

    @saveTrigger
    @passwordCheck
    def deleteElem(self, dbName, keyName, password=None):
        if self.elemLockDict[dbName][keyName] is True:  # element is locked
            self.rdbLogger.warning("Delete Element Locked "
                                "{0}->{1}".format(dbName, keyName))
            return responseCode.ELEM_IS_LOCKED
        else:
            self.lock("ELEM", dbName, keyName)
            self.elemName[dbName].remove(keyName)
            self.elemDict[dbName].pop(keyName)
            try:
                self.elemTTL[dbName].pop(keyName)
                self.invertedTypeDict[dbName].pop(keyName)
            except:
                pass
            self.elemLockDict[dbName].pop(keyName)
            self.writeLog("DELETE_ELEM\t{}\t{}".format(dbName, keyName))
            return responseCode.ELEM_DELETE_SUCCESS

    @keyNameValidity
    @saveTrigger
    @passwordCheck
    def createList(self, dbName, keyName, password=None):
        self.lock("LIST", dbName, keyName)
        self.listName[dbName].add(keyName)
        self.listDict[dbName][keyName] = list()
        self.invertedTypeDict[dbName][keyName] = responseCode.LIST_TYPE
        self.unlock("LIST", dbName, keyName)
        self.writeLog("CREATE_LIST\t{}\t{}".format(dbName, keyName))
        return responseCode.LIST_CREATE_SUCCESS

    @passwordCheck
    def getList(self, dbName, keyName, start=None, end=None, password=None):
        try:
            if start is None and end is None:
                listValue = self.listDict[dbName][keyName]
            elif start is not None and end is not None:
                listValue = self.listDict[dbName][keyName][start:end]
            elif start is not None and end is None:
                listValue = self.listDict[dbName][keyName][start:]
            else:
                listValue = None
        except:
            listValue = None
        self.rdbLogger.info("Get List Success "
                         "{0}->{1}".format(dbName, keyName))
        return (responseCode.LIST_GET_SUCCESS, listValue)

    @passwordCheck
    def getListRandom(self, dbName, keyName, numRand, password=None):
        if len(self.listDict[dbName][keyName]) < numRand:
            return responseCode.LIST_LENGTH_TOO_SHORT, None
        result = random.sample(self.listDict[dbName][keyName], numRand)
        return responseCode.LIST_GET_SUCCESS, result

    @saveTrigger
    @passwordCheck
    def insertList(self, dbName, keyName, value, isLeft=None, password=None):
        if self.listLockDict[dbName][keyName] is True:
            self.rdbLogger.warning("Insert List Locked "
                                "{0}->{1}->{2}".format(dbName, keyName, value))
            return responseCode.LIST_IS_LOCKED
        else:
            self.lock("LIST", dbName, keyName)
            if isLeft is None:
                self.listDict[dbName][keyName].append(value)
                self.writeLog("INSERT_LIST\t{}\t{}\t{}".format(dbName, keyName, value))
            else:
                self.listDict[dbName][keyName].insert(0, value)
                self.writeLog("LEFT_INSERT_LIST\t{}\t{}\t{}".format(dbName, keyName, value))
            self.unlock("LIST", dbName, keyName)
            return responseCode.LIST_INSERT_SUCCESS

    @saveTrigger
    @passwordCheck
    def deleteList(self, dbName, keyName, password=None):
        if self.listLockDict[dbName][keyName] is True:
            self.rdbLogger.warning("Delete List Locked "
                                "{0}->{1}".format(dbName, keyName))
            return responseCode.LIST_IS_LOCKED
        else:
            if len(self.listDict[dbName][keyName]) > 0:
                return responseCode.LIST_NOT_EMPTY

            self.lock("LIST", dbName, keyName)
            self.listName[dbName].remove(keyName)
            self.listDict[dbName].pop(keyName)
            try:
                self.listTTL[dbName].pop(keyName)
                self.invertedTypeDict[dbName].pop(keyName)
            except:
                pass
            self.unlock("LIST", dbName, keyName)
            self.listLockDict[dbName].pop(keyName)
            self.writeLog("DELETE_LIST\t{}\t{}".format(dbName, keyName))
            return responseCode.LIST_DELETE_SUCCESS

    @saveTrigger
    @passwordCheck
    def rmFromList(self, dbName, keyName, value, password=None):
        if self.listLockDict[dbName][keyName] is True:
            self.rdbLogger.warning("Insert List Locked "
                                "{0}->{1}->{2}".format(dbName, keyName, value))
            return responseCode.LIST_IS_LOCKED
        else:
            if value not in self.listDict[dbName][keyName]:
                return responseCode.LIST_NOT_CONTAIN_VALUE
            else:
                self.lock("LIST", dbName, keyName)
                self.listDict[dbName][keyName].remove(value)
                self.unlock("LIST", dbName, keyName)
                self.writeLog("REMOVE_FROM_LIST\t{}\t{}\t{}".format(dbName, keyName, value))
                return responseCode.LIST_REMOVE_SUCCESS

    @passwordCheck
    def searchAllList(self, dbName, password=None):
        if self.isDbExist(dbName) is False:
            return []
        self.rdbLogger.info("Search All List Success {0}".format(dbName))
        return list(self.listName[dbName])

    @saveTrigger
    @passwordCheck
    def clearList(self, dbName, keyName, password=None):
        if self.listLockDict[dbName][keyName] is True:
            self.rdbLogger.warning("Clear List Locked {0}->{1}")
            return responseCode.LIST_IS_LOCKED
        else:
            self.lock("LIST", dbName, keyName)
            self.listDict[dbName][keyName] = []
            self.unlock("LIST", dbName, keyName)
            self.writeLog("CLEAR_LIST\t{}\t{}".format(dbName, keyName))
            return responseCode.LIST_CLEAR_SUCCESS

    @saveTrigger
    @passwordCheck
    def mergeLists(self, dbName, keyName1, keyName2, resultKeyName=None, password=None):
        if resultKeyName is not None:
            self.createList(dbName=dbName, keyName=resultKeyName, password=password)
            self.lock("LIST", dbName, resultKeyName)
            self.listDict[dbName][resultKeyName].extend(self.listDict[dbName][keyName1])
            self.listDict[dbName][resultKeyName].extend(self.listDict[dbName][keyName2])
            self.unlock("LIST", dbName, resultKeyName)
            self.writeLog("MERGE_LIST\t{}\t{}\t{}\t{}".format(dbName, keyName1, keyName2, resultKeyName))
            return responseCode.LIST_MERGE_SUCCESS, self.listDict[dbName][resultKeyName]
        else:
            if self.listLockDict[dbName][keyName1] is False:
                self.lock("LIST", dbName, keyName1)
                self.listDict[dbName][keyName1].extend(self.listDict[dbName][keyName2])
                self.writeLog("MERGE_LIST\t{}\t{}\t{}\t{}".format(dbName, keyName1, keyName2, keyName1))
                return responseCode.LIST_MERGE_SUCCESS, self.listDict[dbName][keyName1]
            else:
                self.rdbLogger.info("List Locked "
                                 "{}->{}".format(dbName, keyName1))
                return responseCode.LIST_IS_LOCKED, []

    @keyNameValidity
    @saveTrigger
    @passwordCheck
    def createHash(self, dbName, keyName, password=None):
        if self.isExist("HASH", dbName, keyName) is False:
            self.hashName[dbName].add(keyName)
            self.lock("HASH", dbName, keyName)
            self.hashDict[dbName][keyName] = dict()
            self.invertedTypeDict[dbName][keyName] = responseCode.HASH_TYPE
            self.unlock("HASH", dbName, keyName)
            self.writeLog("CREATE_HASH\t{}\t{}".format(dbName, keyName))
            return responseCode.HASH_CREATE_SUCCESS
        else:
            self.rdbLogger.warning("Hash Create Fail(Hash Exists) "
                                "{}->{}".format(dbName, keyName))
            return responseCode.HASH_EXISTED

    @passwordCheck
    def getHash(self, dbName, keyName, password=None):
        return responseCode.HASH_GET_SUCCESS, self.hashDict[dbName][keyName]

    @passwordCheck
    def getHashKeySet(self, dbName, keyName, password=None):
        return responseCode.HASH_KEYSET_GET_SUCCESS, list(self.hashDict[dbName][keyName].keys())

    @passwordCheck
    def getHashValues(self, dbName, keyName, password=None):
        return responseCode.HASH_VALUES_GET_SUCCESS, list(self.hashDict[dbName][keyName].values())

    @passwordCheck
    def getMultipleHashValues(self, dbName, keyName, keys, password=None):
        result = list()
        for key in keys:
            try:
                result.append(self.hashDict[dbName][keyName][key])
            except:
                result.append(None)
        return responseCode.HASH_VALUES_GET_SUCCESS, result

    @saveTrigger
    @passwordCheck
    def insertHash(self, dbName, keyName, key, value, password=None):
        if self.hashLockDict[dbName][keyName] is True:
            self.rdbLogger.warning("Hash Is Locked "
                                "{}->{}".format(dbName, keyName))
            return responseCode.HASH_IS_LOCKED
        else:
            self.lock("HASH", dbName, keyName)
            self.hashDict[dbName][keyName][key] = value
            self.unlock("HASH", dbName, keyName)
            self.writeLog("INSERT_HASH\t{}\t{}\t{}\t{}"
                          .format(dbName, keyName, key, value))
            return responseCode.HASH_INSERT_SUCCESS

    @passwordCheck
    def isKeyExist(self, dbName, keyName, key, password=None):
        if dbName not in self.dbNameSet:
            return False
        elif keyName not in self.hashDict[dbName].keys():
            return False
        return key in list(self.hashDict[dbName][keyName].keys())

    @saveTrigger
    @passwordCheck
    def deleteHash(self, dbName, keyName, password=None):
        if self.hashLockDict[dbName][keyName] is True:
            self.rdbLogger.warning("Hash Is Locked "
                                "{}->{}".format(dbName, keyName))
            return responseCode.HASH_IS_LOCKED
        else:
            if len(self.hashDict[dbName][keyName]) > 0:
                return responseCode.HASH_NOT_EMPTY
            self.lock("HASH", dbName, keyName)
            self.hashDict[dbName].pop(keyName)
            self.hashName[dbName].remove(keyName)
            try:
                self.hashTTL[dbName].pop(keyName)
                self.invertedTypeDict[dbName].pop(keyName)
            except:
                pass
            self.unlock("HASH", dbName, keyName)
            self.hashLockDict[dbName].pop(keyName)
            self.writeLog("DELETE_HASH\t{}\t{}".format(dbName, keyName))
            return responseCode.HASH_DELETE_SUCCESS

    @saveTrigger
    @passwordCheck
    def rmFromHash(self, dbName, keyName, key, password=None):
        if self.hashLockDict[dbName][keyName] is True:
            self.rdbLogger.warning("Hash Is Locked "
                                "{}->{}".format(dbName, keyName))
            return responseCode.HASH_IS_LOCKED
        else:
            self.lock("HASH", dbName, keyName)
            self.hashDict[dbName][keyName].pop(key)
            self.unlock("HASH", dbName, keyName)
            self.writeLog("REMOVE_FROM_HASH\t{}\t{}\t{}"
                          .format(dbName, keyName, key))
            return responseCode.HASH_REMOVE_SUCCESS

    @saveTrigger
    @passwordCheck
    def clearHash(self, dbName, keyName, password=None):
        if self.hashLockDict[dbName][keyName] is True:
            self.rdbLogger.warning("Hash Is Locked {}->{}"
                                   .format(dbName, keyName))
            return responseCode.HASH_IS_LOCKED
        else:
            self.lock("HASH", dbName, keyName)
            self.hashDict[dbName][keyName].clear()
            self.unlock("HASH", dbName, keyName)
            self.writeLog("CLEAR_HASH\t{}\t{}".format(dbName, keyName))
            return responseCode.HASH_CLEAR_SUCCESS

    @saveTrigger
    @passwordCheck
    def replaceHash(self, dbName, keyName, hashValue, password=None):
        if self.hashLockDict[dbName][keyName] is True:
            self.rdbLogger.warning("Hash Is Locked "
                                "{}->{}".format(dbName, keyName))
            return responseCode.HASH_IS_LOCKED
        else:
            self.lock("HASH", dbName, keyName)
            self.hashDict[dbName][keyName] = hashValue
            self.unlock("HASH", dbName, keyName)
            self.writeLog("REPLACE_HASH\t{}\t{}\t{}"
                          .format(dbName, keyName, str(hashValue)))
            return responseCode.HASH_REPLACE_SUCCESS

    @saveTrigger
    @passwordCheck
    def mergeHashs(self, dbName, keyName1, keyName2, resultKeyName=None, mergeMode=0, password=None):

        baseDictName = keyName1 if mergeMode == 0 else keyName2
        otherDictName = keyName2 if mergeMode == 0 else keyName1

        if resultKeyName is not None:
            self.createHash(dbName=dbName, keyName=resultKeyName, password=password)
            self.lock("HASH", dbName, resultKeyName)
            baseKeys = self.hashDict[dbName][baseDictName].keys()
            otherKeys = self.hashDict[dbName][otherDictName].keys()
            self.hashDict[dbName][resultKeyName] = self.hashDict[dbName][baseDictName].copy()
            for key in otherKeys:
                if key not in baseKeys:
                    self.hashDict[dbName][resultKeyName][key] = self.hashDict[dbName][otherDictName][key]
            self.unlock("HASH", dbName, resultKeyName)
            self.writeLog("MERGE_HASH\t{}\t{}\t{}\t{}\{}"
                          .format(dbName, keyName1, keyName2, resultKeyName, mergeMode))

        else:
            if self.hashLockDict[dbName][baseDictName] is False:
                self.lock("HASH", dbName, baseDictName)
                baseKeys = self.hashDict[dbName][baseDictName].keys()
                otherKeys = self.hashDict[dbName][otherDictName].keys()
                for key in otherKeys:
                    if key not in baseKeys:
                        self.hashDict[dbName][baseDictName][key] = self.hashDict[dbName][otherDictName][key]
                self.unlock("HASH", dbName, baseDictName)
                self.writeLog("MERGE_HASH\t{}\t{}\t{}\t{}\t{}"
                              .format(dbName, keyName1, keyName2, baseDictName, mergeMode))
            else:
                self.rdbLogger.warning("Hash Is Locked "
                                    "{}->{} or {}->{}".
                                       format(dbName, keyName1, dbName, keyName2))
                return responseCode.HASH_IS_LOCKED
        return responseCode.HASH_MERGE_SUCCESS

    @passwordCheck
    def searchAllHash(self, dbName, password=None):
        if self.isDbExist(dbName) is False:
            return []
        self.rdbLogger.info("Search All Hash Success "
                         "{0}".format(dbName))
        return list(self.hashName[dbName])

    @saveTrigger
    @passwordCheck
    def increaseHash(self, dbName, keyName, key, value, password=None):
        if isinstance(self.hashDict[dbName][keyName][key], int) is False:
            self.rdbLogger.warning("Hash Value Type Is Not Integer "
                                "{}->{}:{}".format(dbName, keyName, key))
            return responseCode.ELEM_TYPE_ERROR, None

        self.lock("HASH", dbName, keyName)
        self.hashDict[dbName][keyName][key] += value
        self.unlock("HASH", dbName, keyName)
        self.writeLog("INCREASE_HASH\t{}\t{}\t{}\t{}"
                      .format(dbName, keyName, key, value))
        return responseCode.HASH_INCR_SUCCESS, self.hashDict[dbName][keyName][key]

    @saveTrigger
    @passwordCheck
    def decreaseHash(self, dbName, keyName, key, value, password=None):
        if isinstance(self.hashDict[dbName][keyName][key], int) is False:
            self.rdbLogger.warning("Hash Value Type Is Not Integer "
                                "{}->{}:{}".format(dbName, keyName, key))
            return responseCode.ELEM_TYPE_ERROR, None

        self.lock("HASH", dbName, keyName)
        self.hashDict[dbName][keyName][key] -= value
        self.unlock("HASH", dbName, keyName)
        self.writeLog("DECREASE_HASH\t{}\t{}\t{}\t{}"
                      .format(dbName, keyName, key, value))
        return responseCode.HASH_DECR_SUCCESS, self.hashDict[dbName][keyName][key]

    @keyNameValidity
    @saveTrigger
    @passwordCheck
    def createSet(self, dbName, keyName, password=None):
        self.lock("SET", dbName, keyName)
        self.setName[dbName].add(keyName)
        self.setDict[dbName][keyName] = set()
        self.invertedTypeDict[dbName][keyName] = responseCode.SET_TYPE
        self.unlock("SET", dbName, keyName)
        self.writeLog("CREATE_SET\t{}\t{}".format(dbName, keyName))
        return responseCode.SET_CREATE_SUCCESS

    @passwordCheck
    def getSet(self, dbName, keyName, password=None):
        return list(self.setDict[dbName][keyName])

    @passwordCheck
    def getSetRandom(self, dbName, setName, numRand, password=None):
        if len(self.setDict[dbName][setName]) < numRand:
            return responseCode.SET_LENGTH_TOO_SHORT, None
        result = random.sample(self.setDict[dbName][setName], numRand)
        return responseCode.SET_GET_SUCCESS, result

    @saveTrigger
    @passwordCheck
    def insertSet(self, dbName, keyName, value, password=None):
        if self.setLockDict[dbName][keyName] is True:
            self.rdbLogger.warning("Set Is Locked "
                                "{0}->{1}".format(dbName, keyName))
            return responseCode.SET_IS_LOCKED
        else:
            if value not in self.setDict[dbName][keyName]:
                self.lock("SET", dbName, keyName)
                self.setDict[dbName][keyName].add(value)
                self.unlock("SET", dbName, keyName)
                self.writeLog("INSERT_SET\t{}\t{}\t{}"
                              .format(dbName, keyName, value))
                return responseCode.SET_INSERT_SUCCESS
            else:
                return responseCode.SET_VALUE_ALREADY_EXIST

    @saveTrigger
    @passwordCheck
    def rmFromSet(self, dbName, keyName, value, password=None):
        if self.setLockDict[dbName][keyName] is True:
            self.rdbLogger.warning("Set Is Locked "
                                "{0}->{1}".format(dbName, keyName))
            return responseCode.SET_IS_LOCKED
        else:
            if value in self.setDict[dbName][keyName]:
                self.lock("SET", dbName, keyName)
                self.setDict[dbName][keyName].discard(value)
                self.unlock("SET", dbName, keyName)
                self.writeLog("REMOVE_FROM_SET\t{}\t{}\t{}"
                              .format(dbName, keyName, value))
                return responseCode.SET_REMOVE_SUCCESS
            else:
                return responseCode.SET_VALUE_NOT_EXIST

    @saveTrigger
    @passwordCheck
    def clearSet(self, dbName, keyName, password=None):
        if self.setLockDict[dbName][keyName] is True:
            self.rdbLogger.warning("Set Is Locked "
                                "{0}->{1}".format(dbName, keyName))
            return responseCode.SET_IS_LOCKED
        else:
            self.lock("SET", dbName, keyName)
            self.setDict[dbName][keyName].clear()
            self.unlock("SET", dbName, keyName)
            self.writeLog("CLEAR_SET\t{}\t{}"
                          .format(dbName, keyName))
            return responseCode.SET_CLEAR_SUCCESS

    @saveTrigger
    @passwordCheck
    def deleteSet(self, dbName, keyName, password=None):
        if self.setLockDict[dbName][keyName] is True:
            self.rdbLogger.warning("Set Is Locked "
                                "{0}->{1}".format(dbName, keyName))
            return responseCode.SET_IS_LOCKED
        else:
            if len(self.setDict[dbName][keyName]) > 0:
                return responseCode.SET_NOT_EMPTY
            self.lock("SET", dbName, keyName)
            self.setName[dbName].discard(keyName)
            self.setDict[dbName].pop(keyName)
            self.invertedTypeDict[dbName].pop(keyName)
            self.unlock("SET", dbName, keyName)
            self.setLockDict[dbName].pop(keyName)
            self.writeLog("DELETE_SET\t{}\t{}"
                          .format(dbName, keyName))
            return responseCode.SET_DELETE_SUCCESS

    @passwordCheck
    def searchAllSet(self, dbName, password=None):
        if self.isDbExist(dbName) is False:
            return []
        self.rdbLogger.info("Search All Set Success "
                         "{0}".format(dbName))
        return list(self.setName[dbName])

    @saveTrigger
    @passwordCheck
    def unionSet(self, dbName, keyName1, keyName2, unionResult, password=None):
        if (self.setLockDict[dbName][keyName1] is True
                or self.setLockDict[dbName][keyName2] is True):
            self.rdbLogger.warning("Set Is Locked "
                                "{0}->{1} or {2}->{3}"
                                   .format(dbName, keyName1, dbName, keyName2))
            return responseCode.SET_IS_LOCKED
        else:
            self.lock("SET", dbName, keyName1)
            self.lock("SET", dbName, keyName2)
            unionResult.append(list(self.setDict[dbName][keyName1].
                                    union(self.setDict[dbName][keyName2])))
            self.unlock("SET", dbName, keyName1)
            self.unlock("SET", dbName, keyName2)
            self.rdbLogger.info("Set Union Success "
                             "{}->{} unions {}->{} to {}->{}"
                                .format(dbName, keyName1, dbName, keyName2, dbName, unionResult))
            return responseCode.SET_UNION_SUCCESS

    @saveTrigger
    @passwordCheck
    def intersectSet(self, dbName, setName1, setName2, intersectResult, password=None):
        if (self.setLockDict[dbName][setName1] is True
            or self.setLockDict[dbName][setName2] is True):
            self.rdbLogger.warning("Set Is Locked "
                                "{0}->{1} or {2}->{3}"
                                   .format(dbName, setName1, dbName, setName2))
            return responseCode.SET_IS_LOCKED
        else:
            self.lock("SET", dbName, setName1)
            self.lock("SET", dbName, setName2)
            intersectResult.append(list(self.setDict[dbName][setName1].
                                        intersection(self.setDict[dbName][setName2])))
            self.unlock("SET", dbName, setName1)
            self.unlock("SET", dbName, setName2)
            self.rdbLogger.info("Set Intersect Success "
                             "{}->{} intersects {}->{} to {}->{}"
                                .format(dbName, setName1, dbName, setName2, dbName, intersectResult))
            return responseCode.SET_INTERSECT_SUCCESS

    @saveTrigger
    @passwordCheck
    def diffSet(self, dbName, setName1, setName2, diffResult, password=None):
        if (self.setLockDict[dbName][setName1] is True
            or self.setLockDict[dbName][setName2] is True):
            self.rdbLogger.warning("Set Is Locked "
                                "{}->{} or {}->{}"
                                   .format(dbName, setName1, dbName, setName2))
            return responseCode.SET_IS_LOCKED
        else:
            self.lock("SET", dbName, setName1)
            self.lock("SET", dbName, setName2)
            diffResult.append(list(self.setDict[dbName][setName1].
                                   difference(self.setDict[dbName][setName2])))
            self.unlock("SET", dbName, setName1)
            self.unlock("SET", dbName, setName2)
            self.rdbLogger.info("Set Diff Success "
                             "{}->{} unions {}->{} to {}->{}"
                                .format(dbName, setName1, dbName, setName2, dbName, diffResult))
            return responseCode.SET_DIFF_SUCCESS

    @saveTrigger
    @passwordCheck
    def replaceSet(self, dbName, keyName, value, password=None):
        if self.setLockDict[dbName][keyName] is True:
            self.rdbLogger.warning("Set Is Locked "
                                "{}->{}".format(dbName, keyName))
            return responseCode.SET_IS_LOCKED
        else:
            self.lock("SET", dbName, keyName)
            self.setDict[dbName][keyName] = value
            self.unlock("SET", dbName, keyName)
            self.writeLog("REPLACE_SET\t{}\t{}\t{}"
                          .format(dbName, keyName, str(value)))
            return responseCode.SET_REPLACE_SUCCESS

    @keyNameValidity
    @saveTrigger
    @passwordCheck
    def createZSet(self, dbName, keyName, password=None):
        self.lock("ZSET", dbName, keyName)
        self.zsetName[dbName].add(keyName)
        self.zsetDict[dbName][keyName] = ZSet()
        self.invertedTypeDict[dbName][keyName] = responseCode.ZSET_TYPE
        self.unlock("ZSET", dbName, keyName)
        self.writeLog("CREATE_ZSET\t{}\t{}".format(dbName, keyName))
        return responseCode.ZSET_CREATE_SUCCESS

    @passwordCheck
    def getZSet(self, dbName, keyName, password=None):
        return self.zsetDict[dbName][keyName].get()

    @saveTrigger
    @passwordCheck
    def insertZSet(self, dbName, keyName, value, score, password=None):
        if self.zsetLockDict[dbName][keyName] is True:
            self.rdbLogger.warning("ZSet Is Locked "
                                "{0}->{1}".format(dbName, keyName))
            return responseCode.ZSET_IS_LOCKED
        else:
            try:
                self.lock("ZSET", dbName, keyName)
                if self.zsetDict[dbName][keyName].add(value, score) is True:
                    self.writeLog("INSERT_ZSET\t{}\t{}\t{}\t{}"
                                  .format(dbName, keyName, value, score))
                    return responseCode.ZSET_INSERT_SUCCESS
                else:
                    self.rdbLogger.warning("ZSet Insert Fail(Value Existed) "
                                        "{}->{}:{}"
                                           .format(dbName, keyName, value))
                    return responseCode.ZSET_VALUE_ALREADY_EXIST
            finally:
                self.unlock("ZSET", dbName, keyName)

    @saveTrigger
    @passwordCheck
    def rmFromZSet(self, dbName, keyName, value, password=None):
        if self.zsetLockDict[dbName][keyName] is True:
            self.rdbLogger.warning("ZSet Is Locked "
                                "{}->{}".format(dbName, keyName))
            return responseCode.ZSET_IS_LOCKED
        else:
            self.lock("ZSET", dbName, keyName)
            result = self.zsetDict[dbName][keyName].remove(value)
            self.unlock("ZSET", dbName, keyName)
            if(result is True):
                self.writeLog("REMOVE_FROM_ZSET\t{}\t{}\t{}"
                              .format(dbName, keyName, value))
            else:
                self.rdbLogger.info("ZSet Remove Fail(Value Not Existed) "
                                 "{}->{}:{}".format(dbName, keyName, value))
            return responseCode.ZSET_REMOVE_SUCCESS if result is True \
                else responseCode.ZSET_NOT_CONTAIN_VALUE

    @saveTrigger
    @passwordCheck
    def clearZSet(self, dbName, keyName, password=None):
        if self.zsetLockDict[dbName][keyName] is True:
            self.rdbLogger.warning("ZSet Is Locked "
                                "{}->{}".format(dbName, keyName))
            return responseCode.ZSET_IS_LOCKED
        else:
            self.lock("ZSET", dbName, keyName)
            self.zsetDict[dbName][keyName].clear()
            self.unlock("ZSET", dbName, keyName)
            self.writeLog("CLEAR_ZSET\t{}\t{}".format(dbName, keyName))
            return responseCode.ZSET_CLEAR_SUCCESS

    @saveTrigger
    @passwordCheck
    def deleteZSet(self, dbName, keyName, password=None):
        if self.zsetLockDict[dbName][keyName] is True:
            self.rdbLogger.warning("ZSet Is Locked "
                                "{}->{}".format(dbName, keyName))
            return responseCode.ZSET_IS_LOCKED
        else:
            if len(self.zsetDict[dbName][keyName].valueDict) > 0:
                return responseCode.ZSET_NOT_EMPTY
            self.lock("ZSET", dbName, keyName)
            self.zsetName[dbName].discard(keyName)
            self.zsetDict[dbName].pop(keyName)
            self.invertedTypeDict[dbName].pop(keyName)
            self.unlock("ZSET", dbName, keyName)
            self.zsetLockDict[dbName].pop(keyName)
            self.writeLog("DELETE_ZSET\t{}\t{}"
                          .format(dbName, keyName))
            return responseCode.ZSET_DELETE_SUCCESS

    @passwordCheck
    def searchAllZSet(self, dbName, password=None):
        if self.isDbExist(dbName) is False:
            return []
        self.rdbLogger.info("Search All ZSet Success "
                         "{}".format(dbName))
        return list(self.zsetName[dbName])

    @passwordCheck
    def findMinFromZSet(self, dbName, keyName, password=None):
        return self.zsetDict[dbName][keyName].findMin()

    @passwordCheck
    def findMaxFromZSet(self, dbName, keyName, password=None):
        return self.zsetDict[dbName][keyName].findMax()

    @passwordCheck
    def getScoreFromZSet(self, dbName, keyName, valueName, password=None):
        result = self.zsetDict[dbName][keyName].find(valueName)
        return result[1]

    @passwordCheck
    def getValuesByRange(self, dbName, keyName, start, end, password=None):
        traverseResult = self.zsetDict[dbName][keyName].get()
        result = dict()
        for each in traverseResult:
            if each[1] >= start and each[1] < end:
                result[each[0]] = each[1]
        return result

    @passwordCheck
    def getSize(self, dbName, keyName, type, password=None):
        if type == "ZSET":
            return (responseCode.GET_SIZE_SUCCESS, self.zsetDict[dbName][keyName].size())
        data = self.getValueDict(type)[dbName][keyName]
        return (responseCode.GET_SIZE_SUCCESS, len(data))

    @passwordCheck
    def getRank(self, dbName, keyName, value, password=None):
        return self.zsetDict[dbName][keyName].getRank(value)

    @saveTrigger
    @passwordCheck
    def rmByScore(self, dbName, keyName, start, end, password=None):
        if self.zsetLockDict[dbName][keyName] is True:
            self.rdbLogger.warning("ZSet Is Locked "
                                "{}->{}".format(dbName, keyName))
            return (responseCode.ZSET_IS_LOCKED, 0)
        else:
            self.lock("ZSET", dbName, keyName)
            result = self.zsetDict[dbName][keyName].removeByScore(start, end)
            self.unlock("ZSET", dbName, keyName)
            self.writeLog("REMOVE_FROM_ZSET_BY_SCORE\t{}\t{}\t{}"
                          .format(dbName, keyName, str(result)))
            return (responseCode.ZSET_REMOVE_BY_SCORE_SUCCESS, len(result))

    @saveTrigger
    def addDatabase(self, adminKey, dbName):
        if adminKey != self.adminKey:
            return responseCode.ADMIN_KEY_ERROR

        if self.saveLock is True:
            self.rdbLogger.warning("Database Save Locked "
                                "{0}".format(dbName))
            return responseCode.DB_SAVE_LOCKED
        else:
            if dbName not in self.dbNameSet:
                self.dbNameSet.add(dbName)
                self.elemName[dbName] = set()
                self.elemDict[dbName] = dict()
                self.elemLockDict[dbName] = dict()
                self.listName[dbName] = set()
                self.listDict[dbName] = dict()
                self.elemLockDict[dbName] = dict()
                self.writeLog("ADD_DATABASE\t{}".format(dbName))
                return responseCode.DB_CREATE_SUCCESS
            else:
                self.rdbLogger.warning("Database Already Exists "
                                    "{}".format(dbName))
                return responseCode.DB_EXISTED

    def getAllDatabase(self, adminKey):
        if adminKey != self.adminKey:
            return responseCode.ADMIN_KEY_ERROR, None
        else:
            self.rdbLogger.info("Get All Database Names Success")
            return responseCode.DB_GET_SUCCESS, list(self.dbNameSet)

    @saveTrigger
    def delDatabase(self, adminKey, dbName):
        if adminKey != self.adminKey:
            return responseCode.ADMIN_KEY_ERROR
        if self.isDbExist(dbName) is True:
            if self.saveLock is False:
                self.saveLock = True
                self.dbNameSet.remove(dbName)
                for root, dirs, files in os.walk("data"+os.sep+dbName, topdown=False):
                    for name in files:
                        os.remove(os.path.join(root, name))
                    for name in dirs:
                        os.rmdir(os.path.join(root, name))
                os.rmdir("data"+os.sep+dbName)
                self.saveLock = False
                self.writeLog("DELETE_DATABASE\t{}".format(dbName))
                return responseCode.DB_DELETE_SUCCESS
            else:
                self.rdbLogger.warning("Database Delete Fail(Save Locked) "
                                    "{}".format(dbName))
                return responseCode.DB_SAVE_LOCKED
        else:
            self.rdbLogger.warning("Database Delete Fail(Not Existed) "
                                "{}".format(dbName))
            return responseCode.DB_NOT_EXIST

    def saveZSet(self, dbName, dataFileName, valueFileName, TTLFileName):
        with open("data" + os.sep + dbName + os.sep + dataFileName, "w") as nameFile:
            nameFile.write(json.dumps(list(self.zsetName[dbName])))
        with open("data" + os.sep + dbName + os.sep + TTLFileName, "w") as TTLFile:
            TTLFile.write(json.dumps(self.zsetTTL[dbName]))
        with open("data" + os.sep + dbName + os.sep + valueFileName, "w") as valueFile:
            saveDict = dict()
            for zsetName in self.zsetDict[dbName].keys():
                zset = self.zsetDict[dbName][zsetName]
                saveDict[zsetName] = zset.valueDict
            valueFile.write(json.dumps(saveDict))

    def saveData(self, dbName, dataType, dataFileName, valueFileName, TTLFileName):
        names = self.getNameSet(dataType)
        values = self.getValueDict(dataType)
        ttl = self.getTTLDict(dataType)

        with open("data" + os.sep + dbName + os.sep + dataFileName, "w") as nameFile:
            nameFile.write(json.dumps(list(names[dbName])))
        with open("data" + os.sep + dbName + os.sep + valueFileName, "w") as valueFile:
            if dataType == "SET":
                setValue = values[dbName].copy()
                for key in setValue.keys():
                    setValue[key] = list(setValue[key])
                valueFile.write(json.dumps(setValue))
            else:
                valueFile.write(json.dumps(values[dbName]))
        with open("data" + os.sep + dbName + os.sep + TTLFileName, "w") as TTLFile:
            TTLFile.write(json.dumps(ttl[dbName]))

    def saveDatabase(self):
        if self.saveSchema == "RDB":
            self.rdbSave()
        elif self.saveSchema == "AOF":
            pass

    def rdbSave(self):
        if self.saveLock is False:
            # check if the data directory exists
            if os.path.exists("./data/") is False:
                os.makedirs("data")

            for dbName in self.dbNameSet:
                if os.path.exists("data{}{}".format(os.sep,dbName)) is False:
                    os.makedirs("data{}{}".format(os.sep,dbName))

            self.saveLock = True

            for dbName in self.dbNameSet:
                self.saveData(dbName, "ELEM", "elemName.txt", "elemValue.txt", "elemTTL.txt")
                self.saveData(dbName, "LIST", "listName.txt", "listValue.txt", "listTTL.txt")
                self.saveData(dbName, "HASH", "hashName.txt", "hashValue.txt", "hashTTL.txt")
                self.saveData(dbName, "SET", "setName.txt", "setValue.txt", "setTTL.txt")
                self.saveZSet(dbName, "zsetName.txt","zsetValue.txt", "zsetTTL.txt")

            self.saveLock = False
            self.rdbLogger.info("Database Save Success")
            self.opCount = 0    # once save process is done, reload the op count
            return responseCode.DB_SAVE_SUCCESS

        else:
            self.rdbLogger.warning("Database Save Locked")
            return responseCode.DB_SAVE_LOCKED

    def loadDatabase(self):
        if self.saveSchema == "RDB":
            self.rdbLoad()
        elif self.saveSchema == "AOF":
            self.aofLoad()

    def rdbLoadZSet(self, dbName, dataFileName, valueFileName, TTLFileName):
        # load names
        with open("data" + os.sep + dbName + os.sep + dataFileName, "r") as nameFile:
            names = json.loads(nameFile.read())
            for name in names:
                self.zsetName[dbName].add(name)
                self.zsetLockDict[dbName][name] = False

        # load values
        with open("data" + os.sep + dbName + os.sep + valueFileName, "r") as valueFile:
            data = json.loads(valueFile.read())
            for zsetName in data.keys():
                self.zsetDict[dbName][zsetName] = ZSet()
                values = data[zsetName]
                for key in values.keys():
                    self.zsetDict[dbName][zsetName].add(key, values[key])

        # load TTL
        with open("data" + os.sep + dbName + os.sep + TTLFileName, "r") as TTLFile:
            self.zsetTTL[dbName] = json.loads(TTLFile.read())

    def rdbLoadData(self, dbName, dataType, dataFileName, valueFileName, TTLFileName):
        nameDict = self.getNameSet(dataType)
        valueDict = self.getValueDict(dataType)
        ttlDict = self.getTTLDict(dataType)
        lockDict = self.getLockDict(dataType)

        # load names
        with open("data" + os.sep + dbName + os.sep + dataFileName, "r") as nameFile:
            names = json.loads(nameFile.read())
            for name in names:
                nameDict[dbName].add(name)
                lockDict[dbName][name] = False
                self.invertedTypeDict[dbName][name] = self.translateType(dataType)

        # load values
        with open("data" + os.sep + dbName + os.sep + valueFileName, "r") as valueFile:
            if dataType == "SET":
                setValue = json.loads(valueFile.read())
                for key in setValue.keys():
                    setValue[key] = set(setValue[key])
                valueDict[dbName] = setValue
            else:
                valueDict[dbName] = json.loads(valueFile.read())
        # load TTL
        with open("data" + os.sep + dbName + os.sep + TTLFileName, "r") as TTLFile:
            ttlDict[dbName] = json.loads(TTLFile.read())

    def rdbLoad(self):
        try:
            if not os.path.exists("data"):
                os.mkdir("data")
                for dbName in self.dbNameSet:
                    os.mkdir("data/{}".format(dbName))
            else:
                dbNameSet = os.listdir("data")  # find all dbName in the data directory

                customDbNameSet = set(dbNameSet).difference(set(self.dbNameSet))
                DataFactory.initDataContainer(self.elemName, self.elemDict, self.elemLockDict, customDbNameSet)
                DataFactory.initDataContainer(self.listName, self.listDict, self.listLockDict, customDbNameSet)
                DataFactory.initDataContainer(self.hashName, self.hashDict, self.hashLockDict, customDbNameSet)
                DataFactory.initDataContainer(self.setName, self.setDict, self.setLockDict, customDbNameSet)
                DataFactory.initDataContainer(self.zsetName, self.zsetDict, self.zsetLockDict, customDbNameSet)

                for dbName in dbNameSet:
                    self.dbNameSet.add(dbName)
                    self.invertedTypeDict[dbName] = dict()

                    # load data
                    self.rdbLoadData(dbName, "ELEM", "elemName.txt", "elemValue.txt", "elemTTL.txt")
                    self.rdbLoadData(dbName, "LIST", "listName.txt", "listValue.txt", "listTTL.txt")
                    self.rdbLoadData(dbName, "HASH", "hashName.txt", "hashValue.txt", "hashTTL.txt")
                    self.rdbLoadData(dbName, "SET", "setName.txt", "setValue.txt", "setTTL.txt")
                    self.rdbLoadZSet(dbName, "zsetName.txt", "zsetValue.txt", "zsetTTL.txt")

            self.rdbLogger.info("Database Load Success")

        except Exception as e:
            self.rdbLogger.warning("Database Load Fail {0}".format(str(e)))

    def aofSave(self):
        pass

    def aofLoad(self):
        self.aofLoader.build()

    def setDbPassword(self, adminKey, dbName, password):
        if adminKey != self.adminKey:
            return responseCode.ADMIN_KEY_ERROR

        if dbName in self.dbPassword.keys():
            return responseCode.DB_PASSWORD_EXIST

        if (len(password) < NoSqlDb.DB_PASSWORD_MIN_LENGTH or
            len(password) > NoSqlDb.DB_PASSWORD_MAX_LENGTH):
            return responseCode.DB_PASSWORD_LENGTH_ERROR

        self.dbPassword[dbName] = password
        self.writeLog("SET_DATABASE_PWD\t{}\t{}"
                      .format(dbName, password))
        return responseCode.DB_PASSWORD_SET_SUCCESS

    def changeDbPassword(self, adminKey, dbName, originalPwd, newPwd):
        if adminKey != self.adminKey:
            return responseCode.ADMIN_KEY_ERROR

        if dbName not in self.dbPassword.keys():
            return responseCode.DB_PASSWORD_NOT_EXIST

        if self.dbPassword[dbName] != originalPwd:
            return responseCode.DB_PASSWORD_ERROR

        if (Utils.isInt(newPwd) or
            len(newPwd) < NoSqlDb.DB_PASSWORD_MIN_LENGTH or
            len(newPwd) > NoSqlDb.DB_PASSWORD_MAX_LENGTH):
            return responseCode.ELEM_TYPE_ERROR

        self.dbPassword[dbName] = newPwd
        self.writeLog("CHANGE_DATABASE_PWD\t{}\t{}"
                      .format(dbName, originalPwd, newPwd))
        return responseCode.DB_PASSWORD_CHANGE_SUCCESS

    def removeDbPassword(self, adminKey, dbName):
        if adminKey != self.adminKey:
            return responseCode.ADMIN_KEY_ERROR

        if dbName not in self.dbPassword.keys():
            return responseCode.DB_PASSWORD_NOT_EXIST

        self.dbPassword.pop(dbName)
        self.writeLog("REMOVE_DATABASE_PWD\t{}".format(dbName))
        return responseCode.DB_PASSWORD_REMOVE_SUCCESS
