__author__ = 'Ma Haoxiang'

# import
import re
import os
import json
import logging
import random
from ZSet import zset
from TTLTool import *

class NoSqlDb(object):

    def __init__(self, config):
        self.dbNameSet = {"db0", "db1", "db2", "db3", "db4"}  # initialize databases
        self.saveTrigger = config["SAVE_TRIGGER"]
        self.opCount = 0

        self.initDb()
        self.initLog(config)
        self.loadDb()

    def initDb(self):

        self.invertedTypeDict = dict()

        # element structures
        self.elemName = dict()
        self.elemDict = dict()
        self.elemLockDict = dict()

        # list structures
        self.listName = dict()
        self.listDict = dict()
        self.listLockDict = dict()

        # hash structures
        self.hashName = dict()
        self.hashDict = dict()
        self.hashLockDict = dict()

        # set structures
        self.setName = dict()
        self.setDict = dict()
        self.setLockDict = dict()

        # zset structures
        self.zsetName = dict()
        self.zsetDict = dict()
        self.zsetLockDict = dict()

        self.saveLock = False

        # TTL structure
        self.elemTTL = dict()
        self.listTTL = dict()
        self.hashTTL = dict()
        self.setTTL = dict()
        self.zsetTTL = dict()

        for dbName in self.dbNameSet:
            self.invertedTypeDict[dbName] = dict()

            self.elemName[dbName] = set()
            self.elemDict[dbName] = dict()
            self.elemLockDict[dbName] = dict()

            self.listName[dbName] = set()
            self.listDict[dbName] = dict()
            self.listDict[dbName] = dict()

            self.hashName[dbName] = set()
            self.hashDict[dbName] = dict()
            self.hashLockDict[dbName] = dict()

            self.setName[dbName] = set()
            self.setDict[dbName] = dict()
            self.setLockDict[dbName] = dict()

            self.zsetName[dbName] = set()
            self.zsetDict[dbName] = dict()
            self.zsetLockDict[dbName] = dict()

            self.elemTTL[dbName] = dict()
            self.listTTL[dbName] = dict()
            self.hashTTL[dbName] = dict()
            self.setTTL[dbName] = dict()
            self.zsetTTL[dbName] = dict()

    def initLog(self, config):
        # check log directory
        if os.path.exists(config["LOG_PATH"]) is False:
            os.mkdir(config["LOG_PATH"])

        # register a logger
        self.logger = logging.getLogger('dbLogger')
        self.logger.setLevel(logging.INFO)
        hdr = logging.FileHandler(config["LOG_PATH"] + "db.log", mode="a")
        formatter = logging.Formatter('[%(asctime)s] %(name)s:'
                                      '%(levelname)s: %(message)s')
        hdr.setFormatter(formatter)
        self.logger.addHandler(hdr)

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
        if type == "ELEM":
            lockDict = self.elemLockDict
        elif type == "LIST":
            lockDict = self.listLockDict
        elif type == "HASH":
            lockDict = self.hashLockDict
        elif type == "SET":
            lockDict = self.setLockDict
        elif type == "ZSET":
            lockDict = self.zsetLockDict
        else:
            raise Exception("Type Error")
        return lockDict

    def getNameSet(self, type):
        if type == "ELEM":
            nameSet = self.elemName
        elif type == "LIST":
            nameSet = self.listName
        elif type == "HASH":
            nameSet = self.hashName
        elif type == "SET":
            nameSet = self.setName
        elif type == "ZSET":
            nameSet = self.zsetName
        else:
            raise Exception("Type Error")
        return nameSet

    def getTTLDict(self, type):
        if type == "ELEM":
            ttlDict = self.elemTTL
        elif type == "LIST":
            ttlDict = self.listTTL
        elif type == "HASH":
            ttlDict = self.hashTTL
        elif type == "SET":
            ttlDict = self.setTTL
        elif type == "ZSET":
            ttlDict = self.zsetTTL
        else:
            raise Exception("Type Error")
        return ttlDict

    def getValueDict(self, type):
        if type == "ELEM":
            valueDict = self.elemDict
        elif type == "LIST":
            valueDict = self.listDict
        elif type == "HASH":
            valueDict = self.hashDict
        elif type == "SET":
            valueDict = self.setDict
        elif type == "ZSET":
            valueDict = self.zsetDict
        else:
            raise Exception("Type Error")
        return valueDict

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

    def searchByRE(self, dbName, expression, dataType):
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
        self.logger.info(logInfo.format(dbName, expression))
        return list(searchResult)

    def showTTL(self, dbName, keyName, dataType):
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

    @keyNameValidity
    @saveTrigger
    def createElem(self, dbName, elemName, value):
        self.lock("ELEM", dbName, elemName) # lock this element avoiding r/w implements
        self.elemName[dbName].add(elemName)
        self.elemDict[dbName][elemName] = value
        self.invertedTypeDict[dbName][elemName] = responseCode.ELEM_TYPE
        self.unlock("ELEM", dbName, elemName)
        self.logger.info("Create Element Success "
                         "{0}->{1}->{2}".format(dbName, elemName, value))
        return responseCode.ELEM_CREATE_SUCCESS

    def updateElem(self, dbName, elemName, value):
        if self.elemLockDict[dbName][elemName] is True: # element is locked
            self.logger.warning("Update Element Locked "
                                "{0}->{1}->{2}".format(dbName, elemName, value))
            return responseCode.ELEM_IS_LOCKED

        else:   # update the value
            self.lock("ELEM", dbName, elemName)
            self.elemDict[dbName][elemName] = value
            self.unlock("ELEM", dbName, elemName)
            self.logger.info("Update Element Success "
                             "{0}->{1}->{2}".format(dbName, elemName, value))
            return responseCode.ELEM_UPDATE_SUCCESS

    def getElem(self, elemName, dbName):
        try:
            elemValue = self.elemDict[dbName][elemName]
        except:
            elemValue = None
        self.logger.info("Get Element Success "
                         "{0}->{1}".format(dbName, elemName))
        return elemValue

    def searchAllElem(self, dbName):
        if self.isDbExist(dbName) is False:
            return []
        self.logger.info("Search All Elements in {0}".format(dbName))
        return list(self.elemName[dbName])

    @saveTrigger
    def increaseElem(self, elemName, dbName):
        if self.elemLockDict[dbName][elemName] is True: # element is locked
            self.logger.warning("Increase Element Locked "
                                "{0}->{1}".format(dbName, elemName))
            return responseCode.ELEM_IS_LOCKED
        else:
            self.lock("ELEM", dbName, elemName)
            self.elemDict[dbName][elemName] += 1
            self.unlock("ELEM", dbName, elemName)
            self.logger.info("Increase Element Success "
                             "{0}->{1}".format(dbName, elemName))
            return responseCode.ELEM_INCR_SUCCESS

    @saveTrigger
    def decreaseElem(self, elemName, dbName):
        if self.elemLockDict[dbName][elemName] is True: # element is locked
            self.logger.warning("Decrease Element Locked "
                                "{0}->{1}".format(dbName, elemName))
            return responseCode.ELEM_IS_LOCKED
        else:
            self.lock("ELEM", dbName, elemName)
            self.elemDict[dbName][elemName] -= 1
            self.unlock("ELEM", dbName, elemName)
            self.logger.info("Decrease Element Success "
                             "{0}->{1}".format(dbName, elemName))
            return responseCode.ELEM_DECR_SUCCESS

    @saveTrigger
    def deleteElem(self, elemName, dbName):
        if self.elemLockDict[dbName][elemName] is True:  # element is locked
            self.logger.warning("Delete Element Locked "
                                "{0}->{1}".format(dbName, elemName))
            return responseCode.ELEM_IS_LOCKED
        else:
            self.lock("ELEM", dbName, elemName)
            self.elemName[dbName].remove(elemName)
            self.elemDict[dbName].pop(elemName)
            try:
                self.elemTTL[dbName].pop(elemName)
                self.invertedTypeDict[dbName].pop(elemName)
            except:
                pass
            self.elemLockDict[dbName].pop(elemName)
            self.logger.info("Delete Element Success "
                             "{0}->{1}".format(dbName, elemName))
            return responseCode.ELEM_DELETE_SUCCESS

    @keyNameValidity
    @saveTrigger
    def createList(self, dbName, listName):
        self.lock("LIST", dbName, listName)
        self.listName[dbName].add(listName)
        self.listDict[dbName][listName] = list()
        self.invertedTypeDict[dbName][listName] = responseCode.LIST_TYPE
        self.unlock("LIST", dbName, listName)
        self.logger.info("Create List Success "
                         "{0}->{1}".format(dbName, listName))
        return responseCode.LIST_CREATE_SUCCESS

    def getList(self, listName, dbName, start=None, end=None):
        try:
            if start is None and end is None:
                listValue = self.listDict[dbName][listName]
            elif start is not None and end is not None:
                listValue = self.listDict[dbName][listName][start:end]
            elif start is not None and end is None:
                listValue = self.listDict[dbName][listName][start:]
            else:
                listValue = None
        except:
            listValue = None
        self.logger.info("Get List Success "
                         "{0}->{1}".format(dbName, listName))
        return listValue

    def getListRandom(self, dbName, listName, numRand):
        if len(self.listDict[dbName][listName]) < numRand:
            return responseCode.LIST_LENGTH_TOO_SHORT, None
        result = random.sample(self.listDict[dbName][listName], numRand)
        return responseCode.LIST_GET_SUCCESS, result

    @saveTrigger
    def insertList(self, listName, value, dbName, isLeft=None):
        if self.listLockDict[dbName][listName] is True:
            self.logger.warning("Insert List Locked "
                                "{0}->{1}->{2}".format(dbName, listName, value))
            return responseCode.LIST_IS_LOCKED
        else:
            self.lock("LIST", dbName, listName)
            if isLeft is None:
                self.listDict[dbName][listName].append(value)
            else:
                self.listDict[dbName][listName].insert(0, value)
            self.unlock("LIST", dbName, listName)
            self.logger.info("Insert List Success "
                             "{0}->{1}->{2}".format(dbName, listName, value))
            return responseCode.LIST_INSERT_SUCCESS

    @saveTrigger
    def deleteList(self, listName, dbName):
        if self.listLockDict[dbName][listName] is True:
            self.logger.warning("Delete List Locked "
                                "{0}->{1}".format(dbName, listName))
            return responseCode.LIST_IS_LOCKED
        else:
            self.lock("LIST", dbName, listName)
            self.listName[dbName].remove(listName)
            self.listDict[dbName].pop(listName)
            try:
                self.listTTL[dbName].pop(listName)
                self.invertedTypeDict[dbName].pop(listName)
            except:
                pass
            self.unlock("LIST", dbName, listName)
            self.listLockDict[dbName].pop(listName)
            self.logger.info("Delete List Success "
                             "{0}->{1}".format(dbName, listName))
            return responseCode.LIST_DELETE_SUCCESS

    @saveTrigger
    def rmFromList(self, dbName, listName, value):
        if self.listLockDict[dbName][listName] is True:
            self.logger.warning("Insert List Locked "
                                "{0}->{1}->{2}".format(dbName, listName, value))
            return responseCode.LIST_IS_LOCKED
        else:
            if value not in self.listDict[dbName][listName]:
                return responseCode.LIST_NOT_CONTAIN_VALUE
            else:
                self.lock("LIST", dbName, listName)
                self.listDict[dbName][listName].remove(value)
                self.unlock("LIST", dbName, listName)
                self.logger.info("Remove From List Success "
                                 "{0}->{1}->{2}".format(dbName, listName, value))
                return responseCode.LIST_REMOVE_SUCCESS

    def searchAllList(self, dbName):
        if self.isDbExist(dbName) is False:
            return []
        self.logger.info("Search All List Success {0}".format(dbName))
        return list(self.listName[dbName])

    @saveTrigger
    def clearList(self, dbName, listName):
        if self.listLockDict[dbName][listName] is True:
            self.logger.warning("Clear List Locked {0}->{1}")
            return responseCode.LIST_IS_LOCKED
        else:
            self.lock("LIST", dbName, listName)
            self.listDict[dbName][listName] = []
            self.unlock("LIST", dbName, listName)
            self.logger.info("List Clear Success "
                             "{}->{}".format(dbName, listName))
            return responseCode.LIST_CLEAR_SUCCESS

    @saveTrigger
    def mergeLists(self, dbName, listName1, listName2, resultListName=None):
        if resultListName is not None:
            self.createList(dbName, resultListName)
            self.lock("LIST", dbName, resultListName)
            self.listDict[dbName][resultListName].extend(self.listDict[dbName][listName1])
            self.listDict[dbName][resultListName].extend(self.listDict[dbName][listName2])
            self.unlock("LIST", dbName, resultListName)
            self.logger.info("Lists Merge Success "
                             "{} merges {}->{}".
                             format(dbName, listName1, listName2, resultListName))
            return responseCode.LIST_MERGE_SUCCESS, self.listDict[dbName][resultListName]
        else:
            if self.listLockDict[dbName][listName1] is False:
                self.lock("LIST", dbName, listName1)
                self.listDict[dbName][listName1].extend(self.listDict[dbName][listName2])
                self.logger.info("Lists Merge Success "
                                 "{} merges {}->{}".
                                 format(dbName, listName1, listName2, listName1))
                return responseCode.LIST_MERGE_SUCCESS, self.listDict[dbName][listName1]
            else:
                self.logger.info("List Locked "
                                 "{}->{}".format(dbName, listName1))
                return responseCode.LIST_IS_LOCKED, []

    @keyNameValidity
    @saveTrigger
    def createHash(self, dbName, hashName):
        if self.isExist("HASH", dbName, hashName) is False:
            self.hashName[dbName].add(hashName)
            self.lock("HASH", dbName, hashName)
            self.hashDict[dbName][hashName] = dict()
            self.invertedTypeDict[dbName][hashName] = responseCode.HASH_TYPE
            self.unlock("HASH", dbName, hashName)
            self.logger.info("Hash Create Success "
                             "{}->{}".format(dbName, hashName))
            return responseCode.HASH_CREATE_SUCCESS
        else:
            self.logger.warning("Hash Create Fail(Hash Exists) "
                                "{}->{}".format(dbName, hashName))
            return responseCode.HASH_EXISTED

    def getHash(self, dbName, hashName):
        return self.hashDict[dbName][hashName]

    def getHashKeySet(self, dbName, hashName):
        return list(self.hashDict[dbName][hashName].keys())

    def getHashValues(self, dbName, hashName):
        return list(self.hashDict[dbName][hashName].values())

    def getMultipleHashValues(self, dbName, hashName, keyNames):
        result = list()
        for keyName in keyNames:
            try:
                result.append(self.hashDict[dbName][hashName][keyName])
            except:
                result.append(None)
        return result

    @saveTrigger
    def insertHash(self, dbName, hashName, keyName, value):
        if self.hashLockDict[dbName][hashName] is True:
            self.logger.warning("Hash Is Locked "
                                "{}->{}".format(dbName, hashName))
            return responseCode.HASH_IS_LOCKED
        else:
            self.lock("HASH", dbName, hashName)
            self.hashDict[dbName][hashName][keyName] = value
            self.unlock("HASH", dbName, hashName)
            self.logger.info("Hash Insert Success "
                             "{}->{} {}:{}".
                             format(dbName, hashName, keyName, value))
            return responseCode.HASH_INSERT_SUCCESS

    def isKeyExist(self, dbName, hashName, keyName):
        if dbName not in self.dbNameSet:
            return False
        elif hashName not in self.hashDict[dbName].keys():
            return False
        return keyName in list(self.hashDict[dbName][hashName].keys())

    @saveTrigger
    def deleteHash(self, dbName, hashName):
        if self.hashLockDict[dbName][hashName] is True:
            self.logger.warning("Hash Is Locked "
                                "{}->{}".format(dbName, hashName))
            return responseCode.HASH_IS_LOCKED
        else:
            self.lock("HASH", dbName, hashName)
            self.hashDict[dbName].pop(hashName)
            self.hashName[dbName].remove(hashName)
            try:
                self.hashTTL[dbName].pop(hashName)
                self.invertedTypeDict[dbName].pop(hashName)
            except:
                pass
            self.unlock("HASH", dbName, hashName)
            self.hashLockDict[dbName].pop(hashName)
            self.logger.info("Hash Delete Success "
                             "{}->{}".format(dbName, hashName))
            return responseCode.HASH_DELETE_SUCCESS

    @saveTrigger
    def rmFromHash(self, dbName, hashName, keyName):
        if self.hashLockDict[dbName][hashName] is True:
            self.logger.warning("Hash Is Locked "
                                "{}->{}".format(dbName, hashName))
            return responseCode.HASH_IS_LOCKED
        else:
            self.lock("HASH", dbName, hashName)
            self.hashDict[dbName][hashName].pop(keyName)
            self.unlock("HASH", dbName, hashName)
            self.logger.info("Hash Value Remove Success "
                             "{}->{}:{}".format(dbName, hashName, keyName))
            return responseCode.HASH_REMOVE_SUCCESS

    @saveTrigger
    def clearHash(self, dbName, hashName):
        if self.hashLockDict[dbName][hashName] is True:
            self.logger.warning("Hash Is Locked "
                                "{}->{}".format(dbName, hashName))
            return responseCode.HASH_IS_LOCKED
        else:
            self.lock("HASH", dbName, hashName)
            self.hashDict[dbName][hashName].clear()
            self.unlock("HASH", dbName, hashName)
            self.logger.info("Hash Clear Success "
                             "{}->{}".format(dbName, hashName))
            return responseCode.HASH_CLEAR_SUCCESS

    @saveTrigger
    def replaceHash(self, dbName, hashName, hashValue):
        if self.hashLockDict[dbName][hashName] is True:
            self.logger.warning("Hash Is Locked "
                                "{}->{}".format(dbName, hashName))
            return responseCode.HASH_IS_LOCKED
        else:
            self.lock("HASH", dbName, hashName)
            self.hashDict[dbName][hashName] = hashValue
            self.unlock("HASH", dbName, hashName)
            self.logger.info("Hash Replace Success "
                             "{}->{}".format(dbName, hashName))
            return responseCode.HASH_REPLACE_SUCCESS

    @saveTrigger
    def mergeHashs(self, dbName, hashName1, hashName2, resultHashName=None, mergeMode=0):
        if mergeMode == 0:
            baseDictName = hashName1
            otherDictName = hashName2
        else:
            baseDictName = hashName2
            otherDictName = hashName1

        if resultHashName is not None:
            self.createHash(dbName, resultHashName)
            self.lock("HASH", dbName, resultHashName)
            baseKeys = self.hashDict[dbName][baseDictName].keys()
            otherKeys = self.hashDict[dbName][otherDictName].keys()
            self.hashDict[dbName][resultHashName] = self.hashDict[dbName][baseDictName].copy()
            for key in otherKeys:
                if key not in baseKeys:
                    self.hashDict[dbName][resultHashName][key] = self.hashDict[dbName][otherDictName][key]
            self.unlock("HASH", dbName, resultHashName)
            self.logger.info("Hash Merge Success "
                             "{} merges {} -> {}".
                             format(hashName1, hashName2, resultHashName))

        else:
            if self.hashLockDict[dbName][baseDictName] is False:
                self.lock("HASH", dbName, baseDictName)
                baseKeys = self.hashDict[dbName][baseDictName].keys()
                otherKeys = self.hashDict[dbName][otherDictName].keys()
                for key in otherKeys:
                    if key not in baseKeys:
                        self.hashDict[dbName][baseDictName][key] = self.hashDict[dbName][otherDictName][key]
                self.logger.info("Hash Merge Success "
                                 "{} merges {} -> {}".
                                 format(hashName1, hashName2, hashName1))
            else:
                self.logger.warning("Hash Is Locked "
                                    "{}->{} or {}->{}".
                                    format(dbName, hashName1, dbName, hashName2))
                return responseCode.HASH_IS_LOCKED
        return responseCode.HASH_MERGE_SUCCESS

    def searchAllHash(self, dbName):
        if self.isDbExist(dbName) is False:
            return []
        self.logger.info("Search All Hash Success "
                         "{0}".format(dbName))
        return list(self.hashName[dbName])

    @saveTrigger
    def increaseHash(self, dbName, hashName, keyName):
        if isinstance(self.hashDict[dbName][hashName][keyName], int) is False:
            self.logger.warning("Hash Value Type Is Not Integer "
                                "{}->{}:{}".format(dbName, hashName, keyName))
            return responseCode.ELEM_TYPE_ERROR, None

        self.lock("HASH", dbName, hashName)
        self.hashDict[dbName][hashName][keyName] += 1
        self.unlock("HASH", dbName, hashName)
        self.logger.info("Hash Value Increase Success "
                         "{}->{}:{}".format(dbName, hashName, keyName))
        return responseCode.HASH_INCR_SUCCESS, self.hashDict[dbName][hashName][keyName]

    @saveTrigger
    def decreaseHash(self, dbName, hashName, keyName):
        if isinstance(self.hashDict[dbName][hashName][keyName], int) is False:
            self.logger.warning("Hash Value Type Is Not Integer "
                                "{}->{}:{}".format(dbName, hashName, keyName))
            return responseCode.ELEM_TYPE_ERROR, None

        self.lock("HASH", dbName, hashName)
        self.hashDict[dbName][hashName][keyName] -= 1
        self.unlock("HASH", dbName, hashName)
        self.logger.info("Hash Value Decrease Success "
                         "{}->{}:{}".format(dbName, hashName, keyName))
        return responseCode.HASH_DECR_SUCCESS, self.hashDict[dbName][hashName][keyName]

    @keyNameValidity
    @saveTrigger
    def createSet(self, dbName, setName):
        self.lock("SET", dbName, setName)
        self.setName[dbName].add(setName)
        self.setDict[dbName][setName] = set()
        self.invertedTypeDict[dbName][setName] = responseCode.SET_TYPE
        self.unlock("SET", dbName, setName)
        self.logger.info("Set Create Success "
                         "{0}->{1}".format(dbName, setName))
        return responseCode.SET_CREATE_SUCCESS

    def getSet(self, dbName, setName):
        return list(self.setDict[dbName][setName])

    def getSetRandom(self, dbName, setName, numRand):
        if len(self.setDict[dbName][setName]) < numRand:
            return responseCode.SET_LENGTH_TOO_SHORT, None
        result = random.sample(self.setDict[dbName][setName], numRand)
        return responseCode.SET_GET_SUCCESS, result

    @saveTrigger
    def insertSet(self, dbName, setName, setValue):
        if self.setLockDict[dbName][setName] is True:
            self.logger.warning("Set Is Locked "
                                "{0}->{1}".format(dbName, setName))
            return responseCode.SET_IS_LOCKED
        else:
            if setValue not in self.setDict[dbName][setName]:
                self.lock("SET", dbName, setName)
                self.setDict[dbName][setName].add(setValue)
                self.unlock("SET", dbName, setName)
                self.logger.info("Set Insert Success "
                                 "0}->{1}->{2}".format(dbName, setName, setValue))
                return responseCode.SET_INSERT_SUCCESS
            else:
                return responseCode.SET_VALUE_ALREADY_EXIST

    @saveTrigger
    def rmFromSet(self, dbName, setName, setValue):
        if self.setLockDict[dbName][setName] is True:
            self.logger.warning("Set Is Locked "
                                "{0}->{1}".format(dbName, setName))
            return responseCode.SET_IS_LOCKED
        else:
            if setValue in self.setDict[dbName][setName]:
                self.lock("SET", dbName, setName)
                self.setDict[dbName][setName].discard(setValue)
                self.unlock("SET", dbName, setName)
                self.logger.info("Set Remove Success "
                                 "{0}->{1}->{2}".format(dbName, setName, setValue))
                return responseCode.SET_REMOVE_SUCCESS
            else:
                return responseCode.SET_VALUE_NOT_EXIST

    @saveTrigger
    def clearSet(self, dbName, setName):
        if self.setLockDict[dbName][setName] is True:
            self.logger.warning("Set Is Locked "
                                "{0}->{1}".format(dbName, setName))
            return responseCode.SET_IS_LOCKED
        else:
            self.lock("SET", dbName, setName)
            self.setDict[dbName][setName].clear()
            self.unlock("SET", dbName, setName)
            self.logger.info("Set Clear Success "
                             "{0}->{1}".format(dbName, setName))
            return responseCode.SET_CLEAR_SUCCESS

    @saveTrigger
    def deleteSet(self, dbName, setName):
        if self.setLockDict[dbName][setName] is True:
            self.logger.warning("Set Is Locked "
                                "{0}->{1}".format(dbName, setName))
            return responseCode.SET_IS_LOCKED
        else:
            self.lock("SET", dbName, setName)
            self.setName[dbName].discard(setName)
            self.setDict[dbName].pop(setName)
            self.invertedTypeDict[dbName].pop(setName)
            self.unlock("SET", dbName, setName)
            self.setLockDict[dbName].pop(setName)
            self.logger.info("Set Delete Success "
                             "{0}->{1}".format(dbName, setName))
            return responseCode.SET_DELETE_SUCCESS

    def searchAllSet(self, dbName):
        if self.isDbExist(dbName) is False:
            return []
        self.logger.info("Search All Set Success "
                         "{0}".format(dbName))
        return list(self.setName[dbName])

    @saveTrigger
    def unionSet(self, dbName, setName1, setName2, unionResult):
        if (self.setLockDict[dbName][setName1] is True
                or self.setLockDict[dbName][setName2] is True):
            self.logger.warning("Set Is Locked "
                                "{0}->{1} or {2}->{3}".
                                format(dbName, setName1, dbName, setName2))
            return responseCode.SET_IS_LOCKED
        else:
            self.lock("SET", dbName, setName1)
            self.lock("SET", dbName, setName2)
            unionResult.append(list(self.setDict[dbName][setName1].
                                    union(self.setDict[dbName][setName2])))
            self.unlock("SET", dbName, setName1)
            self.unlock("SET", dbName, setName2)
            self.logger.info("Set Union Success "
                             "{}->{} unions {}->{} to {}->{}".
                             format(dbName, setName1, dbName, setName2, dbName, unionResult))
            return responseCode.SET_UNION_SUCCESS

    @saveTrigger
    def intersectSet(self, dbName, setName1, setName2, intersectResult):
        if (self.setLockDict[dbName][setName1] is True
            or self.setLockDict[dbName][setName2] is True):
            self.logger.warning("Set Is Locked "
                                "{0}->{1} or {2}->{3}".
                                format(dbName, setName1, dbName, setName2))
            return responseCode.SET_IS_LOCKED
        else:
            self.lock("SET", dbName, setName1)
            self.lock("SET", dbName, setName2)
            intersectResult.append(list(self.setDict[dbName][setName1].
                                        intersection(self.setDict[dbName][setName2])))
            self.unlock("SET", dbName, setName1)
            self.unlock("SET", dbName, setName2)
            self.logger.info("Set Intersect Success "
                             "{}->{} intersects {}->{} to {}->{}".
                             format(dbName, setName1, dbName, setName2, dbName, intersectResult))
            return responseCode.SET_INTERSECT_SUCCESS

    @saveTrigger
    def diffSet(self, dbName, setName1, setName2, diffResult):
        if (self.setLockDict[dbName][setName1] is True
            or self.setLockDict[dbName][setName2] is True):
            self.logger.warning("Set Is Locked "
                                "{}->{} or {}->{}".
                                format(dbName, setName1, dbName, setName2))
            return responseCode.SET_IS_LOCKED
        else:
            self.lock("SET", dbName, setName1)
            self.lock("SET", dbName, setName2)
            diffResult.append(list(self.setDict[dbName][setName1].
                                   difference(self.setDict[dbName][setName2])))
            self.unlock("SET", dbName, setName1)
            self.unlock("SET", dbName, setName2)
            self.logger.info("Set Diff Success "
                             "{}->{} unions {}->{} to {}->{}".
                             format(dbName, setName1, dbName, setName2, dbName, diffResult))
            return responseCode.SET_DIFF_SUCCESS

    @saveTrigger
    def replaceSet(self, dbName, setName, setValue):
        if self.setLockDict[dbName][setName] is True:
            self.logger.warning("Set Is Locked "
                                "{}->{}".format(dbName, setName))
            return responseCode.SET_IS_LOCKED
        else:
            self.lock("SET", dbName, setName)
            self.setDict[dbName][setName] = setValue
            self.unlock("SET", dbName, setName)
            self.logger.info("Set Replace Success "
                             "{}->{}".format(dbName, setName))
            return responseCode.SET_REPLACE_SUCCESS

    @keyNameValidity
    @saveTrigger
    def createZSet(self, dbName, zsetName):
        self.lock("ZSET", dbName, zsetName)
        self.zsetName[dbName].add(zsetName)
        self.zsetDict[dbName][zsetName] = zset()
        self.invertedTypeDict[dbName][zsetName] = responseCode.ZSET_TYPE
        self.unlock("ZSET", dbName, zsetName)
        self.logger.info("ZSet Create Success "
                         "{0}->{1}".format(dbName, zsetName))
        return responseCode.ZSET_CREATE_SUCCESS

    def getZSet(self, dbName, zsetName):
        return self.zsetDict[dbName][zsetName].get()

    @saveTrigger
    def insertZSet(self, dbName, zsetName, value, score):
        if self.zsetLockDict[dbName][zsetName] is True:
            self.logger.warning("ZSet Is Locked "
                                "{0}->{1}".format(dbName, zsetName))
            return responseCode.ZSET_IS_LOCKED
        else:
            try:
                self.lock("ZSET", dbName, zsetName)
                if self.zsetDict[dbName][zsetName].add(value, score) is True:
                    self.logger.info("ZSet Insert Success "
                                     "{0}->{1}->{2}:{3}".
                                     format(dbName, zsetName, value, score))
                    return responseCode.ZSET_INSERT_SUCCESS
                else:
                    self.logger.warning("ZSet Insert Fail(Value Existed) "
                                        "{}->{}:{}".
                                        format(dbName, zsetName, value))
                    return responseCode.ZSET_VALUE_ALREADY_EXIST
            finally:
                self.unlock("ZSET", dbName, zsetName)

    @saveTrigger
    def rmFromZSet(self, dbName, zsetName, value):
        if self.zsetLockDict[dbName][zsetName] is True:
            self.logger.warning("ZSet Is Locked "
                                "{}->{}".format(dbName, zsetName))
            return responseCode.ZSET_IS_LOCKED
        else:
            self.lock("ZSET", dbName, zsetName)
            result = self.zsetDict[dbName][zsetName].remove(value)
            self.unlock("ZSET", dbName, zsetName)
            if(result is True):
                self.logger.info("ZSet Remove Success "
                                 "{}->{}:{}".
                                 format(dbName, zsetName, value))
            else:
                self.logger.info("ZSet Remove Fail(Value Not Existed) "
                                 "{}->{}:{}".
                                 format(dbName, zsetName, value))
            return responseCode.ZSET_REMOVE_SUCCESS if result is True else responseCode.ZSET_NOT_CONTAIN_VALUE

    @saveTrigger
    def clearZSet(self, dbName, zsetName):
        if self.zsetLockDict[dbName][zsetName] is True:
            self.logger.warning("ZSet Is Locked "
                                "{}->{}".format(dbName, zsetName))
            return responseCode.ZSET_IS_LOCKED
        else:
            self.lock("ZSET", dbName, zsetName)
            self.zsetDict[dbName][zsetName].clear()
            self.unlock("ZSET", dbName, zsetName)
            self.logger.info("ZSet Clear Success "
                             "{}->{}".format(dbName, zsetName))
            return responseCode.ZSET_CLEAR_SUCCESS

    @saveTrigger
    def deleteZSet(self, dbName, zsetName):
        if self.zsetLockDict[dbName][zsetName] is True:
            self.logger.warning("ZSet Is Locked "
                                "{}->{}".format(dbName, zsetName))
            return responseCode.ZSET_IS_LOCKED
        else:
            self.lock("ZSET", dbName, zsetName)
            self.zsetName[dbName].discard(zsetName)
            self.zsetDict[dbName].pop(zsetName)
            self.invertedTypeDict[dbName].pop(zsetName)
            self.unlock("ZSET", dbName, zsetName)
            self.zsetLockDict[dbName].pop(zsetName)
            self.logger.info("ZSet Delete Success "
                             "{}->{}".format(dbName, zsetName))
            return responseCode.ZSET_DELETE_SUCCESS

    def searchAllZSet(self, dbName):
        if self.isDbExist(dbName) is False:
            return []
        self.logger.info("Search All ZSet Success "
                         "{}".format(dbName))
        return list(self.zsetName[dbName])

    def findMinFromZSet(self, dbName, zsetName):
        return self.zsetDict[dbName][zsetName].findMin()

    def findMaxFromZSet(self, dbName, zsetName):
        return self.zsetDict[dbName][zsetName].findMax()

    def getScoreFromZSet(self, dbName, zsetName, valueName):
        result = self.zsetDict[dbName][zsetName].find(valueName)
        return result[1]

    def getValuesByRange(self, dbName, zsetName, start, end):
        traverseResult = self.zsetDict[dbName][zsetName].get()
        traverseResult = [result for result in traverseResult
                          if result[1] >= start and result[1] < end]
        return traverseResult

    def getSize(self, dbName, keyName, type):
        if type == "ZSET":
            return (responseCode.GET_SIZE_SUCCESS, self.zsetDict[dbName][keyName].size())
        data = self.getValueDict(type)[dbName][keyName]
        return (responseCode.GET_SIZE_SUCCESS, len(data))

    def getRank(self, dbName, zsetName, value):
        return self.zsetDict[dbName][zsetName].getRank(value)

    @saveTrigger
    def rmByScore(self, dbName, zsetName, start, end):
        if self.zsetLockDict[dbName][zsetName] is True:
            self.logger.warning("ZSet Is Locked "
                                "{}->{}".format(dbName, zsetName))
            return (responseCode.ZSET_IS_LOCKED, 0)
        else:
            self.lock("ZSET", dbName, zsetName)
            result = self.zsetDict[dbName][zsetName].removeByScore(start, end)
            self.unlock("ZSET", dbName, zsetName)
            self.logger.info("ZSet Remove By Score Success "
                             "{}->{} [{},{})".
                             format(dbName, zsetName, start, end))
            return (responseCode.ZSET_REMOVE_BY_SCORE_SUCCESS, result)

    @saveTrigger
    def addDb(self, dbName):
        if self.saveLock is True:
            self.logger.warning("Database Save Locked "
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
                self.logger.info("Database Add Success "
                                 "{}".format(dbName))
                return responseCode.DB_CREATE_SUCCESS
            else:
                self.logger.warning("Database Already Exists "
                                    "{}".format(dbName))
                return responseCode.DB_EXISTED

    def getAllDatabase(self):
        self.logger.info("Get All Database Names Success")
        return list(self.dbNameSet)

    @saveTrigger
    def delDatabase(self, dbName):
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
                self.logger.info("Database Delete Success "
                                 "{}".format(dbName))
                return responseCode.DB_DELETE_SUCCESS
            else:
                self.logger.warning("Database Delete Fail(Save Locked) "
                                    "{}".format(dbName))
                return responseCode.DB_SAVE_LOCKED
        else:
            self.logger.warning("Database Delete Fail(Not Existed) "
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

    def saveDb(self):
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
            self.logger.info("Database Save Success")
            self.opCount = 0    # once save process is done, reload the op count
            return responseCode.DB_SAVE_SUCCESS

        else:
            self.logger.warning("Database Save Locked")
            return responseCode.DB_SAVE_LOCKED

    def loadZSet(self, dbName, dataFileName, valueFileName, TTLFileName):
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
                self.zsetDict[dbName][zsetName] = zset()
                values = data[zsetName]
                for key in values.keys():
                    self.zsetDict[dbName][zsetName].add(key, values[key])

        # load TTL
        with open("data" + os.sep + dbName + os.sep + TTLFileName, "r") as TTLFile:
            self.zsetTTL[dbName] = json.loads(TTLFile.read())

    def loadData(self, dbName, dataType, dataFileName, valueFileName, TTLFileName):
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

    def loadDb(self):
        try:
            if os.path.exists("data") is False:
                os.mkdir("data")
                for dbName in self.dbNameSet:
                    os.mkdir("data/{}".format(dbName))
            dbNameSet = os.listdir("data")  # find all dbName in the data directory

            for dbName in dbNameSet:
                self.dbNameSet.add(dbName)

                # init element structure
                self.elemName[dbName] = set()
                self.elemLockDict[dbName] = dict()
                self.elemDict[dbName] = dict()

                # init list structure
                self.listName[dbName] = set()
                self.listLockDict[dbName] = dict()
                self.listDict[dbName] = dict()

                # init hash structure
                self.hashName[dbName] = set()
                self.hashLockDict[dbName] = dict()
                self.hashDict[dbName] = dict()

                # init set structure
                self.setName[dbName] = set()
                self.setLockDict[dbName] = dict()
                self.setDict[dbName] = dict()

                # init zset structure
                self.zsetName[dbName] = set()
                self.zsetLockDict[dbName] = dict()
                self.zsetDict[dbName] = dict()

                # load data
                self.loadData(dbName, "ELEM", "elemName.txt", "elemValue.txt", "elemTTL.txt")
                self.loadData(dbName, "LIST", "listName.txt", "listValue.txt", "listTTL.txt")
                self.loadData(dbName, "HASH", "hashName.txt", "hashValue.txt", "hashTTL.txt")
                self.loadData(dbName, "SET", "setName.txt", "setValue.txt", "setTTL.txt")
                self.loadZSet(dbName, "zsetName.txt", "zsetValue.txt", "zsetTTL.txt")

            self.logger.info("Database Load Success")

        except Exception as e:
            self.logger.warning("Database Load Fail {0}".format(str(e)))
