__author__ = 'Ma Haoxiang'

# import
import re
import os
import json
import logging
import time
from response import responseCode

# a decorator for save trigger
def saveTrigger(func):
    def trigger(*args, **kwargs):
        result = func(*args, **kwargs)
        self = args[0]
        self.opCount += 1
        # when opCounts reach the trigger, save automatically
        if(self.opCount == self.saveTrigger):
            self.opCount = 0
            self.saveDb()
            self.logger.info("Auto Save Triggers")
        return result
    return trigger

class NoSqlDb:
    def __init__(self, config):
        self.dbNameSet = {"db0", "db1", "db2", "db3", "db4"}  # initial databases
        self.saveTrigger = config["SAVE_TRIGGER"]
        self.opCount = 0

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

        self.saveLock = False

        # TTL structure
        self.elemTTL = dict()
        self.listTTL = dict()
        self.hashTTL = dict()
        self.setTTL = dict()

        for dbName in self.dbNameSet:
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

            self.elemTTL[dbName] = dict()
            self.listTTL[dbName] = dict()
            self.hashTTL[dbName] = dict()
            self.setTTL[dbName] = dict()

        # check log directory
        if(os.path.exists(config["LOG_PATH"]) is False):
            os.mkdir(config["LOG_PATH"])

        # register a logger
        self.logger = logging.getLogger('dbLogger')
        self.logger.setLevel(logging.INFO)
        hdr = logging.FileHandler(config["LOG_PATH"]+"db.log",mode="a")
        formatter = logging.Formatter('[%(asctime)s] %(name)s:%(levelname)s: %(message)s')
        hdr.setFormatter(formatter)
        self.logger.addHandler(hdr)

        # load data from local file
        self.loadDb()

    def lockElem(self, dbName, elemName):
        self.elemLockDict[dbName][elemName] = True

    def unlockElem(self, dbName, elemName):
        self.elemLockDict[dbName][elemName] = False

    def lockList(self, dbName, listName):
        self.listLockDict[dbName][listName] = True

    def unlockList(self, dbName, listName):
        self.listLockDict[dbName][listName] = False

    def lockHash(self, dbName, hashName):
        self.hashLockDict[dbName][hashName] = True

    def unlockHash(self, dbName, hashName):
        self.hashLockDict[dbName][hashName] = False

    def lockSet(self, dbName, setName):
        self.setLockDict[dbName][setName] = True

    def unlockSet(self, dbName, setName):
        self.setLockDict[dbName][setName] = False

    def isDbExist(self, dbName):
        return dbName in self.dbNameSet

    def isElemExist(self, dbName, elemName):
        if(self.isDbExist(dbName) is True):
            return elemName in self.elemName[dbName]
        else:
            return False

    def isListExist(self, dbName, listName):
        if(self.isDbExist(dbName) is True):
            return listName in self.listName[dbName]
        else:
            return False

    def isHashExist(self, dbName, hashName):
        if(self.isDbExist(dbName) is True):
            return hashName in self.hashName[dbName]
        else:
            return False

    def isSetExist(self, dbName, setName):
        if(self.isDbExist(dbName) is True):
            return setName in self.setName[dbName]
        else:
            return False

    def searchByRE(self, dbName, expression, dataType):
        if(self.isDbExist(dbName) is False):
            return []
        searchResult = set()
        expression = re.sub("\*",".*",expression)   # convert expression to regular expression
        if(dataType == "ELEM"):
            names = self.elemName[dbName]
            logInfo = "Search Element Success {0} {1}"
        elif(dataType == "LIST"):
            names = self.listName[dbName]
            logInfo = "Search LIST Success {0} {1}"
        elif(dataType == "HASH"):
            names = self.hashName[dbName]
            logInfo = "Search Hash Success {0} {1}"
        elif(dataType == "SET"):
            names = self.setName[dbName]
            logInfo = "Search Set Success {0} {1}"
        else:
            names = []
            logInfo = "Search Fail {0} {1}"
        for name in names:
            try:
                if(len(re.findall("({})".format(expression),name)) > 0):
                    searchResult.add(name)
            except:
                continue
        self.logger.info(logInfo.format(dbName, expression))
        return list(searchResult)

    def showTTL(self, dbName, keyName, dataType):
        if(dataType == "ELEM"):
            ttlDict = self.elemTTL[dbName]
        elif(dataType == "LIST"):
            ttlDict = self.listTTL[dbName]
        elif(dataType == "HASH"):
            ttlDict = self.hashTTL[dbName]
        elif(dataType == "SET"):
            ttlDict = self.setTTL[dbName]
        else:
            ttlDict = None

        if(keyName in ttlDict.keys()):
            if(ttlDict[keyName]["status"] == False):
                return responseCode.TTL_EXPIRED
            else:
                curTime = int(time.time())
                ttl = ttlDict[keyName]["ttl"]
                restTime = ttl - (curTime-ttlDict[keyName]["createAt"])
                if(restTime <= 0):
                    ttlDict[keyName]["status"] = False
                    return responseCode.TTL_EXPIRED
                else:
                    return restTime
        else:
            return responseCode.TTL_NO_RECORD

    def isExpired(self, dbName, keyName, dataType):
        if (dataType == "ELEM"):
            ttlDict = self.elemTTL[dbName]
        elif (dataType == "LIST"):
            ttlDict = self.listTTL[dbName]
        elif (dataType == "HASH"):
            ttlDict = self.hashTTL[dbName]
        elif (dataType == "SET"):
            ttlDict = self.setTTL[dbName]
        else:
            ttlDict = None

        curTime = int(time.time())
        if (keyName not in ttlDict.keys()):
            return False
        else:
            if(ttlDict[keyName]["status"] is False):
                return True
            createAt = ttlDict[keyName]["createAt"]
            ttl = ttlDict[keyName]["ttl"]
            if (curTime - createAt >= ttl):
                ttlDict[keyName]["status"] = False
                return True
            else:
                return False

    @saveTrigger
    def createElem(self, elemName, value, dbName):
        self.lockElem(dbName, elemName) # lock this element avoiding r/w implements
        self.elemName[dbName].add(elemName)
        self.elemDict[dbName][elemName] = value
        self.unlockElem(dbName, elemName)
        self.logger.info("Create Element Success {0}->{1}->{2}".format(dbName, elemName, value))
        return responseCode.ELEM_CREATE_SUCCESS

    def updateElem(self, elemName, value, dbName):
        if self.elemLockDict[dbName][elemName] is True: # element is locked
            self.logger.warning("Update Element Locked {0}->{1}->{2}".format(dbName, elemName, value))
            return responseCode.ELEM_IS_LOCKED

        else:   # update the value
            self.lockElem(dbName, elemName)
            self.elemDict[dbName][elemName] = value
            self.unlockElem(dbName, elemName)
            self.logger.info("Update Element Success {0}->{1}->{2}".format(dbName, elemName, value))
            return responseCode.ELEM_UPDATE_SUCCESS

    def getElem(self, elemName, dbName):
        try:
            elemValue = self.elemDict[dbName][elemName]
        except:
            elemValue = None
        self.logger.info("Get Element Success {0}->{1}".format(dbName, elemName))
        return elemValue

    def searchAllElem(self, dbName):
        if(self.isDbExist(dbName) is False):
            return []
        self.logger.info("Search All Elements in {0}".format(dbName))
        return list(self.elemName[dbName])

    @saveTrigger
    def increaseElem(self, elemName, dbName):
        if(self.elemLockDict[dbName][elemName] is True): # element is locked
            self.logger.warning("Increase Element Locked {0}->{1}".format(dbName, elemName))
            return responseCode.ELEM_IS_LOCKED
        else:
            self.lockElem(dbName, elemName)
            self.elemDict[dbName][elemName] += 1
            self.unlockElem(dbName, elemName)
            self.logger.info("Increase Element Success {0}->{1}".format(dbName, elemName))
            return responseCode.ELEM_INCR_SUCCESS

    @saveTrigger
    def decreaseElem(self, elemName, dbName):
        if(self.elemLockDict[dbName][elemName] is True): # element is locked
            self.logger.warning("Decrease Element Locked {0}->{1}".format(dbName, elemName))
            return responseCode.ELEM_IS_LOCKED
        else:
            self.lockElem(dbName, elemName)
            self.elemDict[dbName][elemName] -= 1
            self.unlockElem(dbName, elemName)
            self.logger.info("Decrease Element Success {0}->{1}".format(dbName, elemName))
            return responseCode.ELEM_DECR_SUCCESS

    @saveTrigger
    def deleteElem(self, elemName, dbName):
        if (self.elemLockDict[dbName][elemName] is True):  # element is locked
            self.logger.warning("Delete Element Locked {0}->{1}".format(dbName, elemName))
            return responseCode.ELEM_IS_LOCKED
        else:
            self.lockElem(dbName, elemName)
            self.elemName[dbName].remove(elemName)
            self.elemDict[dbName].pop(elemName)
            try:
                self.elemTTL[dbName].pop(elemName)
            except:
                pass
            self.elemLockDict[dbName].pop(elemName)
            self.logger.info("Delete Element Success {0}->{1}".format(dbName, elemName))
            return responseCode.ELEM_DELETE_SUCCESS

    @saveTrigger
    def setElemTTL(self, dbName, elemName, ttl):
        if(self.elemLockDict[dbName][elemName] is True):
            self.logger.warning("Element Locked {0}->{1}".format(dbName, elemName))
            return responseCode.ELEM_IS_LOCKED
        else:
            self.lockElem(dbName, elemName)
            self.elemTTL[dbName][elemName] = {"createAt":int(time.time()),
                                              "ttl":int(ttl),
                                              "status":True}
            self.unlockElem(dbName, elemName)
            return responseCode.ELEM_TTL_SET_SUCCESS

    @saveTrigger
    def clearElemTTL(self, dbName, elemName):
        if (self.elemLockDict[dbName][elemName] is True):
            self.logger.warning("Element Locked {0}->{1}".format(dbName, elemName))
            return responseCode.ELEM_IS_LOCKED
        else:
            self.lockElem(dbName, elemName)
            try:
                self.elemTTL[dbName].pop(elemName)
            except:
                pass
            self.unlockElem(dbName, elemName)
            return responseCode.ELEM_TTL_CLEAR_SUCCESS

    @saveTrigger
    def createList(self, listName, dbName):
        self.lockList(dbName, listName)
        self.listName[dbName].add(listName)
        self.listDict[dbName][listName] = list()
        self.unlockList(dbName, listName)
        self.logger.info("Create List Success {0}->{1}".format(dbName, listName))
        return responseCode.LIST_CREATE_SUCCESS

    def getList(self, listName, dbName):
        try:
            listValue = self.listDict[dbName][listName]
        except:
            listValue = None
        self.logger.info("Get List Success {0}->{1}".format(dbName, listName))
        return listValue

    @saveTrigger
    def insertList(self, listName, value, dbName):
        if(self.listLockDict[dbName][listName] is True):
            self.logger.warning("Insert List Locked {0}->{1}->{2}".format(dbName, listName, value))
            return responseCode.LIST_IS_LOCKED
        else:
            self.lockList(dbName, listName)
            self.listDict[dbName][listName].append(value)
            self.unlockList(dbName, listName)
            self.logger.info("Insert List Success {0}->{1}->{2}".format(dbName, listName, value))
            return responseCode.LIST_INSERT_SUCCESS

    @saveTrigger
    def deleteList(self, listName, dbName):
        if(self.listLockDict[dbName][listName] is True):
            self.logger.warning("Delete List Locked {0}->{1}".format(dbName, listName))
            return responseCode.LIST_IS_LOCKED
        else:
            self.lockList(dbName, listName)
            self.listName[dbName].remove(listName)
            self.listDict[dbName].pop(listName)
            try:
                self.listTTL[dbName].pop(listName)
            except:
                pass
            self.unlockList(dbName, listName)
            self.listLockDict[dbName].pop(listName)
            self.logger.info("Delete List Success {0}->{1}".format(dbName, listName))
            return responseCode.LIST_DELETE_SUCCESS

    @saveTrigger
    def rmFromList(self, dbName, listName, value):
        if (self.listLockDict[dbName][listName] is True):
            self.logger.warning("Insert List Locked {0}->{1}->{2}".format(dbName, listName, value))
            return responseCode.LIST_IS_LOCKED
        else:
            if(value not in self.listDict[dbName][listName]):
                return responseCode.LIST_NOT_CONTAIN_VALUE
            else:
                self.lockList(dbName, listName)
                self.listDict[dbName][listName].remove(value)
                self.unlockList(dbName, listName)
                self.logger.info("Remove From List Success {0}->{1}->{2}".format(dbName, listName, value))
                return responseCode.LIST_REMOVE_SUCCESS

    def searchAllList(self, dbName):
        if(self.isDbExist(dbName) is False):
            return []
        self.logger.info("Search All List Success {0}".format(dbName))
        return list(self.listName[dbName])

    @saveTrigger
    def clearList(self, dbName, listName):
        if(self.listLockDict[dbName][listName] is True):
            self.logger.warning("Clear List Locked {0}->{1}")
            return responseCode.LIST_IS_LOCKED
        else:
            self.lockList(dbName, listName)
            self.listDict[dbName][listName] = []
            self.unlockList(dbName, listName)
            return responseCode.LIST_CLEAR_SUCCESS

    @saveTrigger
    def mergeLists(self, dbName, listName1, listName2, resultListName=None):
        if(resultListName is not None):
            self.createList(resultListName, dbName)
            self.lockList(dbName, resultListName)
            self.listDict[dbName][resultListName].extend(self.listDict[dbName][listName1])
            self.listDict[dbName][resultListName].extend(self.listDict[dbName][listName2])
            self.unlockList(dbName, resultListName)
        else:
            if(self.listLockDict[dbName][listName1] is False):
                self.lockList(dbName, listName1)
                self.listDict[dbName][listName1].extend(self.listDict[dbName][listName2])
            else:
                return responseCode.LIST_IS_LOCKED
        return responseCode.LIST_MERGE_SUCCESS

    @saveTrigger
    def setListTTL(self, dbName, listName, ttl):
        if(self.listLockDict[dbName][listName] is True):
            self.logger.warning("List Locked {0}->{1}".format(dbName, listName))
            return responseCode.LIST_IS_LOCKED
        else:
            self.lockList(dbName, listName)
            self.listTTL[dbName][listName] = {"createAt":int(time.time()),
                                              "ttl":int(ttl),
                                              "status":True}
            self.unlockList(dbName, listName)
            return responseCode.LIST_TTL_SET_SUCCESS

    @saveTrigger
    def clearListTTL(self, dbName, listName):
        if (self.listLockDict[dbName][listName] is True):
            self.logger.warning("List Locked {0}->{1}".format(dbName, listName))
            return responseCode.LIST_IS_LOCKED
        else:
            self.lockList(dbName, listName)
            try:
                self.listTTL[dbName].pop(listName)
            except:
                pass
            self.unlockList(dbName, listName)
            return responseCode.LIST_TTL_CLEAR_SUCCESS

    @saveTrigger
    def createHash(self, dbName, hashName):
        if(self.isHashExist(dbName,hashName) is False):
            self.hashName[dbName].add(hashName)
            self.lockHash(dbName, hashName)
            self.hashDict[dbName][hashName] = dict()
            self.unlockHash(dbName, hashName)
            return responseCode.HASH_CREATE_SUCCESS
        else:
            return responseCode.HASH_EXISTED

    def getHash(self, dbName, hashName):
        return self.hashDict[dbName][hashName]

    @saveTrigger
    def insertHash(self, dbName, hashName, keyName, value):
        if(self.hashLockDict[dbName][hashName] is True):
            return responseCode.HASH_IS_LOCKED
        else:
            self.lockHash(dbName, hashName)
            self.hashDict[dbName][hashName][keyName] = value
            self.unlockHash(dbName, hashName)
            return responseCode.HASH_INSERT_SUCCESS

    def isKeyExist(self, dbName, hashName, keyName):
        return keyName in list(self.hashDict[dbName][hashName].keys())

    @saveTrigger
    def deleteHash(self, dbName, hashName):
        if(self.hashLockDict[dbName][hashName] is True):
            return responseCode.HASH_IS_LOCKED
        else:
            self.lockHash(dbName, hashName)
            self.hashDict[dbName].pop(hashName)
            self.hashName[dbName].remove(hashName)
            try:
                self.hashTTL[dbName].pop(hashName)
            except:
                pass
            self.unlockHash(dbName, hashName)
            self.hashLockDict[dbName].pop(hashName)
            return responseCode.HASH_DELETE_SUCCESS

    @saveTrigger
    def rmFromHash(self, dbName, hashName, keyName):
        if(self.hashLockDict[dbName][hashName] is True):
            return responseCode.HASH_IS_LOCKED
        else:
            self.lockHash(dbName, hashName)
            self.hashDict[dbName][hashName].pop(keyName)
            self.unlockHash(dbName, hashName)
            return responseCode.HASH_REMOVE_SUCCESS

    @saveTrigger
    def clearHash(self, dbName, hashName):
        if(self.hashLockDict[dbName][hashName] is True):
            return responseCode.HASH_IS_LOCKED
        else:
            self.lockHash(dbName, hashName)
            self.hashDict[dbName][hashName].clear()
            self.unlockHash(dbName, hashName)
            return responseCode.HASH_CLEAR_SUCCESS

    @saveTrigger
    def replaceHash(self, dbName, hashName, hashValue):
        if(self.hashLockDict[dbName][hashName] is True):
            return responseCode.HASH_IS_LOCKED
        else:
            self.lockHash(dbName, hashName)
            self.hashDict[dbName][hashName] = hashValue
            self.unlockHash(dbName, hashName)
            return responseCode.HASH_REPLACE_SUCCESS

    @saveTrigger
    def mergeHashs(self, dbName, hashName1, hashName2, resultHashName=None, mergeMode=0):
        if (mergeMode == 0):
            baseDictName = hashName1
            otherDictName = hashName2
        else:
            baseDictName = hashName2
            otherDictName = hashName1

        if (resultHashName is not None):
            self.createHash(dbName, resultHashName)
            self.lockHash(dbName, resultHashName)
            baseKeys = self.hashDict[dbName][baseDictName].keys()
            otherKeys = self.hashDict[dbName][otherDictName].keys()
            self.hashDict[dbName][resultHashName] = self.hashDict[dbName][baseDictName].copy()
            for key in otherKeys:
                if(key not in baseKeys):
                    self.hashDict[dbName][resultHashName][key] = self.hashDict[dbName][otherDictName][key]
            self.unlockHash(dbName, resultHashName)

        else:
            if (self.hashLockDict[dbName][baseDictName] is False):
                self.lockHash(dbName, baseDictName)
                baseKeys = self.hashDict[dbName][baseDictName].keys()
                otherKeys = self.hashDict[dbName][otherDictName].keys()
                for key in otherKeys:
                    if(key not in baseKeys):
                        self.hashDict[dbName][baseDictName][key] = self.hashDict[dbName][otherDictName][key]
            else:
                return responseCode.HASH_IS_LOCKED
        return responseCode.HASH_MERGE_SUCCESS

    def searchAllHash(self, dbName):
        if(self.isDbExist(dbName) is False):
            return []
        self.logger.info("Search All Hash Success {0}".format(dbName))
        return list(self.hashName[dbName])

    @saveTrigger
    def setHashTTL(self, dbName, hashName, ttl):
        if(self.hashLockDict[dbName][hashName] is True):
            self.logger.warning("Hash Locked {0}->{1}".format(dbName, hashName))
            return responseCode.HASH_IS_LOCKED
        else:
            self.lockHash(dbName, hashName)
            self.hashTTL[dbName][hashName] = {"createAt":int(time.time()),
                                              "ttl":int(ttl),
                                              "status":True}
            self.unlockHash(dbName, hashName)
            return responseCode.HASH_TTL_SET_SUCCESS

    @saveTrigger
    def clearHashTTL(self, dbName, hashName):
        if (self.hashLockDict[dbName][hashName] is True):
            self.logger.warning("Hash Locked {0}->{1}".format(dbName, hashName))
            return responseCode.HASH_IS_LOCKED
        else:
            self.lockHash(dbName, hashName)
            try:
                self.hashTTL[dbName].pop(hashName)
            except:
                pass
            self.unlockHash(dbName, hashName)
            return responseCode.HASH_TTL_CLEAR_SUCCESS

    @saveTrigger
    def createSet(self, dbName, setName):
        self.lockSet(dbName, setName)
        self.setName[dbName].add(setName)
        self.setDict[dbName][setName] = set()
        self.unlockSet(dbName, setName)
        self.logger.info("Set Create Success {0}->{1}".format(dbName, setName))
        return responseCode.SET_CREATE_SUCCESS

    def getSet(self, dbName, setName):
        return list(self.setDict[dbName][setName])

    @saveTrigger
    def insertSet(self, dbName, setName, setValue):
        if(self.setLockDict[dbName][setName] is True):
            self.logger.warning("Set Is Locked {0}->{1}".format(dbName, setName))
            return responseCode.SET_IS_LOCKED
        else:
            if(setValue not in self.setDict[dbName][setName]):
                self.lockSet(dbName, setName)
                self.setDict[dbName][setName].add(setValue)
                self.unlockSet(dbName, setName)
                self.logger.info("Set Insert Success {0}->{1}->{2}".format(dbName, setName, setValue))
                return responseCode.SET_INSERT_SUCCESS
            else:
                return responseCode.SET_VALUE_ALREADY_EXIST

    @saveTrigger
    def rmFromSet(self, dbName, setName, setValue):
        if(self.setLockDict[dbName][setName] is True):
            self.logger.warning("Set Is Locked {0}->{1}".format(dbName, setName))
            return responseCode.SET_IS_LOCKED
        else:
            if(setValue in self.setDict[dbName][setName]):
                self.lockSet(dbName, setName)
                self.setDict[dbName][setName].discard(setValue)
                self.unlockSet(dbName, setName)
                self.logger.info("Set Remove Success {0}->{1}->{2}".format(dbName, setName, setValue))
                return responseCode.SET_REMOVE_SUCCESS
            else:
                return responseCode.SET_VALUE_NOT_EXIST

    @saveTrigger
    def clearSet(self, dbName, setName):
        if(self.setLockDict[dbName][setName] is True):
            self.logger.warning("Set Is Locked {0}->{1}".format(dbName, setName))
            return responseCode.SET_IS_LOCKED
        else:
            self.lockSet(dbName, setName)
            self.setDict[dbName][setName].clear()
            self.unlockSet(dbName, setName)
            self.logger.info("Set Clear Success {0}->{1}".format(dbName, setName))
            return responseCode.SET_CLEAR_SUCCESS

    @saveTrigger
    def deleteSet(self, dbName, setName):
        if (self.setLockDict[dbName][setName] is True):
            self.logger.warning("Set Is Locked {0}->{1}".format(dbName, setName))
            return responseCode.SET_IS_LOCKED
        else:
            self.lockSet(dbName, setName)
            self.setName[dbName].discard(setName)
            self.setDict[dbName].pop(setName)
            self.unlockSet(dbName, setName)
            self.setLockDict[dbName].pop(setName)
            self.logger.info("Set Delete Success {0}->{1}".format(dbName, setName))
            return responseCode.SET_DELETE_SUCCESS

    def searchAllSet(self, dbName):
        if(self.isDbExist(dbName) is False):
            return []
        self.logger.info("Search All Set Success {0}".format(dbName))
        return list(self.setName[dbName])

    @saveTrigger
    def unionSet(self, dbName, setName1, setName2, unionResult):
        if(self.setLockDict[dbName][setName1] is True
           or self.setLockDict[dbName][setName2] is True):
            self.logger.warning("Set Is Locked {0}->{1} or {2}->{3}".format(dbName, setName1, dbName, setName2))
            return responseCode.SET_IS_LOCKED
        else:
            self.lockSet(dbName, setName1)
            self.lockSet(dbName, setName2)
            unionResult.append(list(self.setDict[dbName][setName1].union(self.setDict[dbName][setName2])))
            self.unlockSet(dbName, setName1)
            self.unlockSet(dbName, setName2)
            return responseCode.SET_UNION_SUCCESS

    @saveTrigger
    def intersectSet(self, dbName, setName1, setName2, intersectResult):
        if (self.setLockDict[dbName][setName1] is True
            or self.setLockDict[dbName][setName2] is True):
            self.logger.warning("Set Is Locked {0}->{1} or {2}->{3}".format(dbName, setName1, dbName, setName2))
            return responseCode.SET_IS_LOCKED
        else:
            self.lockSet(dbName, setName1)
            self.lockSet(dbName, setName2)
            intersectResult.append(list(self.setDict[dbName][setName1].intersection(self.setDict[dbName][setName2])))
            self.unlockSet(dbName, setName1)
            self.unlockSet(dbName, setName2)
            return responseCode.SET_INTERSECT_SUCCESS

    @saveTrigger
    def diffSet(self, dbName, setName1, setName2, diffResult):
        if (self.setLockDict[dbName][setName1] is True
            or self.setLockDict[dbName][setName2] is True):
            self.logger.warning("Set Is Locked {0}->{1} or {2}->{3}".format(dbName, setName1, dbName, setName2))
            return responseCode.SET_IS_LOCKED
        else:
            self.lockSet(dbName, setName1)
            self.lockSet(dbName, setName2)
            diffResult.append(list(self.setDict[dbName][setName1].difference(self.setDict[dbName][setName2])))
            self.unlockSet(dbName, setName1)
            self.unlockSet(dbName, setName2)
            return responseCode.SET_DIFF_SUCCESS

    @saveTrigger
    def replaceSet(self, dbName, setName, setValue):
        if (self.setLockDict[dbName][setName] is True):
            return responseCode.SET_IS_LOCKED
        else:
            self.lockSet(dbName, setName)
            self.setDict[dbName][setName] = setValue
            self.unlockSet(dbName, setName)
            return responseCode.SET_REPLACE_SUCCESS

    @saveTrigger
    def setSetTTL(self, dbName, setName, ttl):
        if (self.setLockDict[dbName][setName] is True):
            self.logger.warning("Set Locked {0}->{1}".format(dbName, setName))
            return responseCode.SET_IS_LOCKED
        else:
            self.lockSet(dbName, setName)
            self.setTTL[dbName][setName] = {"createAt": int(time.time()),
                                              "ttl": int(ttl),
                                              "status": True}
            self.unlockSet(dbName, setName)
            return responseCode.SET_TTL_SET_SUCCESS

    @saveTrigger
    def clearSetTTL(self, dbName, setName):
        if (self.setLockDict[dbName][setName] is True):
            self.logger.warning("Set Locked {0}->{1}".format(dbName, setName))
            return responseCode.SET_IS_LOCKED
        else:
            self.lockSet(dbName, setName)
            try:
                self.setTTL[dbName].pop(setName)
            except:
                pass
            self.unlockSet(dbName, setName)
            return responseCode.SET_TTL_CLEAR_SUCCESS

    @saveTrigger
    def addDb(self, dbName):
        if(self.saveLock is True):
            self.logger.warning("Database Save Locked {0}".format(dbName))
            return responseCode.DB_SAVE_LOCKED
        else:
            if(dbName not in self.dbNameSet):
                self.dbNameSet.add(dbName)
                self.elemName[dbName] = set()
                self.elemDict[dbName] = dict()
                self.elemLockDict[dbName] = dict()
                self.listName[dbName] = set()
                self.listDict[dbName] = dict()
                self.elemLockDict[dbName] = dict()
                self.logger.info("Database Add Success {0}".format(dbName))
                return responseCode.DB_CREATE_SUCCESS
            else:
                self.logger.warning("Database Already Exists {0}".format(dbName))
                return responseCode.DB_EXISTED

    def getAllDatabase(self):
        self.logger.info("Get All Database Names Success")
        return list(self.dbNameSet)

    @saveTrigger
    def delDatabase(self, dbName):
        if(self.isDbExist(dbName) is True):
            if(self.saveLock is False):
                self.saveLock = True
                self.dbNameSet.remove(dbName)
                for root, dirs, files in os.walk("data"+os.sep+dbName, topdown=False):
                    for name in files:
                        os.remove(os.path.join(root, name))
                    for name in dirs:
                        os.rmdir(os.path.join(root, name))
                os.rmdir("data"+os.sep+dbName)
                self.saveLock = False
                return responseCode.DB_DELETE_SUCCESS
            else:
                return responseCode.DB_SAVE_LOCKED
        else:
            return responseCode.DB_NOT_EXIST

    def saveDb(self):
        if(self.saveLock is False):
            # check if the data directory exists
            if(os.path.exists("./data/") is False):
                os.makedirs("data")

            for dbName in self.dbNameSet:
                if(os.path.exists("data{}{}".format(os.sep,dbName)) is False):
                    os.makedirs("data{}{}".format(os.sep,dbName))

            self.saveLock = True
            for dbName in self.dbNameSet:
                # save elements of each db
                with open("data" + os.sep + dbName + os.sep + "elemName.txt", "w") as elemNameFile:
                    elemNameFile.write(json.dumps(list(self.elemName[dbName])))
                with open("data" + os.sep + dbName + os.sep + "elemValue.txt", "w") as elemValueFile:
                    elemValueFile.write(json.dumps(self.elemDict[dbName]))
                with open("data" + os.sep + dbName + os.sep + "elemTTL.txt", "w") as elemTTLFile:
                    elemTTLFile.write(json.dumps(self.elemTTL[dbName]))
                with open("data" + os.sep + dbName + os.sep + "listName.txt", "w") as listNameFile:
                    listNameFile.write(json.dumps(list(self.listName[dbName])))
                with open("data" + os.sep + dbName + os.sep + "listValue.txt", "w") as listValueFile:
                    listValueFile.write(json.dumps(self.listDict[dbName]))
                with open("data" + os.sep + dbName + os.sep + "listTTL.txt", "w") as listTTLFile:
                    listTTLFile.write(json.dumps(self.listTTL[dbName]))
                with open("data" + os.sep + dbName + os.sep + "hashName.txt", "w") as hashNameFile:
                    hashNameFile.write(json.dumps(list(self.hashName[dbName])))
                with open("data" + os.sep + dbName + os.sep + "hashValue.txt", "w") as hashValueFile:
                    hashValueFile.write(json.dumps(self.hashDict[dbName]))
                with open("data" + os.sep + dbName + os.sep + "hashTTL.txt", "w") as hashTTLFile:
                    hashTTLFile.write(json.dumps(self.hashTTL[dbName]))
                with open("data" + os.sep + dbName + os.sep + "setName.txt", "w") as setNameFile:
                    setNameFile.write(json.dumps(list(self.setName[dbName])))
                with open("data" + os.sep + dbName + os.sep + "setValue.txt", "w") as setValueFile:
                    setValue = self.setDict[dbName].copy()
                    for key in setValue.keys():
                        setValue[key] = list(setValue[key])
                    setValueFile.write(json.dumps(setValue))
                with open("data" + os.sep + dbName + os.sep + "setTTL.txt", "w") as setTTLFile:
                    setTTLFile.write(json.dumps(self.setTTL[dbName]))

            self.saveLock = False
            self.logger.info("Database Save Success")
            self.opCount = 0    # once save process is done, reload the op count
            return responseCode.DB_SAVE_SUCCESS

        else:
            self.logger.warning("Database Save Locked")
            return responseCode.DB_SAVE_LOCKED

    def loadDb(self):
        try:
            if(os.path.exists("data") is False):
                os.mkdir("data")
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

                # load element names
                with open("data"+os.sep+dbName+os.sep+"elemName.txt","r") as elemNameFile:
                    elemNames = json.loads(elemNameFile.read())
                    for elemName in elemNames:
                        self.elemName[dbName].add(elemName)
                        self.elemLockDict[dbName][elemName] = False
                # load element values
                with open("data" + os.sep + dbName + os.sep + "elemValue.txt", "r") as elemValueFile:
                    self.elemDict[dbName] = json.loads(elemValueFile.read())
                # load element TTL
                with open("data" + os.sep + dbName + os.sep + "elemTTL.txt", "r") as elemTTLFile:
                    self.elemTTL[dbName] = json.loads(elemTTLFile.read())

                # load list names
                with open("data"+os.sep+dbName+os.sep+"listName.txt","r") as listNameFile:
                    listNames = json.loads(listNameFile.read())
                    for listName in listNames:
                        self.listName[dbName].add(listName)
                        self.listLockDict[dbName][listName] = False
                # load list values
                with open("data" + os.sep + dbName + os.sep + "listValue.txt", "r") as listValueFile:
                    self.listDict[dbName] = json.loads(listValueFile.read())
                # load list TTL
                with open("data" + os.sep + dbName + os.sep + "listTTL.txt", "r") as listTTLFile:
                    self.listTTL[dbName] = json.loads(listTTLFile.read())

                # load hash names
                with open("data"+os.sep+dbName+os.sep+"hashName.txt","r") as hashNameFile:
                    hashNames = json.loads(hashNameFile.read())
                    for hashName in hashNames:
                        self.hashName[dbName].add(hashName)
                        self.hashLockDict[dbName][hashName] = False
                # load hash values
                with open("data" + os.sep + dbName + os.sep + "hashValue.txt", "r") as hashValueFile:
                    self.hashDict[dbName] = json.loads(hashValueFile.read())
                # load hash TTL
                with open("data" + os.sep + dbName + os.sep + "hashTTL.txt", "r") as hashTTLFile:
                    self.hashTTL[dbName] = json.loads(hashTTLFile.read())

                # load set names
                with open("data" + os.sep + dbName + os.sep + "setName.txt", "r") as setNameFile:
                    setNames = json.loads(setNameFile.read())
                    for setName in setNames:
                        self.setName[dbName].add(setName)
                        self.setLockDict[dbName][setName] = False
                # load set values
                with open("data" + os.sep + dbName + os.sep + "setValue.txt", "r") as setValueFile:
                    setValue = json.loads(setValueFile.read())
                    for key in setValue.keys():
                        setValue[key] = set(setValue[key])
                    self.setDict[dbName] = setValue
                # load set TTL
                with open("data" + os.sep + dbName + os.sep + "setTTL.txt", "r") as setTTLFile:
                    self.setTTL[dbName] = json.loads(setTTLFile.read())

            self.logger.info("Database Load Success")

        except Exception as e:
            self.logger.warning("Database Load Fail {0}".format(str(e)))
