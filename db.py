__author__ = 'Ma Haoxiang'

# import
import re
import os
import json
import logging
import time

# a decorator for save trigger
def saveTrigger(func):
    def trigger(*args, **kwargs):
        func(*args, **kwargs)
        self = args[0]
        self.opCount += 1
        if(self.opCount == self.saveTrigger):
            self.opCount = 0
            self.saveDb()
            self.logger.info("Auto Save Triggers")
        return func(*args)
    return trigger

class NoSqlDb:

    ELEM_LOCKED = 0
    ELEM_UNLOCKED = 1
    ELEM_CREATE_SUCCESS = 2
    ELEM_UPDATE_SUCCESS = 3
    ELEM_INCREASE_SUCCESS = 4
    ELEM_DECREASE_SUCCESS = 5
    DB_SAVE_LOCK = 6
    DB_SAVE_SUCCESS = 7
    ELEM_DELETE_SUCCESS = 8
    DB_CREATE_SUCCESS = 9
    DB_EXISTED = 10
    LIST_CREATE_SUCCESS = 11
    LIST_LOCKED = 12
    LIST_INSERT_SUCCESS = 13
    LIST_REMOVE_SUCCESS = 14
    LIST_NOT_CONTAIN_VALUE = 15
    LIST_DELETE_SUCCESS = 16
    DB_DELETE_SUCCESS = 17
    DB_NOT_EXISTED = 18
    HASH_CREATE_SUCCESS = 19
    HASH_EXISTED = 20
    HASH_LOCKED = 21
    HASH_INSERT_SUCCESS = 22
    HASH_DELETE_SUCCESS = 23
    HASH_REMOVE_SUCCESS = 24
    HASH_CLEAR_SUCCESS = 25
    HASH_REPLACE_SUCCESS = 26
    LIST_CLEAR_SUCCESS = 27
    LIST_MERGE_SUCCESS = 28
    HASH_MERGE_SUCCESS = 29
    ELEM_TTL_SET_SUCCESS = 30
    ELEM_TTL_CLEAR_SUCCESS = 31
    LIST_TTL_SET_SUCCESS = 32
    LIST_TTL_CLEAR_SUCCESS = 33
    HASH_TTL_SET_SUCCESS = 34
    HASH_TTL_CLEAR_SUCCESS = 35
    SET_CREATE_SUCCESS = 36
    SET_LOCKED = 37
    SET_VALUE_ALREADY_EXIST = 38
    SET_INSERT_SUCCESS = 39
    SET_VALUE_NOT_EXISTED = 40
    SET_REMOVE_SUCCESS = 41
    SET_CLEAR_SUCCESS = 42
    SET_DELETE_SUCCESS = 43

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
                searchResult.add(re.findall("({})".format(expression),name)[0])
            except:
                continue
        self.logger.info(logInfo.format(dbName, expression))
        return list(searchResult)

    @saveTrigger
    def createElem(self, elemName, value, dbName):
        self.lockElem(dbName, elemName) # lock this element avoiding r/w implements
        self.elemName[dbName].add(elemName)
        self.elemDict[dbName][elemName] = value
        self.unlockElem(dbName, elemName)
        self.logger.info("Create Element Success {0}->{1}->{2}".format(dbName, elemName, value))
        return NoSqlDb.ELEM_CREATE_SUCCESS

    def updateElem(self, elemName, value, dbName):
        if self.elemLockDict[dbName][elemName] is True: # element is locked
            self.logger.warning("Update Element Locked {0}->{1}->{2}".format(dbName, elemName, value))
            return NoSqlDb.ELEM_LOCKED

        else:   # update the value
            self.lockElem(dbName, elemName)
            self.elemDict[dbName][elemName] = value
            self.unlockElem(dbName, elemName)
            self.logger.info("Update Element Success {0}->{1}->{2}".format(dbName, elemName, value))
            return NoSqlDb.ELEM_UPDATE_SUCCESS

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
            return NoSqlDb.ELEM_LOCKED
        else:
            self.lockElem(dbName, elemName)
            self.elemDict[dbName][elemName] += 1
            self.unlockElem(dbName, elemName)
            self.logger.info("Increase Element Success {0}->{1}".format(dbName, elemName))
            return NoSqlDb.ELEM_INCREASE_SUCCESS

    @saveTrigger
    def decreaseElem(self, elemName, dbName):
        if(self.elemLockDict[dbName][elemName] is True): # element is locked
            self.logger.warning("Decrease Element Locked {0}->{1}".format(dbName, elemName))
            return NoSqlDb.ELEM_LOCKED
        else:
            self.lockElem(dbName, elemName)
            self.elemDict[dbName][elemName] -= 1
            self.unlockElem(dbName, elemName)
            self.logger.info("Decrease Element Success {0}->{1}".format(dbName, elemName))
            return NoSqlDb.ELEM_DECREASE_SUCCESS

    @saveTrigger
    def deleteElem(self, elemName, dbName):
        if (self.elemLockDict[dbName][elemName] is True):  # element is locked
            self.logger.warning("Delete Element Locked {0}->{1}".format(dbName, elemName))
            return NoSqlDb.ELEM_LOCKED
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
            return NoSqlDb.ELEM_DELETE_SUCCESS

    @saveTrigger
    def setElemTTL(self, dbName, elemName, ttl):
        if(self.elemLockDict[dbName][elemName] is True):
            self.logger.warning("Element Locked {0}->{1}".format(dbName, elemName))
            return NoSqlDb.ELEM_LOCKED
        else:
            self.lockElem(dbName, elemName)
            self.elemTTL[dbName][elemName] = {"createAt":int(time.time()),
                                              "ttl":int(ttl),
                                              "status":True}
            self.unlockElem(dbName, elemName)
            return NoSqlDb.ELEM_TTL_SET_SUCCESS

    @saveTrigger
    def clearElemTTL(self, dbName, elemName):
        if (self.elemLockDict[dbName][elemName] is True):
            self.logger.warning("Element Locked {0}->{1}".format(dbName, elemName))
            return NoSqlDb.ELEM_LOCKED
        else:
            self.lockElem(dbName, elemName)
            try:
                self.elemTTL[dbName].pop(elemName)
            except:
                pass
            self.unlockElem(dbName, elemName)
            return NoSqlDb.ELEM_TTL_CLEAR_SUCCESS

    def isElemExpired(self, dbName, elemName):
        curTime = int(time.time())
        if(elemName not in self.elemTTL[dbName].keys()):
            return False
        else:
            if(self.elemTTL[dbName][elemName]["status"] is False):
                return True
            createAt = self.elemTTL[dbName][elemName]["createAt"]
            ttl = self.elemTTL[dbName][elemName]["ttl"]
            if(curTime - createAt >= ttl):
                self.elemTTL[dbName][elemName]["status"] = False
                return True
            else:
                return False

    @saveTrigger
    def createList(self, listName, dbName):
        self.lockList(dbName, listName)
        self.listName[dbName].add(listName)
        self.listDict[dbName][listName] = list()
        self.unlockList(dbName, listName)
        self.logger.info("Create List Success {0}->{1}".format(dbName, listName))
        return NoSqlDb.LIST_CREATE_SUCCESS

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
            return NoSqlDb.LIST_LOCKED
        else:
            self.lockList(dbName, listName)
            self.listDict[dbName][listName].append(value)
            self.unlockList(dbName, listName)
            self.logger.info("Insert List Success {0}->{1}->{2}".format(dbName, listName, value))
            return NoSqlDb.LIST_INSERT_SUCCESS

    @saveTrigger
    def deleteList(self, listName, dbName):
        if(self.listLockDict[dbName][listName] is True):
            self.logger.warning("Delete List Locked {0}->{1}".format(dbName, listName))
            return NoSqlDb.LIST_LOCKED
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
            return NoSqlDb.LIST_DELETE_SUCCESS

    @saveTrigger
    def rmFromList(self, dbName, listName, value):
        if (self.listLockDict[dbName][listName] is True):
            self.logger.warning("Insert List Locked {0}->{1}->{2}".format(dbName, listName, value))
            return NoSqlDb.LIST_LOCKED
        else:
            if(value not in self.listDict[dbName][listName]):
                return NoSqlDb.LIST_NOT_CONTAIN_VALUE
            else:
                self.lockList(dbName, listName)
                self.listDict[dbName][listName].remove(value)
                self.unlockList(dbName, listName)
                self.logger.info("Remove From List Success {0}->{1}->{2}".format(dbName, listName, value))
                return NoSqlDb.LIST_REMOVE_SUCCESS

    def searchAllList(self, dbName):
        if(self.isDbExist(dbName) is False):
            return []
        self.logger.info("Search All List Success {0}".format(dbName))
        return list(self.listName[dbName])

    @saveTrigger
    def clearList(self, dbName, listName):
        if(self.listLockDict[dbName][listName] is True):
            self.logger.warning("Clear List Locked {0}->{1}")
            return NoSqlDb.LIST_LOCKED
        else:
            self.lockList(dbName, listName)
            self.listDict[dbName][listName] = []
            self.unlockList(dbName, listName)
            return NoSqlDb.LIST_CLEAR_SUCCESS

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
                return NoSqlDb.LIST_LOCKED
        return NoSqlDb.LIST_MERGE_SUCCESS

    @saveTrigger
    def setListTTL(self, dbName, listName, ttl):
        if(self.listLockDict[dbName][listName] is True):
            self.logger.warning("List Locked {0}->{1}".format(dbName, listName))
            return NoSqlDb.LIST_LOCKED
        else:
            self.lockList(dbName, listName)
            self.listTTL[dbName][listName] = {"createAt":int(time.time()),
                                              "ttl":int(ttl),
                                              "status":True}
            self.unlockList(dbName, listName)
            return NoSqlDb.LIST_TTL_SET_SUCCESS

    def isListExpired(self, dbName, listName):
        curTime = int(time.time())
        if(listName not in self.listTTL[dbName].keys()):
            return False
        else:
            if(self.listTTL[dbName][listName]["status"] is False):
                return True
            createAt = self.listTTL[dbName][listName]["createAt"]
            ttl = self.listTTL[dbName][listName]["ttl"]
            if(curTime - createAt >= ttl):
                self.listTTL[dbName][listName]["status"] = False
                return True
            else:
                return False

    @saveTrigger
    def clearListTTL(self, dbName, listName):
        if (self.listLockDict[dbName][listName] is True):
            self.logger.warning("List Locked {0}->{1}".format(dbName, listName))
            return NoSqlDb.LIST_LOCKED
        else:
            self.lockList(dbName, listName)
            try:
                self.listTTL[dbName].pop(listName)
            except:
                pass
            self.unlockList(dbName, listName)
            return NoSqlDb.LIST_TTL_CLEAR_SUCCESS

    @saveTrigger
    def createHash(self, dbName, hashName):
        if(self.isHashExist(dbName,hashName) is False):
            self.hashName[dbName].add(hashName)
            self.lockHash(dbName, hashName)
            self.hashDict[dbName][hashName] = dict()
            self.unlockHash(dbName, hashName)
            return NoSqlDb.HASH_CREATE_SUCCESS
        else:
            return NoSqlDb.HASH_EXISTED

    def getHash(self, dbName, hashName):
        return self.hashDict[dbName][hashName]

    @saveTrigger
    def insertHash(self, dbName, hashName, keyName, value):
        if(self.hashLockDict[dbName][hashName] is True):
            return NoSqlDb.HASH_LOCKED
        else:
            self.lockHash(dbName, hashName)
            self.hashDict[dbName][hashName][keyName] = value
            self.unlockHash(dbName, hashName)
            return NoSqlDb.HASH_INSERT_SUCCESS

    def isKeyExist(self, dbName, hashName, keyName):
        return keyName in list(self.hashDict[dbName][hashName].keys())

    @saveTrigger
    def deleteHash(self, dbName, hashName):
        if(self.hashLockDict[dbName][hashName] is True):
            return NoSqlDb.HASH_LOCKED
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
            return NoSqlDb.HASH_DELETE_SUCCESS

    @saveTrigger
    def rmFromHash(self, dbName, hashName, keyName):
        if(self.hashLockDict[dbName][hashName] is True):
            return NoSqlDb.HASH_LOCKED
        else:
            self.lockHash(dbName, hashName)
            self.hashDict[dbName][hashName].pop(keyName)
            self.unlockHash(dbName, hashName)
            return NoSqlDb.HASH_REMOVE_SUCCESS

    @saveTrigger
    def clearHash(self, dbName, hashName):
        if(self.hashLockDict[dbName][hashName] is True):
            return NoSqlDb.HASH_LOCKED
        else:
            self.lockHash(dbName, hashName)
            self.hashDict[dbName][hashName].clear()
            self.unlockHash(dbName, hashName)
            return NoSqlDb.HASH_CLEAR_SUCCESS

    @saveTrigger
    def replaceHash(self, dbName, hashName, hashValue):
        if(self.hashLockDict[dbName][hashName] is True):
            return NoSqlDb.HASH_LOCKED
        else:
            self.lockHash(dbName, hashName)
            self.hashDict[dbName][hashName] = hashValue
            self.unlockHash(dbName, hashName)
            return NoSqlDb.HASH_REPLACE_SUCCESS

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
                return NoSqlDb.HASH_LOCKED
        return NoSqlDb.HASH_MERGE_SUCCESS

    def searchAllHash(self, dbName):
        if(self.isDbExist(dbName) is False):
            return []
        self.logger.info("Search All Hash Success {0}".format(dbName))
        return list(self.hashName[dbName])

    @saveTrigger
    def setHashTTL(self, dbName, hashName, ttl):
        if(self.hashLockDict[dbName][hashName] is True):
            self.logger.warning("Hash Locked {0}->{1}".format(dbName, hashName))
            return NoSqlDb.HASH_LOCKED
        else:
            self.lockHash(dbName, hashName)
            self.hashTTL[dbName][hashName] = {"createAt":int(time.time()),
                                              "ttl":int(ttl),
                                              "status":True}
            self.unlockHash(dbName, hashName)
            return NoSqlDb.HASH_TTL_SET_SUCCESS

    def isHashExpired(self, dbName, hashName):
        curTime = int(time.time())
        if(hashName not in self.hashTTL[dbName].keys()):
            return False
        else:
            if(self.hashTTL[dbName][hashName]["status"] is False):
                return True
            createAt = self.hashTTL[dbName][hashName]["createAt"]
            ttl = self.hashTTL[dbName][hashName]["ttl"]
            if(curTime - createAt >= ttl):
                self.hashTTL[dbName][hashName]["status"] = False
                return True
            else:
                return False

    @saveTrigger
    def clearHashTTL(self, dbName, hashName):
        if (self.hashLockDict[dbName][hashName] is True):
            self.logger.warning("Hash Locked {0}->{1}".format(dbName, hashName))
            return NoSqlDb.HASH_LOCKED
        else:
            self.lockHash(dbName, hashName)
            try:
                self.hashTTL[dbName].pop(hashName)
            except:
                pass
            self.unlockHash(dbName, hashName)
            return NoSqlDb.HASH_TTL_CLEAR_SUCCESS

    @saveTrigger
    def createSet(self, dbName, setName):
        self.lockSet(dbName, setName)
        self.setName[dbName].add(setName)
        self.setDict[dbName][setName] = set()
        self.unlockSet(dbName, setName)
        self.logger.info("Set Create Success {0}->{1}".format(dbName, setName))
        return NoSqlDb.SET_CREATE_SUCCESS

    def getSet(self, dbName, setName):
        return list(self.setDict[dbName][setName])

    @saveTrigger
    def insertSet(self, dbName, setName, setValue):
        if(self.setLockDict[dbName][setName] is True):
            self.logger.warning("Set Is Locked {0}->{1}".format(dbName, setName))
            return NoSqlDb.SET_LOCKED
        else:
            if(setValue not in self.setDict[dbName][setName]):
                self.lockSet(dbName, setName)
                self.setDict[dbName][setName].add(setValue)
                self.unlockSet(dbName, setName)
                self.logger.info("Set Insert Success {0}->{1}->{2}".format(dbName, setName, setValue))
                return NoSqlDb.SET_INSERT_SUCCESS
            else:
                return NoSqlDb.SET_VALUE_ALREADY_EXIST

    @saveTrigger
    def rmFromSet(self, dbName, setName, setValue):
        if(self.setLockDict[dbName][setName] is True):
            self.logger.warning("Set Is Locked {0}->{1}".format(dbName, setName))
            return NoSqlDb.SET_LOCKED
        else:
            if(setValue in self.setDict[dbName][setName]):
                self.lockSet(dbName, setName)
                self.setDict[dbName][setName].discard(setValue)
                self.unlockSet(dbName, setName)
                self.logger.info("Set Remove Success {0}->{1}->{2}".format(dbName, setName, setValue))
                return NoSqlDb.SET_REMOVE_SUCCESS
            else:
                return NoSqlDb.SET_VALUE_NOT_EXISTED

    @saveTrigger
    def clearSet(self, dbName, setName):
        if(self.setLockDict[dbName][setName] is True):
            self.logger.warning("Set Is Locked {0}->{1}".format(dbName, setName))
            return NoSqlDb.SET_LOCKED
        else:
            self.lockSet(dbName, setName)
            self.setDict[dbName][setName].clear()
            self.unlockSet(dbName, setName)
            self.logger.info("Set Clear Success {0}->{1}".format(dbName, setName))
            return NoSqlDb.SET_CLEAR_SUCCESS

    @saveTrigger
    def deleteSet(self, dbName, setName):
        if (self.setLockDict[dbName][setName] is True):
            self.logger.warning("Set Is Locked {0}->{1}".format(dbName, setName))
            return NoSqlDb.SET_LOCKED
        else:
            self.lockSet(dbName, setName)
            self.setName[dbName].discard(setName)
            self.setDict[dbName].pop(setName)
            self.unlockSet(dbName, setName)
            self.setLockDict[dbName].pop(setName)
            self.logger.info("Set Delete Success {0}->{1}".format(dbName, setName))
            return NoSqlDb.SET_DELETE_SUCCESS

    def searchAllSet(self, dbName):
        if(self.isDbExist(dbName) is False):
            return []
        self.logger.info("Search All Set Success {0}".format(dbName))
        return list(self.setName[dbName])

    @saveTrigger
    def addDb(self, dbName):
        if(self.saveLock is True):
            self.logger.warning("Database Save Locked {0}".format(dbName))
            return NoSqlDb.DB_SAVE_LOCK
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
                return NoSqlDb.DB_CREATE_SUCCESS
            else:
                self.logger.warning("Database Already Exists {0}".format(dbName))
                return NoSqlDb.DB_EXISTED

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
                return NoSqlDb.DB_DELETE_SUCCESS
            else:
                return NoSqlDb.DB_SAVE_LOCK
        else:
            return NoSqlDb.DB_NOT_EXISTED

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

            self.saveLock = False
            self.logger.info("Database Save Success")
            self.opCount = 0    # once save process is done, reload the op count
            return NoSqlDb.DB_SAVE_SUCCESS

        else:
            self.logger.warning("Database Save Locked")
            return NoSqlDb.DB_SAVE_LOCK

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

            self.logger.info("Database Load Success")

        except Exception as e:
            self.logger.warning("Database Load Fail {0}".format(str(e)))
