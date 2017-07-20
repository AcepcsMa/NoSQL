__author__ = 'Ma Haoxiang'

# import
import re
import os
import json
import logging

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

    def __init__(self, config):
        self.dbNameSet = {"db0", "db1", "db2", "db3", "db4"}  # initial databases

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

        self.saveLock = False

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

    def deleteElem(self, elemName, dbName):
        if (self.elemLockDict[dbName][elemName] is True):  # element is locked
            self.logger.warning("Delete Element Locked {0}->{1}".format(dbName, elemName))
            return NoSqlDb.ELEM_LOCKED
        else:
            self.elemName[dbName].remove(elemName)
            self.elemDict[dbName].pop(elemName)
            self.logger.info("Delete Element Success {0}->{1}".format(dbName, elemName))
            return NoSqlDb.ELEM_DELETE_SUCCESS

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

    def deleteList(self, listName, dbName):
        if(self.listLockDict[dbName][listName] is True):
            self.logger.warning("Delete List Locked {0}->{1}".format(dbName, listName))
            return NoSqlDb.LIST_LOCKED
        else:
            self.lockList(dbName, listName)
            self.listName[dbName].remove(listName)
            self.listDict[dbName].pop(listName)
            self.unlockList(dbName, listName)
            self.listLockDict[dbName].pop(listName)
            self.logger.info("Delete List Success {0}->{1}".format(dbName, listName))
            return NoSqlDb.LIST_DELETE_SUCCESS

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

    def deleteHash(self, dbName, hashName):
        if(self.hashLockDict[dbName][hashName] is True):
            return NoSqlDb.HASH_LOCKED
        else:
            self.lockHash(dbName, hashName)
            self.hashDict[dbName].pop(hashName)
            self.hashName[dbName].remove(hashName)
            self.unlockHash(dbName, hashName)
            self.hashLockDict[dbName].pop(hashName)
            return NoSqlDb.HASH_DELETE_SUCCESS

    def rmFromHash(self, dbName, hashName, keyName):
        if(self.hashLockDict[dbName][hashName] is True):
            return NoSqlDb.HASH_LOCKED
        else:
            self.lockHash(dbName, hashName)
            self.hashDict[dbName][hashName].pop(keyName)
            self.unlockHash(dbName, hashName)
            return NoSqlDb.HASH_REMOVE_SUCCESS

    def searchAllHash(self, dbName):
        if(self.isDbExist(dbName) is False):
            return []
        self.logger.info("Search All Hash Success {0}".format(dbName))
        return list(self.hashName[dbName])

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
                with open("data" + os.sep + dbName + os.sep + "listName.txt", "w") as listNameFile:
                    listNameFile.write(json.dumps(list(self.listName[dbName])))
                with open("data" + os.sep + dbName + os.sep + "listValue.txt", "w") as listValueFile:
                    listValueFile.write(json.dumps(self.listDict[dbName]))
                with open("data" + os.sep + dbName + os.sep + "hashName.txt", "w") as hashNameFile:
                    hashNameFile.write(json.dumps(list(self.hashName[dbName])))
                with open("data" + os.sep + dbName + os.sep + "hashValue.txt", "w") as hashValueFile:
                    hashValueFile.write(json.dumps(self.hashDict[dbName]))

            self.saveLock = False
            self.logger.info("Database Save Success")
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

                # load element names
                with open("data"+os.sep+dbName+os.sep+"elemName.txt","r") as elemNameFile:
                    elemNames = json.loads(elemNameFile.read())
                    for elemName in elemNames:
                        self.elemName[dbName].add(elemName)
                        self.elemLockDict[dbName][elemName] = False

                # load element values
                with open("data" + os.sep + dbName + os.sep + "elemValue.txt", "r") as elemValueFile:
                    self.elemDict[dbName] = json.loads(elemValueFile.read())

                # load list names
                with open("data"+os.sep+dbName+os.sep+"listName.txt","r") as listNameFile:
                    listNames = json.loads(listNameFile.read())
                    for listName in listNames:
                        self.listName[dbName].add(listName)
                        self.listLockDict[dbName][listName] = False

                # load list values
                with open("data" + os.sep + dbName + os.sep + "listValue.txt", "r") as listValueFile:
                    self.listDict[dbName] = json.loads(listValueFile.read())

                # load hash names
                with open("data"+os.sep+dbName+os.sep+"hashName.txt","r") as hashNameFile:
                    hashNames = json.loads(hashNameFile.read())
                    for hashName in hashNames:
                        self.hashName[dbName].add(hashName)
                        self.hashLockDict[dbName][hashName] = False

                # load hash values
                with open("data" + os.sep + dbName + os.sep + "hashValue.txt", "r") as hashValueFile:
                    self.hashDict[dbName] = json.loads(hashValueFile.read())

            self.logger.info("Database Load Success")

        except Exception as e:
            self.logger.warning("Database Load Fail {0}".format(str(e)))
