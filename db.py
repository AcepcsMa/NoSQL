__author__ = 'Ma Haoxiang'

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

    def __init__(self, config):
        self.dbNameSet = {"db0", "db1", "db2", "db3", "db4"}  # initial databases
        self.elemName = dict()
        self.elemDict = dict()
        self.elemLockDict = dict()

        self.saveLock = False

        for dbName in self.dbNameSet:
            self.elemName[dbName] = set()
            self.elemDict[dbName] = dict()
            self.elemLockDict[dbName] = dict()

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


    def isElemExist(self, dbName, elemName):
        return elemName in self.elemName[dbName]


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


    def searchElem(self, expression, dbName):
        searchResult = set()
        expression = re.sub("\*",".*",expression)   # convert expression to regular expression
        for elemName in self.elemName[dbName]:
            try:
                searchResult.add(re.findall("({})".format(expression),elemName)[0])
            except:
                continue

        self.logger.info("Search Element Success {0} {1}".format(dbName, expression))
        return list(searchResult)


    def searchAllElem(self, dbName):
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
                self.logger.info("Database Add Success {0}".format(dbName))
                return NoSqlDb.DB_CREATE_SUCCESS
            else:
                self.logger.warning("Database Already Exists {0}".format(dbName))
                return NoSqlDb.DB_EXISTED


    def getAllDatabase(self):
        print ("!!@@##$$%%^^&&**(())")
        self.logger.info("Get All Database Names Success")
        return list(self.dbNameSet)


    def saveDb(self):
        #print (self.elemName)
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
                    elemValueFile.write((json.dumps(self.elemDict[dbName])))

            self.saveLock = False
            self.logger.info("Database Save Success")
            return NoSqlDb.DB_SAVE_SUCCESS

        else:
            self.logger.warning("Database Save Locked")
            return NoSqlDb.DB_SAVE_LOCK


    def loadDb(self):
        try:
            dbNameSet = os.listdir("data")  # find all dbName in the data directory
            for dbName in dbNameSet:
                self.dbNameSet.add(dbName)
                self.elemName[dbName] = set()
                self.elemLockDict[dbName] = dict()
                self.elemDict[dbName] = dict()

                # load element names
                with open("data"+os.sep+dbName+os.sep+"elemName.txt","r") as elemNameFile:
                    elemNames = json.loads(elemNameFile.read())
                    for elemName in elemNames:
                        self.elemName[dbName].add(elemName)
                        self.elemLockDict[dbName][elemName] = False
                # load element values
                with open("data" + os.sep + dbName + os.sep + "elemValue.txt", "r") as elemValueFile:
                    self.elemDict[dbName] = json.loads(elemValueFile.read())
            self.logger.info("Database Load Success")

        except Exception as e:
            self.logger.warning("Database Load Fail {0}".format(str(e)))
