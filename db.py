__author__ = 'Ma Haoxiang'

import re
import os
import json

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

    def __init__(self):
        self.dbNameSet = {"db0", "db1", "db2", "db3", "db4"}  # initial databases
        self.elemName = dict()
        self.elemDict = dict()
        self.elemLockDict = dict()

        self.saveLock = False

        for dbName in self.dbNameSet:
            self.elemName[dbName] = set()
            self.elemDict[dbName] = dict()
            self.elemLockDict[dbName] = dict()

        self.loadDb()   # load data from file


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
        return NoSqlDb.ELEM_CREATE_SUCCESS


    def updateElem(self, elemName, value, dbName):
        if self.elemLockDict[dbName][elemName] is True: # element is locked
            return NoSqlDb.ELEM_LOCKED

        else:   # update the value
            self.lockElem(dbName, elemName)
            self.elemDict[dbName][elemName] = value
            self.unlockElem(dbName, elemName)
            return NoSqlDb.ELEM_UPDATE_SUCCESS


    def getElem(self, elemName, dbName):
        try:
            elemValue = self.elemDict[dbName][elemName]
        except:
            elemValue = None
        return elemValue


    def searchElem(self, expression, dbName):
        searchResult = set()
        expression = re.sub("\*",".*",expression)   # convert expression to regular expression
        for elemName in self.elemName[dbName]:
            try:
                searchResult.add(re.findall("({})".format(expression),elemName)[0])
            except:
                continue

        return list(searchResult)


    def searchAllElem(self, dbName):
        return list(self.elemName[dbName])


    def increaseElem(self, elemName, dbName):

        if(self.elemLockDict[dbName][elemName] is True): # element is locked
            return NoSqlDb.ELEM_LOCKED
        else:
            self.lockElem(dbName, elemName)
            self.elemDict[dbName][elemName] += 1
            self.unlockElem(dbName, elemName)
            return NoSqlDb.ELEM_INCREASE_SUCCESS


    def decreaseElem(self, elemName, dbName):

        if(self.elemLockDict[dbName][elemName] is True): # element is locked
            return NoSqlDb.ELEM_LOCKED
        else:
            self.lockElem(dbName, elemName)
            self.elemDict[dbName][elemName] -= 1
            self.unlockElem(dbName, elemName)
            return NoSqlDb.ELEM_DECREASE_SUCCESS


    def deleteElem(self, elemName, dbName):
        if (self.elemLockDict[dbName][elemName] is True):  # element is locked
            return NoSqlDb.ELEM_LOCKED
        else:
            self.elemName[dbName].remove(elemName)
            self.elemDict[dbName].pop(elemName)
            return NoSqlDb.ELEM_DELETE_SUCCESS


    def addDb(self, dbName):
        if(self.saveLock is True):
            return NoSqlDb.DB_SAVE_LOCK
        else:
            if(dbName not in self.dbNameSet):
                self.dbNameSet.add(dbName)
                self.elemName[dbName] = set()
                self.elemDict[dbName] = dict()
                self.elemLockDict[dbName] = dict()
                return NoSqlDb.DB_CREATE_SUCCESS
            else:
                return NoSqlDb.DB_EXISTED


    def getAllDatabase(self):
        return list(self.dbNameSet)


    def saveDb(self):
        if(self.saveLock is False):
            try:    # check if the data directory exists
                os.makedirs("data")
            except:
                pass

            for dbName in self.dbNameSet:
                try:
                    os.makedirs("data{}{}".format(os.sep,dbName))
                except:
                    pass

            self.saveLock = True
            for dbName in self.dbNameSet:
                # save elements of each db
                with open("data" + os.sep + dbName + os.sep + "elemName.txt", "w") as elemNameFile:
                    elemNameFile.write(json.dumps(list(self.elemName[dbName])))
                with open("data" + os.sep + dbName + os.sep + "elemValue.txt", "w") as elemValueFile:
                    elemValueFile.write((json.dumps(self.elemDict[dbName])))

            self.saveLock = False
            return NoSqlDb.DB_SAVE_SUCCESS

        else:
            return NoSqlDb.DB_SAVE_LOCK


    def loadDb(self):
        try:
            dbNameSet = os.listdir("data")  # find all dbName in the data directory
            for dbName in dbNameSet:
                self.dbNameSet.add(dbName)

                # recover element names
                with open("data"+os.sep+dbName+os.sep+"elemName.txt","r") as elemNameFile:
                    elemNames = json.loads(elemNameFile.read())
                    for elemName in elemNames:
                        self.elemName[dbName].add(elemName)
                        self.elemLockDict[dbName][elemName] = False
                # recover element values
                with open("data" + os.sep + dbName + os.sep + "elemValue.txt", "r") as elemValueFile:
                    self.elemDict[dbName] = json.loads(elemValueFile.read())

        except Exception as e:
            print (e)
