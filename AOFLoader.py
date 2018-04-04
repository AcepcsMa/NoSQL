__author__ = 'Ma Haoxiang'

class AOFLoader(object):

    def __init__(self, aofLogPath, db):
        self.logPath = aofLogPath
        self.db = db
        self.loadLogs()

    def loadMapping(self):
        self.mapping = {
            "CREATE_ELEM": self.createElem,
            "UPDATE_ELEM": self.updateElem,
            "INCREASE_ELEM": self.increaseElem,
            "DECREASE_ELEM": self.decreaseElem,
            "DELETE_ELEM": self.deleteElem,
            "CREATE_LIST": self.createList,
            "INSERT_LIST": self.rightInsertList,
            "LEFT_INSERT_LIST": self.leftInsertList,
            "DELETE_LIST": self.deleteList,
            "REMOVE_FROM_LIST": self.removeFromList,
            "CLEAR_LIST": self.clearList,
            "MERGE_LIST": self.mergeList
        }

    def loadLogs(self):
        self.logs = []
        with open(self.logPath, "r") as aofLog:
            for line in aofLog.readlines():
                self.logs.append(line)

    def build(self):
        for log in self.logs:
            terms = log.split("\t")
            op = terms[0]
            self.mapping[op](terms[1:])

    def createElem(self, terms):
        pass

    def updateElem(self, terms):
        pass

    def increaseElem(self, terms):
        pass

    def decreaseElem(self, terms):
        pass

    def deleteElem(self, terms):
        pass

    def createList(self, terms):
        pass

    def leftInsertList(self, terms):
        pass

    def rightInsertList(self, terms):
        pass

    def removeFromList(self, terms):
        pass

    def deleteList(self, terms):
        pass

    def clearList(self, terms):
        pass

    def mergeList(self, terms):
        pass

    def createHash(self):
        pass

    def insertHash(self):
        pass

    def clearHash(self):
        pass

    def removeFromHash(self):
        pass

    def deleteHash(self):
        pass

    def replaceHash(self):
        pass

    def mergeHash(self):
        pass

    def increaseHash(self):
        pass

    def decreaseHash(self):
        pass

    def createSet(self):
        pass

    def insertSet(self):
        pass

    def removeFromSet(self):
        pass

    def deleteSet(self):
        pass

    def clearSet(self):
        pass

    def replaceSet(self):
        pass

    def createZSet(self):
        pass

    def insertZSet(self):
        pass

    def removeFromZSet(self):
        pass

    def deleteZSet(self):
        pass

    def clearZSet(self):
        pass

    def removeFromZSetByScore(self):
        pass

    def addDatabase(self):
        pass

    def deleteDatabase(self):
        pass

    def setDatabasePwd(self):
        pass

    def changeDatabasePwd(self):
        pass

    def deleteDatabasePwd(self):
        pass
