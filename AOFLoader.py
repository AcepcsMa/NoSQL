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
            "DELETE_ELEM": self.deleteElem
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

    def createList(self):
        pass

    def leftInsertList(self):
        pass

    def rightInsertList(self):
        pass

    def removeFromList(self):
        pass

    def deleteList(self):
        pass

    def clearList(self):
        pass

    def mergeList(self):
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
