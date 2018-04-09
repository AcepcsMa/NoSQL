__author__ = 'Ma Haoxiang'

class AOFLoader(object):

    def __init__(self, aofLogPath, db):
        self.logPath = aofLogPath
        self.db = db
        self.loadMapping()
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
            "MERGE_LIST": self.mergeList,
            "CREATE_HASH": self.createHash,
            "INSERT_HASH": self.insertHash,
            "DELETE_HASH": self.deleteHash,
            "REMOVE_FROM_HASH": self.removeFromHash,
            "CLEAR_HASH": self.clearHash,
            "REPLACE_HASH": self.replaceHash,
            "MERGE_HASH": self.mergeHash,
            "INCREASE_HASH": self.increaseHash,
            "DECREASE_HASH": self.decreaseHash
        }

    def loadLogs(self):
        self.logs = []
        self.pwdLogs = []
        with open(self.logPath, "r") as aofLog:
            for line in aofLog.readlines():
                if "DATABASE_PWD" in line:
                    self.pwdLogs.append(line)   # separate pwd operations
                else:
                    self.logs.append(line)

    def build(self):
        for log in self.logs:
            terms = log.split("\t")
            op = terms[0]
            self.mapping[op](terms)

    def parseElemArgs(self, terms):
        return {
            "dbName": terms[1],
            "keyName": terms[2],
            "value": terms[3]
        }

    def parseListArgs(self, terms):
        args = {
            "dbName": terms[1],
            "keyName": terms[2]
        }
        if len(terms) == 4:
            args["value"] = terms[3]
        elif len(terms) == 5:
            args.pop("keyName")
            args["keyName1"] = terms[2]
            args["keyName2"] = terms[3]
            args["resultKeyName"] = terms[4]
        return args

    def parseHashArgs(self, terms):
        args = {
            "dbName": terms[1],
            "keyName": terms[2]
        }
        if len(terms) == 4:
            if terms[0] == "REMOVE_FROM_HASH":
                args["key"] = terms[3]
            elif terms[0] == "REPLACE_HASH":
                args["value"] = terms[3]
        elif len(terms) == 5:
            if terms[0] in ["INSERT_HASH", "DECREASE_HASH", "INCREASE_HASH"]:
                args["key"] = terms[3]
                args["value"] = terms[4]
            elif terms[0] == "MERGE_HASH":
                args.pop("keyName")
                args["keyName1"] = terms[2]
                args["keyName2"] = terms[3]
                args["resultKeyName"] = terms[4]
        return args

    def createElem(self, terms):
        args = self.parseElemArgs(terms)
        self.db.createElem(dbName=args["dbName"],
                           keyName=args["keyName"],
                           value=args["value"])

    def updateElem(self, terms):
        args = self.parseElemArgs(terms)
        self.db.updateElem(dbName=args["dbName"],
                           keyName=args["keyName"],
                           value=args["value"])

    def increaseElem(self, terms):
        args = self.parseElemArgs(terms)
        self.db.increaseElem(dbName=args["dbName"],
                             keyName=args["keyName"],
                             value=args["value"])

    def decreaseElem(self, terms):
        args = self.parseElemArgs(terms)
        self.db.decreaseElem(dbName=args["dbName"],
                             keyName=args["keyName"],
                             value=args["value"])

    def deleteElem(self, terms):
        args = self.parseElemArgs(terms)
        self.db.deleteElem(dbName=args["dbName"], keyName=args["keyName"])

    def createList(self, terms):
        args = self.parseListArgs(terms)
        self.db.createList(dbName=args["dbName"], keyName=args["keyName"])

    def leftInsertList(self, terms):
        args = self.parseListArgs(terms)
        self.db.insertList(dbName=args["dbName"], keyName=args["keyName"],
                           value=args["value"], isLeft=True)

    def rightInsertList(self, terms):
        args = self.parseListArgs(terms)
        self.db.insertList(dbName=args["dbName"], keyName=args["keyName"],
                           value=args["value"], isLeft=None)

    def removeFromList(self, terms):
        args = self.parseListArgs(terms)
        self.db.rmFromList(dbName=args["dbName"],
                           keyName=args["keyName"],
                           value=args["value"])

    def deleteList(self, terms):
        args = self.parseListArgs(terms)
        self.db.deleteList(dbName=args["dbName"], keyName=args["keyName"])

    def clearList(self, terms):
        args = self.parseListArgs(terms)
        self.db.clearList(dbName=args["dbName"], keyName=args["keyName"])

    def mergeList(self, terms):
        args = self.parseListArgs(terms)
        self.db.mergeLists(dbName=args["dbName"], keyName1=args["keyName1"],
                           keyName2=args["keyName2"], resultKeyName=args["resultKeyName"])

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
