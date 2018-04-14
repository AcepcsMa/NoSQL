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
            "DECREASE_HASH": self.decreaseHash,
            "CREATE_SET": self.createSet,
            "INSERT_SET": self.insertSet,
            "REMOVE_FROM_SET": self.removeFromSet,
            "CLEAR_SET": self.clearSet,
            "DELETE_SET": self.deleteSet,
            "REPLACE_SET": self.replaceSet
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
        elif len(terms) == 6:
            args.pop("keyName")
            args["keyName1"] = terms[2]
            args["keyName2"] = terms[3]
            args["resultKeyName"] = terms[4]
            args["mergeMode"] = terms[5]
        return args

    def parseSetArgs(self, terms):
        args = {
            "dbName": terms[1],
            "keyName": terms[2]
        }
        if len(terms) == 4:
            args["value"] = terms[3]
        return args

    def parseZSetArgs(self, terms):
        args = {
            "dbName": terms[1],
            "keyName": terms[2]
        }
        if len(terms) == 4:
            args["value"] = terms[3]
        elif len(terms) == 5:
            if terms[0] == "INSERT_ZSET":
                args["value"] = terms[3]
                args["score"] = terms[4]
            if terms[0] == "REMOVE_FROM_ZSET_BY_SCORE":
                args["start"] = terms[3]
                args["end"] = terms[4]
        return args

    def parseDbArgs(self, terms):
        args = {
            "dbName": terms[1]
        }
        if len(terms) == 3:
            args["password"] = terms[2]
        elif len(terms) == 4:
            args["originalPwd"] = terms[2]
            args["newPassword"] = terms[3]
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

    def createHash(self, terms):
        args = self.parseHashArgs(terms)
        self.db.createHash(dbName=args["dbName"], keyName=args["keyName"])

    def insertHash(self, terms):
        args = self.parseHashArgs(terms)
        self.db.insertHash(dbName=args["dbName"], keyName=args["keyName"],
                           key=args["key"], value=args["value"])

    def clearHash(self, terms):
        args = self.parseHashArgs(terms)
        self.db.clearHash(dbName=args["dbName"], keyName=args["keyName"])

    def removeFromHash(self, terms):
        args = self.parseHashArgs(terms)
        self.db.rmFromHash(dbName=args["dbName"],
                           keyName=args["keyName"],
                           key=args["key"])

    def deleteHash(self, terms):
        args = self.parseHashArgs(terms)
        self.db.createHash(dbName=args["dbName"], keyName=args["keyName"])

    def replaceHash(self, terms):
        args = self.parseHashArgs(terms)
        self.db.replaceHash(dbName=args["dbName"],
                            keyName=args["keyName"],
                            hashValue=args["value"])

    def mergeHash(self, terms):
        args = self.parseHashArgs(terms)
        self.db.mergeHashs(dbName=args["dbName"], keyName1=args["keyName1"],
                           keyName2=args["keyName2"], resultKeyName=args["resultKeyName"],
                           mergeMode=args["mergeMode"])

    def increaseHash(self, terms):
        args = self.parseHashArgs(terms)
        self.db.increaseHash(dbName=args["dbName"], keyName=args["keyName"],
                             key=args["key"], value=args["value"])

    def decreaseHash(self, terms):
        args = self.parseHashArgs(terms)
        self.db.decreaseHash(dbName=args["dbName"], keyName=args["keyName"],
                             key=args["key"], value=args["value"])

    def createSet(self, terms):
        args = self.parseSetArgs(terms)
        self.db.createSet(dbName=args["dbName"], keyName=args["keyName"])

    def insertSet(self, terms):
        args = self.parseSetArgs(terms)
        self.db.insertSet(dbName=args["dbName"], 
                          keyName=args["keyName"], 
                          value=args["value"])

    def removeFromSet(self, terms):
        args = self.parseSetArgs(terms)
        self.db.rmFromSet(dbName=args["dbName"], 
                          keyName=args["keyName"], 
                          value=args["value"])

    def deleteSet(self, terms):
        args = self.parseSetArgs(terms)
        self.db.deleteSet(dbName=args["dbName"], keyName=args["keyName"])

    def clearSet(self, terms):
        args = self.parseSetArgs(terms)
        self.db.clearSet(dbName=args["dbName"], keyName=args["keyName"])

    def replaceSet(self, terms):
        args = self.parseSetArgs(terms)
        self.db.replaceSet(dbName=args["dbName"], 
                           keyName=args["keyName"], 
                           value=args["value"])

    def createZSet(self, terms):
        args = self.parseZSetArgs(terms)
        self.db.createZSet(dbName=args["dbName"], keyName=args["keyName"])

    def insertZSet(self, terms):
        args = self.parseZSetArgs(terms)
        self.db.insertZSet(dbName=args["dbName"], keyName=args["keyName"],
                           value=args["value"], score=args["score"])

    def removeFromZSet(self, terms):
        args = self.parseZSetArgs(terms)
        self.db.rmFromZSet(dbName=args["dbName"],
                           keyName=args["keyName"],
                           value=args["value"])

    def deleteZSet(self, terms):
        args = self.parseZSetArgs(terms)
        self.db.deleteZSet(dbName=args["dbName"], keyName=args["keyName"])

    def clearZSet(self, terms):
        args = self.parseZSetArgs(terms)
        self.db.clearZSet(dbName=args["dbName"], keyName=args["keyName"])

    def removeFromZSetByScore(self, terms):
        args = self.parseZSetArgs(terms)
        self.db.createZSet(dbName=args["dbName"], keyName=args["keyName"],
                           start=args["start"], end=args["end"])

    def addDatabase(self, terms):
        args = self.parseDbArgs(terms)
        adminKey = self.db.adminKey
        self.db.addDatabase(adminKey=adminKey, dbName=args["dbName"])

    def deleteDatabase(self, terms):
        args = self.parseDbArgs(terms)
        adminKey = self.db.adminKey
        self.db.delDatabase(adminKey=adminKey, dbName=args["dbName"])

    def setDatabasePwd(self, terms):
        args = self.parseDbArgs(terms)
        adminKey = self.db.adminKey
        self.db.setDbPassword(adminKey=adminKey,
                              dbName=args["dbName"],
                              password=args["password"])

    def changeDatabasePwd(self, terms):
        args = self.parseDbArgs(terms)
        adminKey = self.db.adminKey
        self.db.changeDbPassword(adminKey=adminKey, dbName=args["dbName"],
                      originalPwd=args["originalPwd"], newPwd=args["newPassword"])

    def deleteDatabasePwd(self, terms):
        args = self.parseDbArgs(terms)
        adminKey = self.db.adminKey
        self.db.removeDbPassword(adminKey=adminKey, dbName=args["dbName"])
