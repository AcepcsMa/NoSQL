__author__ = 'Ma Haoxiang'

# import
import time
import threading

class ttlTimer(threading.Thread):

    def __init__(self, database, checkInterval=5):
        super(ttlTimer, self).__init__()
        self.checkInterval = checkInterval
        self.database = database[0]
        self.checkLock = False

    def setInterval(self, interval):
        self.checkInterval = interval

    def checkTTL(self):
        self.checkLock = True
        dbNames = self.database.getAllDatabase()
        for dbName in dbNames:
            elemTTL = self.database.elemTTL[dbName]
            listTTL = self.database.listTTL[dbName]
            hashTTL = self.database.hashTTL[dbName]
            setTTL = self.database.setTTL[dbName]
            zsetTTL = self.database.zsetTTL[dbName]
            self.checkTypeTTL(elemTTL)
            self.checkTypeTTL(listTTL)
            self.checkTypeTTL(hashTTL)
            self.checkTypeTTL(setTTL)
            self.checkTypeTTL(zsetTTL)

        self.checkLock = False

    def checkTypeTTL(self, ttlDict):
        curTime = int(time.time())
        for key in ttlDict.keys():
            if ttlDict[key]["status"] is True:
                if curTime - ttlDict[key]["createAt"] >= ttlDict[key]["ttl"]:
                    ttlDict[key]["status"] = False

    def run(self):
        while True:
            if self.checkLock is False:
                self.checkTTL()
            time.sleep(self.checkInterval)

