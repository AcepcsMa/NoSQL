__author__ = 'Ma Haoxiang'

# import
import time
import threading

class ttlTimer(threading.Thread):

    def __init__(self, database, checkInterval=5):
        super(ttlTimer,self).__init__()
        self.checkInterval = checkInterval
        self.database = database
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
            curTime = int(time.time())
            for key in elemTTL.keys():
                if(elemTTL[key]["status"] is True):
                    if(curTime - elemTTL[key]["createAt"] >= elemTTL[key]["ttl"]):
                        self.database.elemTTL[dbName][key]["status"] = False
            for key in listTTL.keys():
                if(listTTL[key]["status"] is True):
                    if(curTime - listTTL[key]["createAt"] >= listTTL[key]["ttl"]):
                        self.database.listTTL[dbName][key]["status"] = False
            for key in hashTTL.keys():
                if(hashTTL[key]["status"] is True):
                    if(curTime - hashTTL[key]["createAt"] >= hashTTL[key]["ttl"]):
                        self.database.hashTTL[dbName][key]["status"] = False
        self.checkLock = False

    def run(self):
        while(True):
            if(self.checkLock is False):
                self.checkTTL()
            time.sleep(self.checkInterval)

