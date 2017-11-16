__author__ = 'Ma Haoxiang'

import time
from Decorator import *

# class of TTL tools
class TTLTool:

    def __init__(self, databaseList):
        self.database = databaseList[0]

    # make the response message
    def makeMessage(self, msg, typeCode, data):
        message = {
            "msg": msg,
            "typeCode": typeCode,
            "data": data
        }
        return message

    def setTTL(self, dbName, keyName, ttl, dataType):
        lockDict = self.database.getLockDict(dataType)
        ttlDict = self.database.getTTLDict(dataType)

        if (lockDict[dbName][keyName] is True):
            self.database.logger.warning("{} Is Locked {}->{}".format(dataType, dbName, keyName))
            msg = self.makeMessage("{} Is Locked".format(dataType), responseCode.LOCKED, keyName)
            return msg
        else:
            self.database.lock(dataType, dbName, keyName)
            ttlDict[dbName][keyName] = {"createAt": int(time.time()),
                                              "ttl": int(ttl),
                                              "status": True}
            self.database.unlock(dataType, dbName, keyName)
            self.database.logger.info("TTL Set Success {}->{}:{}".format(dbName, keyName, ttl))
            msg = self.makeMessage("TTL Set Success", responseCode.TTL_SET_SUCCESS, keyName)
            self.database.opCount += 1
            return msg

    def clearListTTL(self, dbName, keyName, dataType):
        lockDict = self.database.getLockDict(dataType)
        ttlDict = self.database.getTTLDict(dataType)

        if (lockDict[dbName][keyName] is True):
            self.database.logger.warning("{} Locked {}->{}".format(dataType, dbName, keyName))
            msg = self.makeMessage("{} Is Locked".format(dataType), responseCode.LOCKED, keyName)
            return msg
        else:
            self.database.lock(dataType, dbName, keyName)
            try:
                ttlDict[dbName].pop(keyName)
            except:
                msg = self.makeMessage("{} Is Not Set TTL".format(dataType), responseCode.NOT_SET_TTL, keyName)
                return msg
            finally:
                self.database.unlock(dataType, dbName, keyName)
            self.database.logger.info("{} TTL Clear Success {}->{}".format(dataType, dbName, keyName))
            msg = self.makeMessage("TTL Clear Success", responseCode.TTL_CLEAR_SUCCESS, keyName)
            self.database.opCount += 1
            return msg
