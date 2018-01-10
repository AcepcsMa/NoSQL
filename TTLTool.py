__author__ = 'Ma Haoxiang'

import time
from Decorator import *
from Utils import Utils

# class of TTL tools
class TTLTool(object):

    def __init__(self, databaseList):
        self.database = databaseList[0]

    def setTTL(self, dbName, keyName, ttl, dataType):
        lockDict = self.database.getLockDict(dataType)
        ttlDict = self.database.getTTLDict(dataType)

        if lockDict[dbName][keyName] is True:
            self.database.logger.warning("{} Is Locked {}->{}".
                                         format(dataType, dbName, keyName))
            msg = Utils.makeMessage("{} Is Locked".format(dataType),
                                   responseCode.LOCKED, keyName)
            return msg
        else:
            self.database.lock(dataType, dbName, keyName)
            ttlDict[dbName][keyName] = {"createAt": int(time.time()),
                                              "ttl": int(ttl),
                                              "status": True}
            self.database.unlock(dataType, dbName, keyName)
            self.database.logger.info("TTL Set Success {}->{}:{}"
                                      .format(dbName, keyName, ttl))
            msg = Utils.makeMessage(responseCode.detail[responseCode.TTL_SET_SUCCESS],
                                   responseCode.TTL_SET_SUCCESS, keyName)
            self.database.opCount += 1
            return msg

    def clearTTL(self, dbName, keyName, dataType):
        lockDict = self.database.getLockDict(dataType)
        ttlDict = self.database.getTTLDict(dataType)

        if lockDict[dbName][keyName] is True:
            self.database.logger.warning("{} Locked {}->{}".
                                         format(dataType, dbName, keyName))
            msg = Utils.makeMessage("{} Is Locked".format(dataType),
                                   responseCode.LOCKED, keyName)
            return msg
        else:
            self.database.lock(dataType, dbName, keyName)
            try:
                ttlDict[dbName].pop(keyName)
            except:
                msg = Utils.makeMessage("{} Is Not Set TTL".format(dataType),
                                       responseCode.NOT_SET_TTL, keyName)
                return msg
            finally:
                self.database.unlock(dataType, dbName, keyName)
            self.database.logger.info("{} TTL Clear Success {}->{}".
                                      format(dataType, dbName, keyName))
            msg = Utils.makeMessage("TTL Clear Success",
                                   responseCode.TTL_CLEAR_SUCCESS, keyName)
            self.database.opCount += 1
            return msg

    def showTTL(self, dbName, keyName, dataType):
        try:
            ttlDict = self.database.getTTLDict(dataType)
            curTime = int(time.time())
            ttl = ttlDict[dbName][keyName]["ttl"]
            restTime = ttl - (curTime - ttlDict[dbName][keyName]["createAt"])
            if restTime <= 0:
                ttlDict[dbName][keyName]["status"] = False
                code, result = responseCode.TTL_EXPIRED, None
            else:
                code, result = responseCode.TTL_SHOW_SUCCESS, restTime
            return Utils.makeMessage(responseCode.detail[code],
                                     code,
                                     result)
        except:
            return Utils.makeMessage(responseCode.detail[responseCode.ELEM_TYPE_ERROR],
                                     responseCode.ELEM_TYPE_ERROR,
                                     keyName)
