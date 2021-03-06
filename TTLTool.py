__author__ = 'Ma Haoxiang'

import time
from Decorator import *
from Utils import Utils

# class of TTL tools
class TTLTool(object):

    def __init__(self, databaseList):
        self.database = databaseList[0]

    def setTTL(self, dbName, keyName, ttl, dataType):
        if not self.database.isDbExist(dbName):
            code, result = responseCode.DB_NOT_EXIST, dbName
        elif not Utils.isInt(ttl):
            code, result = responseCode.ELEM_TYPE_ERROR, ttl

        else:
            lockDict = self.database.getLockDict(dataType)
            ttlDict = self.database.getTTLDict(dataType)

            try:
                if lockDict[dbName][keyName] is True:
                    self.database.logger.warning("{} Is Locked {}->{}".
                                                 format(dataType, dbName, keyName))
                    code, result = responseCode.LOCKED, keyName
                else:
                    self.database.lock(dataType, dbName, keyName)
                    ttlDict[dbName][keyName] = {"createAt": int(time.time()),
                                                "ttl": int(ttl),
                                                "status": True}
                    self.database.unlock(dataType, dbName, keyName)
                    self.database.logger.info("TTL Set Success {}->{}:{}"
                                              .format(dbName, keyName, ttl))
                    self.database.opCount += 1
                    code, result = responseCode.TTL_SET_SUCCESS, keyName
            except:
                code, result = responseCode.KEY_NAME_INVALID, keyName

        return Utils.makeMessage(responseCode.detail[code],
                                 code,
                                 result)

    def clearTTL(self, dbName, keyName, dataType):
        if not self.database.isDbExist(dbName):
            code, result = responseCode.DB_NOT_EXIST, dbName
        else:
            lockDict = self.database.getLockDict(dataType)
            ttlDict = self.database.getTTLDict(dataType)
            try:
                if lockDict[dbName][keyName] is True:
                    self.database.logger.warning("{} Locked {}->{}".
                                                 format(dataType, dbName, keyName))
                    code, result = responseCode.LOCKED, keyName
                else:
                    self.database.lock(dataType, dbName, keyName)
                    try:
                        ttlDict[dbName].pop(keyName)
                        self.database.logger.info("{} TTL Clear Success {}->{}".
                                                  format(dataType, dbName, keyName))
                        self.database.opCount += 1
                        code, result = responseCode.TTL_CLEAR_SUCCESS, keyName
                    except:
                        code, result = responseCode.NOT_SET_TTL, keyName
                    finally:
                        self.database.unlock(dataType, dbName, keyName)
            except:
                code, result = responseCode.KEY_NAME_INVALID, keyName

        return Utils.makeMessage(responseCode.detail[code],
                                 code,
                                 result)

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
