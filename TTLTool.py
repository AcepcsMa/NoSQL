__author__ = 'Ma Haoxiang'

from response import responseCode
import time

# class of TTL tools
class TTLTool:

    def __init__(self, database):
        self.database = database

    def setTTL(self, dbName, keyName, ttl, dataType):

        lockDict = self.database.getLockDict(dataType)
        ttlDict = self.database.getTTLDict(dataType)
        typeName = self.database.translateType(dataType)

        if (lockDict[dbName][keyName] is True):
            self.database.logger.warning("{} Is Locked {}->{}".format(typeName, dbName, keyName))
            return responseCode.LOCKED
        else:
            self.database.lock(typeName, dbName, keyName)
            ttlDict[dbName][keyName] = {"createAt": int(time.time()),
                                              "ttl": int(ttl),
                                              "status": True}
            self.database.unlock("LIST", dbName, keyName)
            self.database.logger.info("List TTL Set Success {}->{}:{}".format(dbName, keyName, ttl))
            return responseCode.TTL_SET_SUCCESS

    def clearListTTL(self, dbName, keyName, dataType):

        lockDict = self.database.getLockDict(dataType)
        ttlDict = self.database.getTTLDict(dataType)
        typeName = self.database.translateType(dataType)

        if (lockDict[dbName][keyName] is True):
            self.database.logger.warning("{} Locked {}->{}".format(typeName, dbName, keyName))
            return responseCode.LOCKED
        else:
            self.database.lock(typeName, dbName, keyName)
            try:
                ttlDict[dbName].pop(keyName)
            except:
                return responseCode.NOT_SET_TTL
            finally:
                self.database.unlock(typeName, dbName, keyName)
            self.database.logger.info("{} TTL Clear Success {}->{}".format(typeName, dbName, keyName))
            return responseCode.TTL_CLEAR_SUCCESS