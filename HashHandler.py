__author__ = 'Ma Haoxiang'

# import
from Decorator import *
from Utils import Utils

class HashHandler(object):
    
    def __init__(self, database):
        self.database = database

    # create a hash
    @validTypeCheck
    def createHash(self, dbName, keyName, password=None):
        if self.database.isDbExist(dbName):
            code = self.database.createHash(dbName=dbName,
                                            keyName=keyName,
                                            password=password)
            result = keyName
        else:
            code, result = responseCode.DB_NOT_EXIST, dbName
        msg = Utils.makeMessage(responseCode.detail[code],
                                code,
                                result)
        return msg

    # get hash
    @validTypeCheck
    def getHash(self, dbName, keyName, password=None):
        if self.database.isExist("HASH", dbName, keyName):
            if self.database.isExpired("HASH", dbName, keyName) is False:
                code, result = self.database.getHash(dbName=dbName,
                                                     keyName=keyName,
                                                     password=password)
            else:
                code, result = responseCode.HASH_EXPIRED, keyName
        else:
            code, result = responseCode.HASH_NOT_EXISTED, keyName
        msg = Utils.makeMessage(responseCode.detail[code],
                                code,
                                result)
        return msg

    # get keyset of the given hash
    @validTypeCheck
    def getKeySet(self, dbName, keyName, password=None):
        if self.database.isExist("HASH", dbName, keyName):
            if self.database.isExpired("HASH", dbName, keyName) is False:
                code, result = self.database.getHashKeySet(dbName=dbName,
                                                           keyName=keyName,
                                                           password=password)
            else:
                code, result = responseCode.HASH_EXPIRED, keyName
        else:
            code, result = responseCode.HASH_NOT_EXISTED, keyName
        msg = Utils.makeMessage(responseCode.detail[code],
                                code,
                                result)
        return msg

    @validTypeCheck
    def getValues(self, dbName, keyName, password=None):
        if self.database.isExist("HASH", dbName, keyName):
            if self.database.isExpired("HASH", dbName, keyName) is False:
                code, result = self.database.getHashValues(dbName=dbName,
                                                           keyName=keyName,
                                                           password=password)
            else:
                code, result = responseCode.HASH_EXPIRED, keyName
        else:
            code, result = responseCode.HASH_NOT_EXISTED, keyName
        msg = Utils.makeMessage(responseCode.detail[code],
                                code,
                                result)
        return msg

    @validTypeCheck
    def getMultipleValues(self, dbName, keyName, keys, password=None):
        if self.database.isExist("HASH", dbName, keyName):
            if self.database.isExpired("HASH", dbName, keyName) is False:
                code, result = self.database.getMultipleHashValues(dbName=dbName, keyName=keyName,
                                                                   keys=keys, password=password)
            else:
                code, result = responseCode.HASH_EXPIRED, keyName
        else:
            code, result = responseCode.HASH_NOT_EXISTED, keyName
        msg = Utils.makeMessage(responseCode.detail[code],
                                code,
                                result)
        return msg

    # insert a key-value data into the given hash
    @validTypeCheck
    def insertHash(self, dbName, keyName, key, value, password=None):
        if Utils.isValidType(keyName) is False:
            return Utils.makeMessage(responseCode.detail[responseCode.ELEM_TYPE_ERROR],
                                     responseCode.ELEM_TYPE_ERROR,
                                     keyName)

        if self.database.isExist("HASH", dbName, keyName):
            if self.database.isExpired("HASH", dbName, keyName) is False:
                code = self.database.insertHash(dbName=dbName, keyName=keyName,
                                                key=key, value=value,
                                                password=password)
            else:
                code = responseCode.HASH_EXPIRED
        else:
            code = responseCode.HASH_NOT_EXISTED
        msg = Utils.makeMessage(responseCode.detail[code],
                                code,
                                keyName)
        return msg

    # check if a key exists in the given hash
    @validTypeCheck
    def isKeyExist(self, dbName, keyName, key, password=None):
        if self.database.isExist("HASH", dbName, keyName):
            if self.database.isExpired("HASH", dbName, keyName) is False:
                result = self.database.isKeyExist(dbName=dbName, keyName=keyName,
                                                  key=key, password=password)
                code = responseCode.HASH_KEY_EXIST if result is True else responseCode.HASH_KEY_NOT_EXIST
                result = key
            else:
                code, result = responseCode.HASH_EXPIRED, keyName
        else:
            code, result = responseCode.HASH_NOT_EXISTED, keyName
        msg = Utils.makeMessage(responseCode.detail[code],
                                code,
                                result)
        return msg

    # delete the given hash
    @validTypeCheck
    def deleteHash(self, dbName, keyName, password=None):
        if self.database.isExist("HASH", dbName, keyName):
            code = self.database.deleteHash(dbName=dbName,
                                            keyName=keyName,
                                            password=password)
        else:
            code = responseCode.HASH_NOT_EXISTED
        msg = Utils.makeMessage(responseCode.detail[code],
                                code,
                                keyName)
        return msg

    # remove a key-value data from the given hash
    @validTypeCheck
    def rmFromHash(self, dbName, keyName, key, password=None):
        if self.database.isKeyExist(dbName, keyName, key):
            if self.database.isExpired("HASH", dbName, keyName) is False:
                code = self.database.rmFromHash(dbName=dbName, keyName=keyName,
                                                key=key, password=password)
                result = key
            else:
                code, result = responseCode.HASH_EXPIRED, keyName
        else:
            code, result = responseCode.HASH_KEY_NOT_EXIST, key
        msg = Utils.makeMessage(responseCode.detail[code],
                                code,
                                result)
        return msg

    # clear the entire hash
    @validTypeCheck
    def clearHash(self, dbName, keyName, password=None):
        if self.database.isExist("HASH", dbName, keyName) is True:
            if self.database.isExpired("HASH", dbName, keyName) is False:
                code = self.database.clearHash(dbName=dbName,
                                               keyName=keyName,
                                               password=password)
            else:
                code = responseCode.HASH_EXPIRED
        else:
            code = responseCode.HASH_NOT_EXISTED
        msg = Utils.makeMessage(responseCode.detail[code],
                                code,
                                keyName)
        return msg

    # replace the existed hash with a new value
    @validTypeCheck
    def replaceHash(self, dbName, keyName, value, password=None):
        if Utils.isDict(value) is False :
            code = responseCode.ELEM_TYPE_ERROR
        else:
            if self.database.isExist("HASH", dbName, keyName) is True:
                if self.database.isExpired("HASH", dbName, keyName) is False:
                    code = self.database.replaceHash(dbName=dbName, keyName=keyName,
                                                     hashValue=value, password=password)
                else:
                    code = responseCode.HASH_EXPIRED
            else:
                code = responseCode.HASH_NOT_EXISTED
        msg = Utils.makeMessage(responseCode.detail[code],
                                code,
                                keyName)
        return msg

    # merge two hashs
    @validTypeCheck
    def mergeHashs(self, dbName, keyName1, keyName2, resultKeyName=None, mergeMode=0, password=None):
        if Utils.isValidType(keyName2) is False:
            return Utils.makeMessage(responseCode.detail[responseCode.ELEM_TYPE_ERROR],
                                    responseCode.ELEM_TYPE_ERROR,
                                    keyName2)

        if resultKeyName is not None:
            if self.database.isExist("HASH", dbName, resultKeyName) is True:
                return Utils.makeMessage(responseCode.detail[responseCode.MERGE_RESULT_EXIST],
                                        responseCode.MERGE_RESULT_EXIST,
                                        resultKeyName)

        if self.database.isExist("HASH", dbName, keyName1, keyName2):
            if self.database.isExpired("HASH", dbName, keyName1, keyName2) is False:
                code = self.database.mergeHashs(dbName=dbName, keyName1=keyName1,
                                                keyName2=keyName2, resultKeyName=resultKeyName,
                                                mergeMode=mergeMode, password=password)
                result = resultKeyName
            else:
                code, result = responseCode.HASH_EXPIRED, "{} or {}".format(keyName1, keyName2)
        else:
            code, result = responseCode.HASH_NOT_EXISTED, "{} or {}".format(keyName1, keyName2)
        msg = Utils.makeMessage(responseCode.detail[code],
                               code,
                               result)
        return msg

    # search hash names using regular expression
    def searchHash(self, dbName, expression, password=None):
        if self.database.isDbExist(dbName) is False:
            return Utils.makeMessage(responseCode.detail[responseCode.DB_NOT_EXIST],
                                   responseCode.DB_NOT_EXIST,
                                   dbName)

        if Utils.isValidType(dbName):
            result = self.database.searchByRE(dbName=dbName, expression=expression,
                                              dataType="HASH", password=password)
            code = responseCode.HASH_SEARCH_SUCCESS
        else:
            code, result = responseCode.ELEM_TYPE_ERROR, dbName
        msg = Utils.makeMessage(responseCode.detail[code],
                               code,
                               result)
        return msg

    # return all hash names in the given database
    def searchAllHash(self, dbName, password=None):
        if Utils.isValidType(dbName):
            if self.database.isDbExist(dbName):
                result = self.database.searchAllHash(dbName=dbName,
                                                     password=password)
                code = responseCode.HASH_SEARCH_SUCCESS
            else:
                code, result = responseCode.DB_NOT_EXIST, dbName
        else:
            code, result = responseCode.ELEM_TYPE_ERROR, dbName
        msg = Utils.makeMessage(responseCode.detail[code],
                               code,
                               result)
        return msg

    @validTypeCheck
    def getSize(self, dbName, keyName, password=None):
        if self.database.isExist("HASH", dbName, keyName) is False:
            code, result = responseCode.HASH_NOT_EXISTED, keyName
        else:
            code, result = self.database.getSize(dbName=dbName, keyName=keyName,
                                                 type="HASH", password=password)
        msg = Utils.makeMessage(responseCode.detail[code],
                                code,
                                result)
        return msg

    @validTypeCheck
    def increaseHash(self, dbName, keyName, key, password=None):
        if self.database.isKeyExist(dbName, keyName, key):
            if self.database.isExpired("HASH", dbName, keyName) is False:
                code, result = self.database.increaseHash(dbName=dbName, keyName=keyName,
                                                          key=key, password=password)
            else:
                code, result = responseCode.HASH_EXPIRED, keyName
        else:
            code, result = responseCode.HASH_NOT_EXISTED, keyName
        msg = Utils.makeMessage(responseCode.detail[code],
                                code,
                                result)
        return msg

    @validTypeCheck
    def decreaseHash(self, dbName, keyName, key, password=None):
        if self.database.isKeyExist(dbName, keyName, key):
            if self.database.isExpired("HASH", dbName, keyName) is False:
                code, result = self.database.decreaseHash(dbName=dbName, keyName=keyName,
                                                          key=key, password=password)
            else:
                code, result = responseCode.HASH_EXPIRED, keyName
        else:
            code, result = responseCode.HASH_NOT_EXISTED, keyName
        msg = Utils.makeMessage(responseCode.detail[code],
                                code,
                                result)
        return msg
