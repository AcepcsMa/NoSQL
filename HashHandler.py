__author__ = 'Ma Haoxiang'

# import
from Decorator import *
from Utils import Utils

class HashHandler(object):
    
    def __init__(self, database):
        self.database = database

    # create a hash
    @validTypeCheck
    def createHash(self, dbName, hashName):
        if self.database.isDbExist(dbName):
            result = self.database.createHash(dbName, hashName)
            msg = Utils.makeMessage(responseCode.detail[result],
                                   result,
                                   hashName)
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.DB_NOT_EXIST],
                                   responseCode.DB_NOT_EXIST,
                                   dbName)
        return msg

    # get hash
    @validTypeCheck
    def getHash(self, dbName, hashName):
        if self.database.isExist("HASH", dbName, hashName):
            if self.database.isExpired("HASH", dbName, hashName) is False:
                hashValue = self.database.getHash(dbName, hashName)
                msg = Utils.makeMessage(responseCode.detail[responseCode.HASH_GET_SUCCESS],
                                       responseCode.HASH_GET_SUCCESS,
                                       hashValue)
            else:
                msg = Utils.makeMessage(responseCode.detail[responseCode.HASH_EXPIRED],
                                       responseCode.HASH_EXPIRED,
                                       hashName)
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.HASH_NOT_EXISTED],
                                   responseCode.HASH_NOT_EXISTED,
                                   hashName)
        return msg

    # get keyset of the given hash
    @validTypeCheck
    def getKeySet(self, dbName, hashName):
        if self.database.isExist("HASH", dbName, hashName):
            if self.database.isExpired("HASH", dbName, hashName) is False:
                keySet = self.database.getHashKeySet(dbName, hashName)
                msg = Utils.makeMessage(responseCode.detail[responseCode.HASH_KEYSET_GET_SUCCESS],
                                       responseCode.HASH_KEYSET_GET_SUCCESS,
                                       keySet)
            else:
                msg = Utils.makeMessage(responseCode.detail[responseCode.HASH_EXPIRED],
                                       responseCode.HASH_EXPIRED,
                                       hashName)
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.HASH_NOT_EXISTED],
                                   responseCode.HASH_NOT_EXISTED,
                                   hashName)
        return msg

    @validTypeCheck
    def getValues(self, dbName, hashName):
        if self.database.isExist("HASH", dbName, hashName):
            if self.database.isExpired("HASH", dbName, hashName) is False:
                valueList = self.database.getHashValues(dbName, hashName)
                msg = Utils.makeMessage(responseCode.detail[responseCode.HASH_GET_SUCCESS],
                                       responseCode.HASH_VALUES_GET_SUCCESS,
                                       valueList)
            else:
                msg = Utils.makeMessage(responseCode.detail[responseCode.HASH_EXPIRED],
                                       responseCode.HASH_EXPIRED,
                                       hashName)
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.HASH_NOT_EXISTED],
                                   responseCode.HASH_NOT_EXISTED,
                                   hashName)
        return msg

    @validTypeCheck
    def getMultipleValues(self, dbName, hashName, keyNames):
        if self.database.isExist("HASH", dbName, hashName):
            if self.database.isExpired("HASH", dbName, hashName) is False:
                valueList = self.database.getMultipleHashValues(dbName, hashName, keyNames)
                msg = Utils.makeMessage(responseCode.detail[responseCode.HASH_VALUES_GET_SUCCESS],
                                       responseCode.HASH_VALUES_GET_SUCCESS,
                                       valueList)
            else:
                msg = Utils.makeMessage(responseCode.detail[responseCode.HASH_EXPIRED],
                                       responseCode.HASH_EXPIRED,
                                       hashName)
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.HASH_NOT_EXISTED],
                                   responseCode.HASH_NOT_EXISTED,
                                   hashName)
        return msg

    # insert a key-value data into the given hash
    @validTypeCheck
    def insertHash(self, dbName, hashName, keyName, value):
        if Utils.isValidType(keyName) is False:
            msg = Utils.makeMessage(responseCode.detail[responseCode.ELEM_TYPE_ERROR],
                                   responseCode.ELEM_TYPE_ERROR,
                                   hashName)
            return msg

        if self.database.isExist("HASH", dbName, hashName):
            if self.database.isExpired("HASH", dbName, hashName) is False:
                result = self.database.insertHash(dbName, hashName, keyName, value)
                msg = Utils.makeMessage(responseCode.detail[result],
                                       result,
                                       hashName)
            else:
                msg = Utils.makeMessage(responseCode.detail[responseCode.HASH_EXPIRED],
                                       responseCode.HASH_EXPIRED,
                                       hashName)
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.HASH_NOT_EXISTED],
                                   responseCode.HASH_NOT_EXISTED,
                                   hashName)
        return msg

    # check if a key exists in the given hash
    @validTypeCheck
    def isKeyExist(self, dbName, hashName, keyName):
        if self.database.isExist("HASH", dbName, hashName):
            if self.database.isExpired("HASH", dbName, hashName) is False:
                result = self.database.isKeyExist(dbName, hashName, keyName)
                result = responseCode.HASH_KEY_EXIST if result is True else responseCode.HASH_KEY_NOT_EXIST
                msg = Utils.makeMessage(responseCode.detail[result],
                                       result,
                                       keyName)
            else:
                msg = Utils.makeMessage(responseCode.detail[responseCode.HASH_EXPIRED],
                                       responseCode.HASH_EXPIRED,
                                       hashName)
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.HASH_NOT_EXISTED],
                                   responseCode.HASH_NOT_EXISTED,
                                   hashName)
        return msg

    # delete the given hash
    @validTypeCheck
    def deleteHash(self, dbName, hashName):
        if self.database.isExist("HASH", dbName, hashName):
            result = self.database.deleteHash(dbName, hashName)
            msg = Utils.makeMessage(responseCode.detail[result],
                                   result,
                                   hashName)
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.HASH_NOT_EXISTED],
                                   responseCode.HASH_NOT_EXISTED,
                                   hashName)
        return msg

    # remove a key-value data from the given hash
    @validTypeCheck
    def rmFromHash(self, dbName, hashName, keyName):
        if self.database.isKeyExist(dbName, hashName, keyName):
            if self.database.isExpired("HASH", dbName, hashName) is False:
                result = self.database.rmFromHash(dbName, hashName, keyName)
                msg = Utils.makeMessage(responseCode.detail[result],
                                       result,
                                       hashName)
            else:
                msg = Utils.makeMessage(responseCode.detail[responseCode.HASH_EXPIRED],
                                       responseCode.HASH_EXPIRED,
                                       hashName)
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.HASH_KEY_NOT_EXIST],
                                   responseCode.HASH_KEY_NOT_EXIST,
                                   keyName)
        return msg

    # clear the entire hash
    @validTypeCheck
    def clearHash(self, dbName, hashName):
        if self.database.isExist("HASH", dbName, hashName) is True:
            if self.database.isExpired("HASH", dbName, hashName) is False:
                result = self.database.clearHash(dbName, hashName)
                msg = Utils.makeMessage(responseCode.detail[result],
                                       result,
                                       hashName)
            else:
                msg = Utils.makeMessage(responseCode.detail[responseCode.HASH_EXPIRED],
                                       responseCode.HASH_EXPIRED,
                                       hashName)
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.HASH_NOT_EXISTED],
                                   responseCode.HASH_NOT_EXISTED,
                                   hashName)
        return msg

    # replace the existed hash with a new value
    @validTypeCheck
    def replaceHash(self, dbName, hashName, hashValue):
        if Utils.isDict(hashValue) is False :
            msg = Utils.makeMessage(responseCode.detail[responseCode.ELEM_TYPE_ERROR],
                                   responseCode.ELEM_TYPE_ERROR,
                                   hashName)
        else:
            if self.database.isExist("HASH", dbName, hashName) is True:
                if self.database.isExpired("HASH", dbName, hashName) is False:
                    result = self.database.replaceHash(dbName, hashName, hashValue)
                    msg = Utils.makeMessage(responseCode.detail[result],
                                           result,
                                           hashName)
                else:
                    msg = Utils.makeMessage(responseCode.detail[responseCode.HASH_EXPIRED],
                                           responseCode.HASH_EXPIRED,
                                           hashName)
            else:
                msg = Utils.makeMessage(responseCode.detail[responseCode.HASH_NOT_EXISTED],
                                       responseCode.HASH_NOT_EXISTED,
                                       hashName)
        return msg

    # merge two hashs
    @validTypeCheck
    def mergeHashs(self, dbName, hashName1, hashName2, resultHashName=None, mergeMode=0):
        if Utils.isValidType(hashName2) is False:
            msg = Utils.makeMessage(responseCode.detail[responseCode.ELEM_TYPE_ERROR],
                                   responseCode.ELEM_TYPE_ERROR,
                                   hashName2)
            return msg

        if resultHashName is not None:
            if self.database.isExist("HASH", dbName, resultHashName) is True:
                msg = Utils.makeMessage(responseCode.detail[responseCode.MERGE_RESULT_EXIST],
                                       responseCode.MERGE_RESULT_EXIST,
                                       resultHashName)
                return msg

        if self.database.isExist("HASH", dbName, hashName1, hashName2):
            if self.database.isExpired("HASH", dbName, hashName1, hashName2) is False:
                result = self.database.mergeHashs(dbName, hashName1,
                                                  hashName2, resultHashName,
                                                  mergeMode)
                msg = Utils.makeMessage(responseCode.detail[result],
                                       result,
                                       resultHashName)
            else:
                msg = Utils.makeMessage(responseCode.detail[responseCode.HASH_EXPIRED],
                                       responseCode.HASH_EXPIRED,
                                       "{} or {}".format(hashName1, hashName2))
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.HASH_NOT_EXISTED],
                                   responseCode.HASH_NOT_EXISTED,
                                   "{} or {}".format(hashName1, hashName2))
        return msg

    # search hash names using regular expression
    def searchHash(self, dbName, expression):
        if self.database.isDbExist(dbName) is False:
            msg = Utils.makeMessage(responseCode.detail[responseCode.DB_NOT_EXIST],
                                   responseCode.DB_NOT_EXIST,
                                   dbName)
            return msg

        if Utils.isValidType(dbName):
            searchResult = self.database.searchByRE(dbName, expression, "HASH")
            msg = Utils.makeMessage(responseCode.detail[responseCode.HASH_SEARCH_SUCCESS],
                                   responseCode.HASH_SEARCH_SUCCESS,
                                   searchResult)
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.ELEM_TYPE_ERROR],
                                   responseCode.ELEM_TYPE_ERROR,
                                   dbName)
        return msg

    # return all hash names in the given database
    def searchAllHash(self, dbName):
        if Utils.isValidType(dbName):
            if self.database.isDbExist(dbName):
                searchResult = self.database.searchAllHash(dbName)
                msg = Utils.makeMessage(responseCode.detail[responseCode.HASH_SEARCH_SUCCESS],
                                       responseCode.HASH_SEARCH_SUCCESS,
                                       searchResult)
            else:
                msg = Utils.makeMessage(responseCode.detail[responseCode.DB_NOT_EXIST],
                                       responseCode.DB_NOT_EXIST,
                                       dbName)
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.ELEM_TYPE_ERROR],
                                   responseCode.ELEM_TYPE_ERROR,
                                   dbName)
        return msg

    @validTypeCheck
    def showTTL(self, dbName, keyName):
        if self.database.isDbExist(dbName):
            code, result = self.database.showTTL(dbName, keyName, "HASH")
            msg = Utils.makeMessage(responseCode.detail[code],
                                   code,
                                   result)
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.DB_NOT_EXIST],
                                   responseCode.DB_NOT_EXIST,
                                   dbName)
        return msg

    @validTypeCheck
    def getSize(self, dbName, hashName):
        if self.database.isExist("HASH", dbName, hashName) is False:
            msg = Utils.makeMessage(responseCode.detail[responseCode.HASH_NOT_EXISTED],
                                   responseCode.HASH_NOT_EXISTED,
                                   hashName)
        else:
            code, result = self.database.getSize(dbName, hashName, "HASH")
            msg = Utils.makeMessage(responseCode.detail[code],
                                   code,
                                   result)
        return msg

    @validTypeCheck
    def increaseHash(self, dbName, hashName, keyName):
        if self.database.isKeyExist(dbName, hashName, keyName):
            if self.database.isExpired("HASH", dbName, hashName) is False:
                code, value = self.database.increaseHash(dbName, hashName, keyName)
                msg = Utils.makeMessage(responseCode.detail[code],
                                       code,
                                       value)
            else:
                msg = Utils.makeMessage(responseCode.detail[responseCode.HASH_EXPIRED],
                                       responseCode.HASH_EXPIRED,
                                       hashName)
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.HASH_NOT_EXISTED],
                                   responseCode.HASH_NOT_EXISTED,
                                   hashName)
        return msg

    @validTypeCheck
    def decreaseHash(self, dbName, hashName, keyName):
        if self.database.isKeyExist(dbName, hashName, keyName):
            if self.database.isExpired("HASH", dbName, hashName) is False:
                code, value = self.database.decreaseHash(dbName, hashName, keyName)
                msg = Utils.makeMessage(responseCode.detail[code],
                                       code,
                                       value)
            else:
                msg = Utils.makeMessage(responseCode.detail[responseCode.HASH_EXPIRED],
                                       responseCode.HASH_EXPIRED,
                                       hashName)
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.HASH_NOT_EXISTED],
                                   responseCode.HASH_NOT_EXISTED,
                                   hashName)
        return msg
