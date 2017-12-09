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
    def getKeySet(self, dbName, keyName):
        if self.database.isExist("HASH", dbName, keyName):
            if self.database.isExpired("HASH", dbName, keyName) is False:
                keySet = self.database.getHashKeySet(dbName, keyName)
                msg = Utils.makeMessage(responseCode.detail[responseCode.HASH_KEYSET_GET_SUCCESS],
                                        responseCode.HASH_KEYSET_GET_SUCCESS,
                                        keySet)
            else:
                msg = Utils.makeMessage(responseCode.detail[responseCode.HASH_EXPIRED],
                                        responseCode.HASH_EXPIRED,
                                        keyName)
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.HASH_NOT_EXISTED],
                                    responseCode.HASH_NOT_EXISTED,
                                    keyName)
        return msg

    @validTypeCheck
    def getValues(self, dbName, keyName):
        if self.database.isExist("HASH", dbName, keyName):
            if self.database.isExpired("HASH", dbName, keyName) is False:
                valueList = self.database.getHashValues(dbName, keyName)
                msg = Utils.makeMessage(responseCode.detail[responseCode.HASH_GET_SUCCESS],
                                        responseCode.HASH_VALUES_GET_SUCCESS,
                                        valueList)
            else:
                msg = Utils.makeMessage(responseCode.detail[responseCode.HASH_EXPIRED],
                                        responseCode.HASH_EXPIRED,
                                        keyName)
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.HASH_NOT_EXISTED],
                                    responseCode.HASH_NOT_EXISTED,
                                    keyName)
        return msg

    @validTypeCheck
    def getMultipleValues(self, dbName, keyName, keys):
        if self.database.isExist("HASH", dbName, keyName):
            if self.database.isExpired("HASH", dbName, keyName) is False:
                valueList = self.database.getMultipleHashValues(dbName, keyName, keys)
                msg = Utils.makeMessage(responseCode.detail[responseCode.HASH_VALUES_GET_SUCCESS],
                                        responseCode.HASH_VALUES_GET_SUCCESS,
                                        valueList)
            else:
                msg = Utils.makeMessage(responseCode.detail[responseCode.HASH_EXPIRED],
                                        responseCode.HASH_EXPIRED,
                                        keyName)
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.HASH_NOT_EXISTED],
                                    responseCode.HASH_NOT_EXISTED,
                                    keyName)
        return msg

    # insert a key-value data into the given hash
    @validTypeCheck
    def insertHash(self, dbName, keyName, key, value):
        if Utils.isValidType(keyName) is False:
            msg = Utils.makeMessage(responseCode.detail[responseCode.ELEM_TYPE_ERROR],
                                   responseCode.ELEM_TYPE_ERROR,
                                   keyName)
            return msg

        if self.database.isExist("HASH", dbName, keyName):
            if self.database.isExpired("HASH", dbName, keyName) is False:
                result = self.database.insertHash(dbName, keyName, key, value)
                msg = Utils.makeMessage(responseCode.detail[result],
                                        result,
                                        keyName)
            else:
                msg = Utils.makeMessage(responseCode.detail[responseCode.HASH_EXPIRED],
                                        responseCode.HASH_EXPIRED,
                                        keyName)
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.HASH_NOT_EXISTED],
                                    responseCode.HASH_NOT_EXISTED,
                                    keyName)
        return msg

    # check if a key exists in the given hash
    @validTypeCheck
    def isKeyExist(self, dbName, keyName, key):
        if self.database.isExist("HASH", dbName, keyName):
            if self.database.isExpired("HASH", dbName, keyName) is False:
                result = self.database.isKeyExist(dbName, keyName, key)
                result = responseCode.HASH_KEY_EXIST if result is True else responseCode.HASH_KEY_NOT_EXIST
                msg = Utils.makeMessage(responseCode.detail[result],
                                        result,
                                        key)
            else:
                msg = Utils.makeMessage(responseCode.detail[responseCode.HASH_EXPIRED],
                                        responseCode.HASH_EXPIRED,
                                        keyName)
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.HASH_NOT_EXISTED],
                                    responseCode.HASH_NOT_EXISTED,
                                    keyName)
        return msg

    # delete the given hash
    @validTypeCheck
    def deleteHash(self, dbName, keyName):
        if self.database.isExist("HASH", dbName, keyName):
            result = self.database.deleteHash(dbName, keyName)
            msg = Utils.makeMessage(responseCode.detail[result],
                                    result,
                                    keyName)
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.HASH_NOT_EXISTED],
                                    responseCode.HASH_NOT_EXISTED,
                                    keyName)
        return msg

    # remove a key-value data from the given hash
    @validTypeCheck
    def rmFromHash(self, dbName, keyName, key):
        if self.database.isKeyExist(dbName, keyName, key):
            if self.database.isExpired("HASH", dbName, keyName) is False:
                result = self.database.rmFromHash(dbName, keyName, key)
                msg = Utils.makeMessage(responseCode.detail[result],
                                        result,
                                        keyName)
            else:
                msg = Utils.makeMessage(responseCode.detail[responseCode.HASH_EXPIRED],
                                        responseCode.HASH_EXPIRED,
                                        keyName)
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.HASH_KEY_NOT_EXIST],
                                    responseCode.HASH_KEY_NOT_EXIST,
                                    key)
        return msg

    # clear the entire hash
    @validTypeCheck
    def clearHash(self, dbName, keyName):
        if self.database.isExist("HASH", dbName, keyName) is True:
            if self.database.isExpired("HASH", dbName, keyName) is False:
                result = self.database.clearHash(dbName, keyName)
                msg = Utils.makeMessage(responseCode.detail[result],
                                        result,
                                        keyName)
            else:
                msg = Utils.makeMessage(responseCode.detail[responseCode.HASH_EXPIRED],
                                        responseCode.HASH_EXPIRED,
                                        keyName)
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.HASH_NOT_EXISTED],
                                    responseCode.HASH_NOT_EXISTED,
                                    keyName)
        return msg

    # replace the existed hash with a new value
    @validTypeCheck
    def replaceHash(self, dbName, keyName, value):
        if Utils.isDict(value) is False :
            msg = Utils.makeMessage(responseCode.detail[responseCode.ELEM_TYPE_ERROR],
                                    responseCode.ELEM_TYPE_ERROR,
                                    keyName)
        else:
            if self.database.isExist("HASH", dbName, keyName) is True:
                if self.database.isExpired("HASH", dbName, keyName) is False:
                    result = self.database.replaceHash(dbName, keyName, value)
                    msg = Utils.makeMessage(responseCode.detail[result],
                                            result,
                                            keyName)
                else:
                    msg = Utils.makeMessage(responseCode.detail[responseCode.HASH_EXPIRED],
                                            responseCode.HASH_EXPIRED,
                                            keyName)
            else:
                msg = Utils.makeMessage(responseCode.detail[responseCode.HASH_NOT_EXISTED],
                                        responseCode.HASH_NOT_EXISTED,
                                        keyName)
        return msg

    # merge two hashs
    @validTypeCheck
    def mergeHashs(self, dbName, keyName1, keyName2, resultKeyName=None, mergeMode=0):
        if Utils.isValidType(keyName2) is False:
            msg = Utils.makeMessage(responseCode.detail[responseCode.ELEM_TYPE_ERROR],
                                    responseCode.ELEM_TYPE_ERROR,
                                    keyName2)
            return msg

        if resultKeyName is not None:
            if self.database.isExist("HASH", dbName, resultKeyName) is True:
                msg = Utils.makeMessage(responseCode.detail[responseCode.MERGE_RESULT_EXIST],
                                        responseCode.MERGE_RESULT_EXIST,
                                        resultKeyName)
                return msg

        if self.database.isExist("HASH", dbName, keyName1, keyName2):
            if self.database.isExpired("HASH", dbName, keyName1, keyName2) is False:
                result = self.database.mergeHashs(dbName, keyName1,
                                                  keyName2, resultKeyName,
                                                  mergeMode)
                msg = Utils.makeMessage(responseCode.detail[result],
                                        result,
                                        resultKeyName)
            else:
                msg = Utils.makeMessage(responseCode.detail[responseCode.HASH_EXPIRED],
                                       responseCode.HASH_EXPIRED,
                                       "{} or {}".format(keyName1, keyName2))
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.HASH_NOT_EXISTED],
                                   responseCode.HASH_NOT_EXISTED,
                                   "{} or {}".format(keyName1, keyName2))
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
    def getSize(self, dbName, keyName):
        if self.database.isExist("HASH", dbName, keyName) is False:
            msg = Utils.makeMessage(responseCode.detail[responseCode.HASH_NOT_EXISTED],
                                    responseCode.HASH_NOT_EXISTED,
                                    keyName)
        else:
            code, result = self.database.getSize(dbName, keyName, "HASH")
            msg = Utils.makeMessage(responseCode.detail[code],
                                    code,
                                    result)
        return msg

    @validTypeCheck
    def increaseHash(self, dbName, keyName, key):
        if self.database.isKeyExist(dbName, keyName, key):
            if self.database.isExpired("HASH", dbName, keyName) is False:
                code, value = self.database.increaseHash(dbName, keyName, key)
                msg = Utils.makeMessage(responseCode.detail[code],
                                        code,
                                        value)
            else:
                msg = Utils.makeMessage(responseCode.detail[responseCode.HASH_EXPIRED],
                                        responseCode.HASH_EXPIRED,
                                        keyName)
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.HASH_NOT_EXISTED],
                                    responseCode.HASH_NOT_EXISTED,
                                    keyName)
        return msg

    @validTypeCheck
    def decreaseHash(self, dbName, keyName, key):
        if self.database.isKeyExist(dbName, keyName, key):
            if self.database.isExpired("HASH", dbName, keyName) is False:
                code, value = self.database.decreaseHash(dbName, keyName, key)
                msg = Utils.makeMessage(responseCode.detail[code],
                                        code,
                                        value)
            else:
                msg = Utils.makeMessage(responseCode.detail[responseCode.HASH_EXPIRED],
                                        responseCode.HASH_EXPIRED,
                                        keyName)
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.HASH_NOT_EXISTED],
                                    responseCode.HASH_NOT_EXISTED,
                                    keyName)
        return msg
