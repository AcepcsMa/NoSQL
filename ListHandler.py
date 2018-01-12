__author__ = 'Ma Haoxiang'

# import
from Decorator import *
from Utils import Utils

class ListHandler(object):

    def __init__(self, database):
        self.database = database

    # create a list in the database
    @validTypeCheck
    def createList(self, dbName, keyName, password=None):
        if self.database.isDbExist(dbName):
            if self.database.isExist("LIST", dbName, keyName) is False:
                code = self.database.createList(dbName=dbName,
                                                  keyName=keyName,
                                                  password=password)
            else:
                code = responseCode.LIST_ALREADY_EXIST
            result = keyName
        else:
            code, result = responseCode.DB_NOT_EXIST, dbName
        msg = Utils.makeMessage(responseCode.detail[code],
                                code,
                                result)
        return msg

    # get the value of a given list
    @validTypeCheck
    def getList(self, dbName, keyName, password=None):
        if self.database.isExist("LIST", dbName, keyName) is True:
            if self.database.isExpired("LIST", dbName, keyName) is False:
                result = self.database.getList(dbName=dbName,
                                               keyName=keyName,
                                               password=password)
                code = result if result == responseCode.DB_PASSWORD_ERROR else result[0]
                result = dbName if result == responseCode.DB_PASSWORD_ERROR else result[1]
            else:
                code, result = responseCode.LIST_EXPIRED, keyName
        else:
            code, result = responseCode.LIST_NOT_EXIST, keyName
        msg = Utils.makeMessage(responseCode.detail[code],
                                code,
                                result)
        return msg

    # get values of a given list from start index to end index
    @validTypeCheck
    def getListByRange(self, dbName, keyName, start, end, password=None):
        if start > end:
            return Utils.makeMessage(responseCode.detail[responseCode.LIST_RANGE_ERROR],
                                    responseCode.LIST_RANGE_ERROR,
                                    keyName)

        if self.database.isExist("LIST", dbName, keyName) is True:
            if self.database.isExpired("LIST", dbName, keyName) is False:
                result = self.database.getList(dbName=dbName, keyName=keyName,
                                               start=start, end=end,
                                               password=password)
                code = result if result == responseCode.DB_PASSWORD_ERROR else result[0]
                result = dbName if result == responseCode.DB_PASSWORD_ERROR else result[1]
            else:
                code, result = responseCode.LIST_EXPIRED, keyName
        else:
            code, result = responseCode.LIST_NOT_EXIST, keyName
        msg = Utils.makeMessage(responseCode.detail[code],
                                code,
                                result)
        return msg

    # left get from list
    @validTypeCheck
    def getListL(self, dbName, keyName, count, password=None):
        if self.database.isExist("LIST", dbName, keyName) is True:
            if self.database.isExpired("LIST", dbName, keyName) is False:
                result = self.database.getList(dbName=dbName,
                                               keyName=keyName,
                                               start=0,
                                               end=count,
                                               password=password)
                code = result if result == responseCode.DB_PASSWORD_ERROR else result[0]
                result = dbName if result == responseCode.DB_PASSWORD_ERROR else result[1]
            else:
                code, result = responseCode.LIST_EXPIRED, keyName
        else:
            code, result = responseCode.LIST_NOT_EXIST, keyName
        msg = Utils.makeMessage(responseCode.detail[code],
                                code,
                                result)
        return msg

    @validTypeCheck
    def getListR(self, dbName, keyName, count, password=None):
        if self.database.isExist("LIST", dbName, keyName) is True:
            if self.database.isExpired("LIST", dbName, keyName) is False:
                result = self.database.getList(dbName=dbName,
                                               keyName=keyName,
                                               start=-count,
                                               password=password)
                code = result if result == responseCode.DB_PASSWORD_ERROR else result[0]
                result = dbName if result == responseCode.DB_PASSWORD_ERROR else result[1]
            else:
                code, result = responseCode.LIST_EXPIRED, keyName
        else:
            code, result = responseCode.LIST_NOT_EXIST, keyName
        msg = Utils.makeMessage(responseCode.detail[code],
                                code,
                                result)
        return msg

    @validTypeCheck
    def getListRandom(self, dbName, keyName, numRand, password=None):
        if numRand <= 0:
            msg = Utils.makeMessage(responseCode.detail[responseCode.INVALID_NUMBER],
                                    responseCode.INVALID_NUMBER,
                                    numRand)
            return msg
        if self.database.isExist("LIST", dbName, keyName) is True:
            if self.database.isExpired("LIST", dbName, keyName) is False:
                code, result = self.database.getListRandom(dbName=dbName, keyName=keyName,
                                                              numRand=numRand, password=password)
            else:
                code, result = responseCode.LIST_EXPIRED, keyName
        else:
            code, result = responseCode.LIST_NOT_EXIST, keyName
        msg = Utils.makeMessage(responseCode.detail[code],
                                code,
                                result)
        return msg

    # insert a value into the given list
    @validTypeCheck
    def insertList(self, dbName, keyName, value, password=None):
        if self.database.isExist("LIST", dbName, keyName) is True:
            if self.database.isExpired("LIST", dbName, keyName) is False:
                code = self.database.insertList(dbName=dbName, keyName=keyName,
                                                value=value, password=password)
            else:
                code = responseCode.LIST_EXPIRED
        else:
            code = responseCode.LIST_NOT_EXIST
        msg = Utils.makeMessage(responseCode.detail[code],
                                code,
                                keyName)
        return msg

    # insert a value into the given list
    @validTypeCheck
    def insertListL(self, dbName, keyName, value, password=None):
        if self.database.isExist("LIST", dbName, keyName) is True:
            if self.database.isExpired("LIST", dbName, keyName) is False:
                code = self.database.insertList(dbName=dbName, keyName=keyName,
                                                value=value, isLeft=True,
                                                password=password)
            else:
                code = responseCode.LIST_EXPIRED
        else:
            code = responseCode.LIST_NOT_EXIST
        msg = Utils.makeMessage(responseCode.detail[code],
                                code,
                                keyName)
        return msg

    # delete a list in the database
    @validTypeCheck
    def deleteList(self, dbName, keyName, password=None):
        if self.database.isExist("LIST", dbName, keyName) is True:
            code = self.database.deleteList(dbName=dbName,
                                            keyName=keyName,
                                            password=password)
        else:
            code = responseCode.LIST_NOT_EXIST
        msg = Utils.makeMessage(responseCode.detail[code],
                                code,
                                keyName)
        return msg

    # remove a value from the given list
    @validTypeCheck
    def rmFromList(self, dbName, keyName, value, password=None):
        if self.database.isExist("LIST", dbName, keyName) is True:
            if self.database.isExpired("LIST", dbName, keyName) is False:
                code = self.database.rmFromList(dbName=dbName, keyName=keyName,
                                                value=value, password=password)
            else:
                code = responseCode.LIST_EXPIRED
        else:   # if list does not exist
            code = responseCode.LIST_NOT_EXIST
        msg = Utils.makeMessage(responseCode.detail[code],
                                code,
                                keyName)
        return msg

    # clear the given list
    @validTypeCheck
    def clearList(self, dbName, keyName, password=None):
        if self.database.isExist("LIST", dbName, keyName) is True:
            if self.database.isExpired("LIST", dbName, keyName) is False:
                code = self.database.clearList(dbName=dbName,
                                               keyName=keyName,
                                               password=password)
            else:
                code = responseCode.LIST_EXPIRED
        else:
            code = responseCode.LIST_NOT_EXIST
        msg = Utils.makeMessage(responseCode.detail[code],
                                code,
                                keyName)
        return msg

    # merge two lists
    def mergeLists(self, dbName, keyName1, keyName2, resultKeyName=None, password=None):
        if Utils.isValidType(dbName, keyName1, keyName2) is False:
            return Utils.makeMessage(responseCode.detail[responseCode.ELEM_TYPE_ERROR],
                                    responseCode.ELEM_TYPE_ERROR,
                                    dbName)

        if resultKeyName is not None:
            if self.database.isExist("LIST", dbName, resultKeyName) is True:
                msg = Utils.makeMessage(responseCode.detail[responseCode.MERGE_RESULT_EXIST],
                                        responseCode.MERGE_RESULT_EXIST,
                                        resultKeyName)
                return msg

        if self.database.isExist("LIST", dbName, keyName1, keyName2):
            if self.database.isExpired("LIST", dbName, keyName1, keyName2) is False:
                code, result = self.database.mergeLists(dbName=dbName, keyName1=keyName1,
                                                        keyName2=keyName2, resultKeyName=resultKeyName,
                                                        password=password)
                msg = Utils.makeMessage(responseCode.detail[code],
                                        code,
                                        result)
            else:
                msg = Utils.makeMessage(responseCode.detail[responseCode.LIST_EXPIRED],
                                        responseCode.LIST_EXPIRED,
                                        "{} or {}".format(keyName1, keyName2))
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.LIST_NOT_EXIST],
                                    responseCode.LIST_NOT_EXIST,
                                    "{} or {}".format(keyName1, keyName2))
        return msg

    # search list names using regular expression
    def searchList(self, dbName, expression, password=None):
        if Utils.isValidType(dbName):
            searchResult = self.database.searchByRE(dbName=dbName, expression=expression,
                                                    dataType="LIST", password=password)
            msg = Utils.makeMessage(responseCode.detail[responseCode.LIST_SEARCH_SUCCESS], 
                                   responseCode.LIST_SEARCH_SUCCESS, 
                                   searchResult)
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.ELEM_TYPE_ERROR],
                                    responseCode.ELEM_TYPE_ERROR,
                                    dbName)
        return msg

    # get all list names in the given database
    def searchAllList(self, dbName, password=None):
        if Utils.isValidType(dbName):
            searchResult = self.database.searchAllList(dbName=dbName,
                                                       password=password)
            msg = Utils.makeMessage(responseCode.detail[responseCode.LIST_SEARCH_SUCCESS],
                                    responseCode.LIST_SEARCH_SUCCESS,
                                    searchResult)
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.ELEM_TYPE_ERROR],
                                    responseCode.ELEM_TYPE_ERROR,
                                    dbName)
        return msg

    @validTypeCheck
    def getSize(self, dbName, keyName, password=None):
        if self.database.isExist("LIST", dbName, keyName) is False:
            code, result = responseCode.LIST_NOT_EXIST, keyName
        else:
            code, result = self.database.getSize(dbName=dbName, keyName=keyName,
                                                 type="LIST", password=password)
        msg = Utils.makeMessage(responseCode.detail[code],
                                code,
                                result)
        return msg
