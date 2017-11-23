__author__ = 'Ma Haoxiang'

# import
from Decorator import *
from Utils import Utils

class ListHandler(object):

    def __init__(self, database):
        self.database = database

    # create a list in the database
    @validTypeCheck
    def createList(self, dbName, listName):
        if self.database.isDbExist(dbName):
            if self.database.isExist("LIST", dbName, listName) is False:
                result = self.database.createList(dbName, listName)
                msg = Utils.makeMessage(responseCode.detail[result],
                                       result,
                                       listName)
            else:
                msg = Utils.makeMessage(responseCode.detail[responseCode.LIST_ALREADY_EXIST],
                                       responseCode.LIST_ALREADY_EXIST,
                                       listName)
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.DB_NOT_EXIST],
                                   responseCode.DB_NOT_EXIST,
                                   dbName)
        return msg

    # get the value of a given list
    @validTypeCheck
    def getList(self, dbName, listName):
        if self.database.isExist("LIST", dbName, listName) is True:
            if self.database.isExpired("LIST", dbName, listName) is False:
                listValue = self.database.getList(listName, dbName)
                msg = Utils.makeMessage(responseCode.detail[responseCode.LIST_GET_SUCCESS],
                                       responseCode.LIST_GET_SUCCESS,
                                       listValue)
            else:
                msg = Utils.makeMessage(responseCode.detail[responseCode.LIST_EXPIRED],
                                       responseCode.LIST_EXPIRED,
                                       listName)
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.LIST_NOT_EXIST],
                                   responseCode.LIST_NOT_EXIST,
                                   listName)
        return msg

    # get values of a given list from start index to end index
    @validTypeCheck
    def getListByRange(self, dbName, listName, start, end):
        if start > end:
            msg = Utils.makeMessage(responseCode.detail[responseCode.LIST_RANGE_ERROR],
                                   responseCode.LIST_RANGE_ERROR,
                                   listName)
            return msg

        if self.database.isExist("LIST", dbName, listName) is True:
            if self.database.isExpired("LIST", dbName, listName) is False:
                listValue = self.database.getList(listName, dbName, start, end)
                msg = Utils.makeMessage(responseCode.detail[responseCode.LIST_GET_SUCCESS],
                                       responseCode.LIST_GET_SUCCESS,
                                       listValue)
            else:
                msg = Utils.makeMessage(responseCode.detail[responseCode.LIST_EXPIRED],
                                       responseCode.LIST_EXPIRED,
                                       listName)
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.LIST_NOT_EXIST],
                                   responseCode.LIST_NOT_EXIST,
                                   listName)
        return msg

    # left get from list
    @validTypeCheck
    def getListL(self, dbName, listName, count):
        if self.database.isExist("LIST", dbName, listName) is True:
            if self.database.isExpired("LIST", dbName, listName) is False:
                listValue = self.database.getList(listName, dbName, 0, count)
                msg = Utils.makeMessage(responseCode.detail[responseCode.LIST_GET_SUCCESS],
                                       responseCode.LIST_GET_SUCCESS,
                                       listValue)
            else:
                msg = Utils.makeMessage(responseCode.detail[responseCode.LIST_EXPIRED],
                                       responseCode.LIST_EXPIRED,
                                       listName)
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.LIST_NOT_EXIST],
                                   responseCode.LIST_NOT_EXIST,
                                   listName)
        return msg

    @validTypeCheck
    def getListR(self, dbName, listName, count):
        if self.database.isExist("LIST", dbName, listName) is True:
            if self.database.isExpired("LIST", dbName, listName) is False:
                listValue = self.database.getList(listName, dbName, -count)
                msg = Utils.makeMessage(responseCode.detail[responseCode.LIST_GET_SUCCESS],
                                       responseCode.LIST_GET_SUCCESS,
                                       listValue)
            else:
                msg = Utils.makeMessage(responseCode.detail[responseCode.LIST_EXPIRED],
                                       responseCode.LIST_EXPIRED,
                                       listName)
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.LIST_NOT_EXIST],
                                   responseCode.LIST_NOT_EXIST,
                                   listName)
        return msg

    @validTypeCheck
    def getListRandom(self, dbName, listName, numRand):
        if numRand <= 0:
            msg = Utils.makeMessage(responseCode.detail[responseCode.INVALID_NUMBER],
                                   responseCode.INVALID_NUMBER,
                                   numRand)
            return msg
        if self.database.isExist("LIST", dbName, listName) is True:
            if self.database.isExpired("LIST", dbName, listName) is False:
                code, listValue = self.database.getListRandom(dbName, listName, numRand)
                msg = Utils.makeMessage(responseCode.detail[code],
                                       code,
                                       listValue)
            else:
                msg = Utils.makeMessage(responseCode.detail[responseCode.LIST_EXPIRED],
                                       responseCode.LIST_EXPIRED,
                                       listName)
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.LIST_NOT_EXIST],
                                   responseCode.LIST_NOT_EXIST,
                                   listName)
        return msg

    # insert a value into the given list
    @validTypeCheck
    def insertList(self, dbName, listName, listValue):
        if self.database.isExist("LIST", dbName, listName) is True:
            if self.database.isExpired("LIST", dbName, listName) is False:
                result = self.database.insertList(listName, listValue, dbName)
                msg = Utils.makeMessage(responseCode.detail[result], 
                                       result, 
                                       listName)
            else:
                msg = Utils.makeMessage(responseCode.detail[responseCode.LIST_EXPIRED], 
                                       responseCode.LIST_EXPIRED, 
                                       listName)
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.LIST_NOT_EXIST], 
                                   responseCode.LIST_NOT_EXIST, 
                                   listName)
        return msg

    # insert a value into the given list
    @validTypeCheck
    def insertListL(self, dbName, listName, listValue):
        if self.database.isExist("LIST", dbName, listName) is True:
            if self.database.isExpired("LIST", dbName, listName) is False:
                result = self.database.insertList(listName, listValue, dbName, True)
                msg = Utils.makeMessage(responseCode.detail[result],
                                       result,
                                       listName)
            else:
                msg = Utils.makeMessage(responseCode.detail[responseCode.LIST_EXPIRED],
                                       responseCode.LIST_EXPIRED,
                                       listName)
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.LIST_NOT_EXIST],
                                   responseCode.LIST_NOT_EXIST,
                                   listName)
        return msg

    # delete a list in the database
    @validTypeCheck
    def deleteList(self, dbName, listName):
        if self.database.isExist("LIST", dbName, listName) is True:
            result = self.database.deleteList(listName, dbName)
            msg = Utils.makeMessage(responseCode.detail[result],
                                   result,
                                   listName)
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.LIST_NOT_EXIST],
                                   responseCode.LIST_NOT_EXIST,
                                   listName)
        return msg

    # remove a value from the given list
    @validTypeCheck
    def rmFromList(self, dbName, listName, value):
        if self.database.isExist("LIST", dbName, listName) is True:
            if self.database.isExpired("LIST", dbName, listName) is False:
                result = self.database.rmFromList(dbName, listName, value)
                msg = Utils.makeMessage(responseCode.detail[result],
                                       result,
                                       listName)
            else:
                msg = Utils.makeMessage(responseCode.detail[responseCode.LIST_EXPIRED],
                                       responseCode.LIST_EXPIRED,
                                       listName)
        else:   # if list does not exist
            msg = Utils.makeMessage(responseCode.detail[responseCode.LIST_NOT_EXIST],
                                   responseCode.LIST_NOT_EXIST,
                                   listName)
        return msg

    # clear the given list
    @validTypeCheck
    def clearList(self, dbName, listName):
        if self.database.isExist("LIST", dbName, listName) is True:
            if self.database.isExpired("LIST", dbName, listName) is False:
                result = self.database.clearList(dbName, listName)
                msg = Utils.makeMessage(responseCode.detail[result],
                                       result,
                                       listName)
            else:
                msg = Utils.makeMessage(responseCode.detail[responseCode.LIST_EXPIRED],
                                       responseCode.LIST_EXPIRED,
                                       listName)
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.LIST_NOT_EXIST],
                                   responseCode.LIST_NOT_EXIST,
                                   listName)
        return msg

    # merge two lists
    def mergeLists(self, dbName, listName1, listName2, resultListName=None):
        if Utils.isValidType(dbName, listName1, listName2) is False:
            msg = Utils.makeMessage(responseCode.detail[responseCode.ELEM_TYPE_ERROR],
                                   responseCode.ELEM_TYPE_ERROR,
                                   dbName)
            return msg

        if resultListName is not None:
            if self.database.isExist("LIST", dbName, resultListName) is True:
                msg = Utils.makeMessage(responseCode.detail[responseCode.MERGE_RESULT_EXIST],
                                       responseCode.MERGE_RESULT_EXIST,
                                       resultListName)
                return msg

        if self.database.isExist("LIST", dbName, listName1, listName2):
            if self.database.isExpired("LIST", dbName, listName1, listName2) is False:
                code, result = self.database.mergeLists(dbName, listName1, listName2, resultListName)
                msg = Utils.makeMessage(responseCode.detail[code],
                                       code,
                                       result)
            else:
                msg = Utils.makeMessage(responseCode.detail[responseCode.LIST_EXPIRED],
                                       responseCode.LIST_EXPIRED,
                                       "{} or {}".format(listName1, listName2))
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.LIST_NOT_EXIST],
                                   responseCode.LIST_NOT_EXIST,
                                   "{} or {}".format(listName1, listName2))
        return msg

    # search list names using regular expression
    def searchList(self, dbName, expression):
        if Utils.isValidType(dbName):
            searchResult = self.database.searchByRE(dbName, expression, "LIST")
            msg = Utils.makeMessage(responseCode.detail[responseCode.LIST_SEARCH_SUCCESS], 
                                   responseCode.LIST_SEARCH_SUCCESS, 
                                   searchResult)
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.ELEM_TYPE_ERROR], responseCode.ELEM_TYPE_ERROR, dbName)
        return msg

    # get all list names in the given database
    def searchAllList(self, dbName):
        if Utils.isValidType(dbName):
            searchResult = self.database.searchAllList(dbName)
            msg = Utils.makeMessage(responseCode.detail[responseCode.LIST_SEARCH_SUCCESS],
                                   responseCode.LIST_SEARCH_SUCCESS,
                                   searchResult)
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.ELEM_TYPE_ERROR],
                                   responseCode.ELEM_TYPE_ERROR,
                                   dbName)
        return msg
    
    @validTypeCheck
    def showTTL(self, dbName, keyName):
        if self.database.isDbExist(dbName):
            code, result = self.database.showTTL(dbName, keyName, "LIST")
            msg = Utils.makeMessage(responseCode.detail[code],
                                   code,
                                   result)
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.DB_NOT_EXIST],
                                   responseCode.DB_NOT_EXIST,
                                   dbName)
        return msg

    @validTypeCheck
    def getSize(self, dbName, listName):
        if self.database.isExist("LIST", dbName, listName) is False:
            msg = Utils.makeMessage(responseCode.detail[responseCode.LIST_NOT_EXIST],
                                   responseCode.LIST_NOT_EXIST,
                                   listName)
        else:
            code, result = self.database.getSize(dbName, listName, "LIST")
            msg = Utils.makeMessage(responseCode.detail[code],
                                   code,
                                   result)
        return msg
