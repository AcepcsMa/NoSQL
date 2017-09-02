__author__ = 'Ma Haoxiang'

# import
from response import responseCode
from decorator import *

class listHandler:
    def __init__(self, database):
        self.database = database

    # check if the type of elem is valid (string or int)
    def isValidType(self, elem):
        return 'str' in str(type(elem)) or 'int' in str(type(elem))

    # check if the type of an elem is INT
    def isInt(self, elem):
        return "int" in str(type(elem))

    # make the response message
    def makeMessage(self, msg, typeCode, data):
        message = {
            "msg":msg,
            "typeCode":typeCode,
            "data":data
        }
        return message

    # create a list in the database
    @validTypeCheck
    def createList(self, dbName, listName):
        if(self.database.isDbExist(dbName)):
            if(self.database.isListExist(dbName, listName) is False):
                result = self.database.createList(dbName, listName)
                msg = self.makeMessage(responseCode.detail[result],result,listName)
            else:
                msg = self.makeMessage("List Already Exists", responseCode.LIST_ALREADY_EXIST, listName)
        else:
            msg = self.makeMessage("Database Does Not Exist", responseCode.DB_NOT_EXIST, dbName)
        return msg

    # get the value of a given list
    @validTypeCheck
    def getList(self, dbName, listName):
        if(self.database.isListExist(dbName, listName) is True):
            if(self.database.isExpired(dbName, listName, "LIST") is False):
                listValue = self.database.getList(listName, dbName)
                msg = self.makeMessage("Get List Success", responseCode.LIST_GET_SUCCESS, listValue)
            else:
                msg = self.makeMessage("List Is Expired", responseCode.LIST_EXPIRED, listName)
        else:
            msg = self.makeMessage("List Does Not Exist", responseCode.LIST_NOT_EXIST, listName)
        return msg

    # insert a value into the given list
    @validTypeCheck
    def insertList(self, dbName, listName, listValue):
        if(self.database.isListExist(dbName, listName) is True):
            if(self.database.isExpired(dbName, listName, "LIST") is False):
                result = self.database.insertList(listName, listValue, dbName)
                msg = self.makeMessage(responseCode.detail[result],result,listName)
            else:
                msg = self.makeMessage("List Is Expired", responseCode.LIST_EXPIRED, listName)
        else:
            msg = self.makeMessage("List Does Not Exist", responseCode.LIST_NOT_EXIST, listName)
        return msg

    # delete a list in the database
    @validTypeCheck
    def deleteList(self, dbName, listName):
        if(self.database.isListExist(dbName, listName) is True):
            result = self.database.deleteList(listName, dbName)
            msg = self.makeMessage(responseCode.detail[result],result,listName)
        else:
            msg = self.makeMessage("List Does Not Exist", responseCode.LIST_NOT_EXIST, listName)
        return msg

    # remove a value from the given list
    @validTypeCheck
    def rmFromList(self, dbName, listName, value):
        if(self.database.isListExist(dbName, listName) is True):
            if(self.database.isExpired(dbName, listName, "LIST") is False):
                result = self.database.rmFromList(dbName, listName, value)
                msg = self.makeMessage(responseCode.detail[result],result,listName)
            else:
                msg = self.makeMessage("List Is Expired", responseCode.LIST_EXPIRED, listName)
        else:   # if list does not exist
            msg = self.makeMessage("List Does Not Exist", responseCode.LIST_NOT_EXIST, listName)
        return msg

    # clear the given list
    @validTypeCheck
    def clearList(self, dbName, listName):
        if(self.database.isListExist(dbName, listName) is True):
            if(self.database.isExpired(dbName, listName, "LIST") is False):
                result = self.database.clearList(dbName, listName)
                msg = self.makeMessage(responseCode.detail[result],result,listName)
            else:
                msg = self.makeMessage("List Is Expired", responseCode.LIST_EXPIRED, listName)
        else:
            msg = self.makeMessage("List Does Not Exist", responseCode.LIST_NOT_EXIST, listName)
        return msg

    # merge two lists
    def mergeLists(self, dbName, listName1, listName2, resultListName=None):
        if(self.isValidType(dbName) is False or self.isValidType(listName1) is False
           or self.isValidType(listName2) is False):
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, dbName)
            return msg

        if(resultListName is not None):
            if(self.database.isListExist(dbName, resultListName) is True):
                msg = self.makeMessage("Merge Result Exists", responseCode.MERGE_RESULT_EXIST, resultListName)
                return msg

        if(self.database.isListExist(dbName, listName1)
           and self.database.isListExist(dbName, listName2)):
            if(self.database.isExpired(dbName, listName1, "LIST") is False
               and self.database.isExpired(dbName, listName2, "LIST") is False):
                result = self.database.mergeLists(dbName, listName1, listName2, resultListName)
                if(result == responseCode.LIST_IS_LOCKED):
                    msg = self.makeMessage("List Is Locked", responseCode.LIST_IS_LOCKED, resultListName)
                elif("list" in str(type(result))):
                    msg = self.makeMessage("List Merge Success", responseCode.LIST_MERGE_SUCCESS, result)
                else:
                    msg = self.makeMessage("Database Error", responseCode.DB_ERROR, dbName)
            else:
                msg = self.makeMessage("List Is Expired", responseCode.LIST_EXPIRED, "{} or {}".format(listName1, listName2))
        else:
            msg = self.makeMessage("List Does Not Exist", responseCode.LIST_NOT_EXIST, "{} or {}".format(listName1, listName2))
        return msg

    # search list names using regular expression
    def searchList(self, dbName, expression):
        if(self.isValidType(dbName)):
            searchResult = self.database.searchByRE(dbName, expression, "LIST")
            msg = self.makeMessage("Search List Success", responseCode.LIST_SEARCH_SUCCESS, searchResult)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, dbName)
        return msg

    # get all list names in the given database
    def searchAllList(self, dbName):
        if(self.isValidType(dbName)):
            searchResult = self.database.searchAllList(dbName)
            msg = self.makeMessage("Search All List Success", responseCode.LIST_SEARCH_SUCCESS, searchResult)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, dbName)
        return msg

    # set TTL for a list
    @validTypeCheck
    def setTTL(self, dbName, listName, ttl):
        if (self.database.isListExist(dbName, listName) is False):
            msg = self.makeMessage("List Does Not Exist", responseCode.LIST_NOT_EXIST, listName)
        else:
            result = self.database.setListTTL(dbName, listName, ttl)
            msg = self.makeMessage(responseCode.detail[result],result,listName)
        return msg

    # clear TTL for a list
    @validTypeCheck
    def clearTTL(self, dbName, listName):
        if (self.database.isListExist(dbName, listName) is False):
            msg = self.makeMessage("List Does Not Exist", responseCode.LIST_NOT_EXIST, listName)
        else:
            result = self.database.clearListTTL(dbName, listName)
            msg = self.makeMessage(responseCode.detail[result],result,listName)
        return msg
    
    @validTypeCheck
    def showTTL(self, dbName, keyName):
        if(self.database.isDbExist(dbName)):
            code, result = self.database.showTTL(dbName, keyName, "LIST")
            msg = self.makeMessage(responseCode.detail[code], code, result)
        else:
            msg = self.makeMessage("Database Does Not Exist", responseCode.DB_NOT_EXIST, dbName)
        return msg
