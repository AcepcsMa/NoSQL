__author__ = 'Marco'

# import
from response import responseCode
from db import NoSqlDb

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
    def createList(self, listName, dbName):
        if(self.isValidType(listName) and self.isValidType(dbName)):
            if(self.database.isListExist(dbName, listName) is False):
                self.database.createList(listName, dbName)
                msg = self.makeMessage("Make List Success", responseCode.LIST_CREATE_SUCCESS, listName)
            else:
                msg = self.makeMessage("List Already Exists", responseCode.LIST_ALREADY_EXIST, listName)
        else:  # the type of elem name or elem value is invalid
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, listName)
        return msg

    # get the value of a given list
    def getList(self, listName, dbName):
        if(self.isValidType(listName) and self.isValidType(dbName)):
            if(self.database.isListExist(dbName, listName) is True):
                listValue = self.database.getList(listName, dbName)
                msg = self.makeMessage("Get List Success", responseCode.LIST_GET_SUCCESS, listValue)
            else:
                msg = self.makeMessage("List Does Not Exist", responseCode.LIST_NOT_EXIST, listName)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, listName)
        return msg

    # insert a value into the given list
    def insertList(self, listName, value, dbName):
        if(self.isValidType(listName)
           and self.isValidType(value)
           and self.isValidType(dbName)):
            # if list exists, execute the insertion
            if(self.database.isListExist(dbName, listName) is True):
                result = self.database.insertList(listName, value, dbName)
                if(result == NoSqlDb.LIST_LOCKED):
                    msg = self.makeMessage("List Is Locked", responseCode.LIST_IS_LOCKED, listName)
                elif(result == NoSqlDb.LIST_INSERT_SUCCESS):
                    msg = self.makeMessage("List Insert Success", responseCode.LIST_INSERT_SUCCESS, listName)
                else:
                    msg = self.makeMessage("Database Error", responseCode.DB_ERROR, dbName)
            else:
                msg = self.makeMessage("List Does Not Exist", responseCode.LIST_NOT_EXIST, listName)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, listName)
        return msg

    # delete a list in the database
    def deleteList(self, listName, dbName):
        if(self.isValidType(listName) and self.isValidType(dbName)):
            if(self.database.isListExist(dbName, listName) is True):
                result = self.database.deleteList(listName, dbName)
                if(result == NoSqlDb.LIST_DELETE_SUCCESS):
                    msg = self.makeMessage("List Delete Success", responseCode.LIST_DELETE_SUCCESS, listName)
                elif(result == NoSqlDb.LIST_LOCKED):
                    msg = self.makeMessage("List Is Locked", responseCode.LIST_IS_LOCKED, listName)
                else:
                    msg = self.makeMessage("Database Error", responseCode.DB_ERROR, dbName)
            else:
                msg = self.makeMessage("List Does Not Exist", responseCode.LIST_NOT_EXIST, listName)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, listName)
        return msg

    # remove a value from the given list
    def rmFromList(self, dbName, listName, value):
        if(self.isValidType(dbName)
           and self.isValidType(listName)
           and self.isValidType(value)):
            # if list exists, execute the removal
            if(self.database.isListExist(dbName, listName) is True):
                result = self.database.rmFromList(dbName, listName, value)
                if(result == NoSqlDb.LIST_NOT_CONTAIN_VALUE):
                    msg = self.makeMessage("List Does Not Contain This Value", responseCode.LIST_NOT_CONTAIN_VALUE, listName)
                elif(result == NoSqlDb.LIST_REMOVE_SUCCESS):
                    msg = self.makeMessage("List Remove Value Success", responseCode.LIST_REMOVE_SUCCESS, listName)
                elif(result == NoSqlDb.LIST_LOCKED):
                    msg = self.makeMessage("List Is Locked", responseCode.LIST_IS_LOCKED, listName)
                else:
                    msg = self.makeMessage("Database Error", responseCode.DB_ERROR, dbName)

            else:   # if list does not exist
                msg = self.makeMessage("List Does Not Exist", responseCode.LIST_NOT_EXIST, listName)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, listName)
        return msg

    # clear the given list
    def clearList(self, dbName, listName):
        if(self.isValidType(dbName) and self.isValidType(listName)):
            if(self.database.isListExist(dbName, listName) is True):
                result = self.database.clearList(dbName, listName)
                if(result == NoSqlDb.LIST_LOCKED):
                    msg = self.makeMessage("List Is Locked", responseCode.LIST_IS_LOCKED, listName)
                elif(result == NoSqlDb.LIST_CLEAR_SUCCESS):
                    msg = self.makeMessage("List Clear Success", responseCode.LIST_CLEAR_SUCCESS, listName)
                else:
                    msg = self.makeMessage("Database Error", responseCode.DB_ERROR, dbName)
            else:
                msg = self.makeMessage("List Does Not Exist", responseCode.LIST_NOT_EXIST, listName)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, listName)
        return msg

    # merge two lists
    def mergeLists(self, dbName, listName1, listName2, resultListName=None):
        if(resultListName is not None):
            if(self.database.isListExist(dbName, resultListName) is True):
                msg = self.makeMessage("Merge Result Exists", responseCode.MERGE_RESULT_EXIST, resultListName)
                return msg

        if(self.database.isListExist(dbName, listName1)
           and self.database.isListExist(dbName, listName2)):
            result = self.database.mergeLists(dbName, listName1, listName2, resultListName)
            if(result == NoSqlDb.LIST_LOCKED):
                msg = self.makeMessage("List Is Locked", responseCode.LIST_IS_LOCKED, resultListName)
            elif(result == NoSqlDb.LIST_MERGE_SUCCESS):
                msg = self.makeMessage("List Merge Success", responseCode.LIST_MERGE_SUCCESS, resultListName)
            else:
                msg = self.makeMessage("Database Error", responseCode.DB_ERROR, dbName)
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
