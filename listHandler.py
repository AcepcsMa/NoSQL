__author__ = 'Ma Haoxiang'

# import
from response import responseCode

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
    def createList(self, dbName, listName):
        if(self.isValidType(listName) and self.isValidType(dbName)):
            if(self.database.isListExist(dbName, listName) is False):
                result = self.database.createList(dbName, listName)
                if(result == responseCode.KEY_NAME_INVALID):
                    msg = self.makeMessage("List Name Is Invalid", responseCode.KEY_NAME_INVALID, listName)
                elif(result == responseCode.LIST_CREATE_SUCCESS):
                    msg = self.makeMessage("Make List Success", responseCode.LIST_CREATE_SUCCESS, listName)
                else:
                    msg = self.makeMessage("Database Error", responseCode.DB_ERROR, dbName)
            else:
                msg = self.makeMessage("List Already Exists", responseCode.LIST_ALREADY_EXIST, listName)
        else:  # the type of elem name or elem value is invalid
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, listName)
        return msg

    # get the value of a given list
    def getList(self, dbName, listName):
        if(self.isValidType(listName) and self.isValidType(dbName)):
            if(self.database.isListExist(dbName, listName) is True):
                if(self.database.isExpired(dbName, listName, "LIST") is False):
                    listValue = self.database.getList(listName, dbName)
                    msg = self.makeMessage("Get List Success", responseCode.LIST_GET_SUCCESS, listValue)
                else:
                    msg = self.makeMessage("List Is Expired", responseCode.LIST_EXPIRED, listName)
            else:
                msg = self.makeMessage("List Does Not Exist", responseCode.LIST_NOT_EXIST, listName)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, listName)
        return msg

    # insert a value into the given list
    def insertList(self, dbName, listName, listValue):
        if(self.isValidType(listName)
           and self.isValidType(dbName)):
            # if list exists, execute the insertion
            if(self.database.isListExist(dbName, listName) is True):
                if(self.database.isExpired(dbName, listName, "LIST") is False):
                    result = self.database.insertList(listName, listValue, dbName)
                    if(result == responseCode.LIST_IS_LOCKED):
                        msg = self.makeMessage("List Is Locked", responseCode.LIST_IS_LOCKED, listName)
                    elif(result == responseCode.LIST_INSERT_SUCCESS):
                        msg = self.makeMessage("List Insert Success", responseCode.LIST_INSERT_SUCCESS, listName)
                    else:
                        msg = self.makeMessage("Database Error", responseCode.DB_ERROR, dbName)
                else:
                    msg = self.makeMessage("List Is Expired", responseCode.LIST_EXPIRED, listName)
            else:
                msg = self.makeMessage("List Does Not Exist", responseCode.LIST_NOT_EXIST, listName)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, listName)
        return msg

    # delete a list in the database
    def deleteList(self, dbName, listName):
        if(self.isValidType(listName) and self.isValidType(dbName)):
            if(self.database.isListExist(dbName, listName) is True):
                result = self.database.deleteList(listName, dbName)
                if(result == responseCode.LIST_DELETE_SUCCESS):
                    msg = self.makeMessage("List Delete Success", responseCode.LIST_DELETE_SUCCESS, listName)
                elif(result == responseCode.LIST_IS_LOCKED):
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
                if(self.database.isExpired(dbName, listName, "LIST") is False):
                    result = self.database.rmFromList(dbName, listName, value)
                    if(result == responseCode.LIST_NOT_CONTAIN_VALUE):
                        msg = self.makeMessage("List Does Not Contain This Value", responseCode.LIST_NOT_CONTAIN_VALUE, listName)
                    elif(result == responseCode.LIST_REMOVE_SUCCESS):
                        msg = self.makeMessage("List Remove Value Success", responseCode.LIST_REMOVE_SUCCESS, listName)
                    elif(result == responseCode.LIST_IS_LOCKED):
                        msg = self.makeMessage("List Is Locked", responseCode.LIST_IS_LOCKED, listName)
                    else:
                        msg = self.makeMessage("Database Error", responseCode.DB_ERROR, dbName)
                else:
                    msg = self.makeMessage("List Is Expired", responseCode.LIST_EXPIRED, listName)
            else:   # if list does not exist
                msg = self.makeMessage("List Does Not Exist", responseCode.LIST_NOT_EXIST, listName)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, listName)
        return msg

    # clear the given list
    def clearList(self, dbName, listName):
        if(self.isValidType(dbName) and self.isValidType(listName)):
            if(self.database.isListExist(dbName, listName) is True):
                if(self.database.isExpired(dbName, listName, "LIST") is False):
                    result = self.database.clearList(dbName, listName)
                    if(result == responseCode.LIST_IS_LOCKED):
                        msg = self.makeMessage("List Is Locked", responseCode.LIST_IS_LOCKED, listName)
                    elif(result == responseCode.LIST_CLEAR_SUCCESS):
                        msg = self.makeMessage("List Clear Success", responseCode.LIST_CLEAR_SUCCESS, listName)
                    else:
                        msg = self.makeMessage("Database Error", responseCode.DB_ERROR, dbName)
                else:
                    msg = self.makeMessage("List Is Expired", responseCode.LIST_EXPIRED, listName)
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
            if(self.database.isListExpired(dbName, listName1) is False
               and self.database.isListExpired(dbName, listName2) is False):
                result = self.database.mergeLists(dbName, listName1, listName2, resultListName)
                if(result == responseCode.LIST_IS_LOCKED):
                    msg = self.makeMessage("List Is Locked", responseCode.LIST_IS_LOCKED, resultListName)
                elif(result == responseCode.LIST_MERGE_SUCCESS):
                    msg = self.makeMessage("List Merge Success", responseCode.LIST_MERGE_SUCCESS, resultListName)
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
    def setTTL(self, dbName, listName, ttl):
        if (self.isValidType(dbName) and self.isValidType(listName)):
            if (self.database.isListExist(dbName, listName) is False):
                msg = self.makeMessage("List Does Not Exist", responseCode.LIST_NOT_EXIST, listName)
            else:
                result = self.database.setListTTL(dbName, listName, ttl)
                if (result == responseCode.LIST_IS_LOCKED):
                    msg = self.makeMessage("List Is Locked", responseCode.LIST_IS_LOCKED, listName)
                elif (result == responseCode.LIST_TTL_SET_SUCCESS):
                    msg = self.makeMessage("List TTL Set Success", responseCode.LIST_TTL_SET_SUCCESS, listName)
                else:
                    msg = self.makeMessage("Database Error", responseCode.DB_ERROR, dbName)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, elemName)
        return msg

    # clear TTL for a list
    def clearTTL(self, dbName, listName):
        if (self.isValidType(dbName) and self.isValidType(listName)):
            if (self.database.isListExist(dbName, listName) is False):
                msg = self.makeMessage("List Does Not Exist", responseCode.LIST_NOT_EXIST, listName)
            else:
                result = self.database.clearListTTL(dbName, listName)
                if (result == responseCode.LIST_IS_LOCKED):
                    msg = self.makeMessage("List Is Locked", responseCode.LIST_IS_LOCKED, listName)
                elif (result == responseCode.LIST_TTL_CLEAR_SUCCESS):
                    msg = self.makeMessage("List TTL Clear Success", responseCode.ELEM_TTL_CLEAR_SUCCESS,
                                           listName)
                else:
                    msg = self.makeMessage("Database Error", responseCode.DB_ERROR, dbName)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, elemName)
        return msg

    def showTTL(self, dbName, keyName):
        if(self.isValidType(dbName) and self.isValidType(keyName)):
            if(self.database.isDbExist(dbName)):
                result = self.database.showTTL(dbName, keyName, "LIST")
                if(result == responseCode.TTL_NO_RECORD):
                    msg = self.makeMessage("TTL No Record", responseCode.TTL_NO_RECORD, keyName)
                elif(result == responseCode.TTL_EXPIRED):
                    msg = self.makeMessage("List TTL Expired", responseCode.LIST_EXPIRED, keyName)
                else:
                    msg = self.makeMessage("TTL Show Success", responseCode.TTL_SHOW_SUCCESS, result)
            else:
                msg = self.makeMessage("Database Does Not Exist", responseCode.DB_NOT_EXIST, dbName)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, dbName)
        return msg
