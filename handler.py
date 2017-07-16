__author__ = 'Ma Haoxiang'

# import
import re
import os
import json
import time
from db import NoSqlDb

class dbHandler:

    # return types
    ELEM_TYPE_ERROR = 0
    CREATE_ELEM_SUCCESS = 1
    ELEM_ALREADY_EXIST = 2
    ELEM_NOT_EXIST = 3
    UPDATE_ELEM_SUCCESS = 4
    GET_ELEM_SUCCESS = 5
    ELEM_INCR_SUCCESS = 6
    ELEM_DECR_SUCCESS = 7
    ELEM_SEARCH_SUCCESS = 8
    DB_SAVE_ERROR = 9
    DB_SAVE_SUCCESS = 10
    ELEM_IS_LOCKED = 11
    DB_SAVE_LOCKED = 12
    ELEM_DELETE_SUCCESS = 13
    DB_EXISTED = 14
    DB_CREATE_SUCCESS = 15
    DB_GET_SUCCESS = 16
    CREATE_LIST_SUCCESS = 17
    LIST_ALREADY_EXIST = 18
    LIST_IS_LOCKED = 19
    LIST_INSERT_SUCCESS = 20
    LIST_NOT_CONTAIN_VALUE = 21
    LIST_REMOVE_SUCCESS = 22
    LIST_NOT_EXIST = 23
    LIST_SEARCH_SUCCESS = 24
    GET_LIST_SUCCESS = 25
    LIST_DELETE_SUCCESS = 26
    DB_DELETE_SUCCESS = 27
    DB_NOT_EXIST = 28


    def __init__(self, database):
        
        self.msg = {
            "msg":None,
            "typeCode":None,
            "data":None
        }

        self.database = database


    # check if the type of elem is valid (string or int)
    def isValidType(self, elem):
        if('str' in str(type(elem)) or 'int' in str(type(elem))):
            return True
        else:
            return False

    # check if the type of an elem is INT
    def isInt(self, elem):
        if("int" in str(type(elem))):
            return True
        else:
            return False

    # make the response message
    def makeMessage(self, msg, typeCode, data):
        message = {
            "msg":msg,
            "typeCode":typeCode,
            "data":data
        }
        return message

    # create an element in the db
    def createElem(self, elemName, value, dbName):
        if(self.isValidType(elemName)
           and self.isValidType(value)
           and self.isValidType(dbName)): # check the type of elem name and elem value
            if(self.database.isElemExist(dbName, elemName) is False):
                self.database.createElem(elemName, value, dbName)
                msg = self.makeMessage("Make Element Success", dbHandler.CREATE_ELEM_SUCCESS, elemName)

            else:   # this elem already exists in the db
                msg = self.makeMessage("Element Already Exists", dbHandler.ELEM_ALREADY_EXIST, elemName)

        else:   # the type of elem name or elem value is invalid
            msg = self.makeMessage("Element Type Error", dbHandler.ELEM_TYPE_ERROR, elemName)
        return msg

    # update the value of an elem in the db
    def updateElem(self, elemName, value, dbName):
        if(self.database.isElemExist(dbName, elemName) is False):
            msg = self.makeMessage("Element Does Not Exist", dbHandler.ELEM_NOT_EXIST, elemName)

        else:   # find the elem in the db
            if(self.isValidType(elemName)
               and self.isValidType(value)
               and self.isValidType(dbName)):
                self.database.updateElem(elemName, value, dbName)
                msg = self.makeMessage("Element Update Success", dbHandler.UPDATE_ELEM_SUCCESS, elemName)
            else:
                msg = self.makeMessage("Element Type Error", dbHandler.ELEM_TYPE_ERROR, elemName)
        return msg

    # get the value of existed elem
    def getElem(self, elemName, dbName):
        if(self.isValidType(elemName)
           and self.isValidType(dbName)):
            if (self.database.isElemExist(dbName, elemName) is False):
                msg = self.makeMessage("Element Does Not Exist", dbHandler.ELEM_NOT_EXIST, elemName)
            else:
                msg = self.makeMessage("Element Get Success", dbHandler.GET_ELEM_SUCCESS, self.database.getElem(elemName, dbName))

        else:
            msg = self.makeMessage("Element Type Error", dbHandler.ELEM_TYPE_ERROR, elemName)
        return msg

    # search element using regular expression
    def searchElem(self, expression, dbName):
        searchResult = self.database.searchElem(expression, dbName)
        msg = self.makeMessage("Element Search Success", dbHandler.ELEM_SEARCH_SUCCESS, searchResult)
        return msg

    # get all element names in the db
    def searchAllElem(self, dbName):
        msg = self.makeMessage("All Elements Search Success", dbHandler.ELEM_SEARCH_SUCCESS, self.database.searchAllElem(dbName))
        return msg

    # increase the value of an element
    def increaseElem(self, elemName, dbName):
        if(self.isValidType(elemName) and self.isValidType(dbName)):
            if(self.database.isElemExist(dbName, elemName) is False):
                msg = self.makeMessage("Element Does Not Exist", dbHandler.ELEM_NOT_EXIST, elemName)
            else:
                if(self.isInt(self.database.getElem(elemName, dbName))): # check if the element can be increased
                    result = self.database.increaseElem(elemName, dbName)
                    if(result == NoSqlDb.ELEM_INCREASE_SUCCESS):
                        data = self.database.getElem(elemName, dbName)
                        msg = self.makeMessage("Element Increase Success", dbHandler.ELEM_INCR_SUCCESS, data)
                    elif(result == NoSqlDb.ELEM_LOCKED):
                        data = self.database.getElem(elemName, dbName)
                        msg = self.makeMessage("Element Is Locked", dbHandler.ELEM_IS_LOCKED, data)
                else:
                    msg = self.makeMessage("Element Type Error", dbHandler.ELEM_TYPE_ERROR, elemName)
        else:
            msg = self.makeMessage("Element Type Error", dbHandler.ELEM_TYPE_ERROR, elemName)
        return msg

    # decrease the value of an element
    def decreaseElem(self, elemName, dbName):
        if(self.isValidType(elemName) and self.isValidType(dbName)):
            if(self.database.isElemExist(dbName, elemName) is False):
                msg = self.makeMessage("Element Does Not Exist", dbHandler.ELEM_NOT_EXIST, elemName)
            else:
                if(self.isInt(self.database.getElem(elemName, dbName))): # check if the element can be increased
                    result = self.database.decreaseElem(elemName, dbName)
                    if(result == NoSqlDb.ELEM_DECREASE_SUCCESS):
                        data = self.database.getElem(elemName, dbName)
                        msg = self.makeMessage("Element Decrease Success", dbHandler.ELEM_DECR_SUCCESS, data)
                    elif(result == NoSqlDb.ELEM_LOCKED):
                        data = self.database.getElem(elemName, dbName)
                        msg = self.makeMessage("Element Is Locked", dbHandler.ELEM_IS_LOCKED, data)
                else:
                    msg = self.makeMessage("Element Type Error", dbHandler.ELEM_TYPE_ERROR, elemName)
        else:
            msg = self.makeMessage("Element Type Error", dbHandler.ELEM_TYPE_ERROR, elemName)
        return msg

    # delete an element in the database
    def deleteElem(self, elemName, dbName):
        if(self.isValidType(elemName) and self.isValidType(dbName)):
            if (self.database.isElemExist(dbName, elemName) is False):
                msg = self.makeMessage("Element Does Not Exist", dbHandler.ELEM_NOT_EXIST, elemName)
            else:
                result = self.database.deleteElem(elemName, dbName)
                if(result == NoSqlDb.ELEM_LOCKED):
                    msg = self.makeMessage("Element Is Locked", dbHandler.ELEM_IS_LOCKED, elemName)
                elif(result == NoSqlDb.ELEM_DELETE_SUCCESS):
                    msg = self.makeMessage("Element Delete Success", dbHandler.ELEM_DELETE_SUCCESS, elemName)
        else:
            msg = self.makeMessage("Element Type Error", dbHandler.ELEM_TYPE_ERROR, elemName)
        return msg

    # create a list in the database
    def createList(self, listName, dbName):
        if(self.isValidType(listName) and self.isValidType(dbName)):
            if(self.database.isListExist(dbName, listName) is False):
                self.database.createList(listName, dbName)
                msg = self.makeMessage("Make List Success", dbHandler.CREATE_LIST_SUCCESS, listName)
            else:
                msg = self.makeMessage("List Already Exists", dbHandler.LIST_ALREADY_EXIST, listName)
        else:  # the type of elem name or elem value is invalid
            msg = self.makeMessage("Element Type Error", dbHandler.ELEM_TYPE_ERROR, listName)
        return msg

    # get the value of a given list
    def getList(self, listName, dbName):
        if(self.isValidType(listName) and self.isValidType(dbName)):
            if(self.database.isListExist(dbName, listName) is True):
                listValue = self.database.getList(listName, dbName)
                msg = self.makeMessage("Get List Success", dbHandler.GET_LIST_SUCCESS, listValue)
            else:
                msg = self.makeMessage("List Does Not Exist", dbHandler.LIST_NOT_EXIST, listName)
        else:
            msg = self.makeMessage("Element Type Error", dbHandler.ELEM_TYPE_ERROR, listName)
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
                    msg = self.makeMessage("List Is Locked", dbHandler.LIST_IS_LOCKED, listName)
                elif(result == NoSqlDb.LIST_INSERT_SUCCESS):
                    msg = self.makeMessage("List Insert Success", dbHandler.LIST_INSERT_SUCCESS, listName)
            else:
                msg = self.makeMessage("List Does Not Exist", dbHandler.LIST_NOT_EXIST, listName)
        else:
            msg = self.makeMessage("Element Type Error", dbHandler.ELEM_TYPE_ERROR, listName)
        return msg

    # delete a list in the database
    def deleteList(self, listName, dbName):
        if(self.isValidType(listName) and self.isValidType(dbName)):
            if(self.database.isListExist(dbName, listName) is True):
                result = self.database.deleteList(listName, dbName)
                if(result == NoSqlDb.LIST_DELETE_SUCCESS):
                    msg = self.makeMessage("List Delete Success", dbHandler.LIST_DELETE_SUCCESS, listName)
                elif(result == NoSqlDb.LIST_LOCKED):
                    msg = self.makeMessage("List Is Locked", dbHandler.LIST_IS_LOCKED, listName)
            else:
                msg = self.makeMessage("List Does Not Exist", dbHandler.LIST_NOT_EXIST, listName)
        else:
            msg = self.makeMessage("Element Type Error", dbHandler.ELEM_TYPE_ERROR, listName)
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
                    msg = self.makeMessage("List Does Not Contain This Value", dbHandler.LIST_NOT_CONTAIN_VALUE, listName)
                elif(result == NoSqlDb.LIST_REMOVE_SUCCESS):
                    msg = self.makeMessage("List Remove Value Success", dbHandler.LIST_REMOVE_SUCCESS, listName)
                elif(result == NoSqlDb.LIST_LOCKED):
                    msg = self.makeMessage("List Is Locked", dbHandler.LIST_IS_LOCKED, listName)

            else:   # if list does not exist
                msg = self.makeMessage("List Does Not Exist", dbHandler.LIST_NOT_EXIST, listName)
        else:
            msg = self.makeMessage("Element Type Error", dbHandler.ELEM_TYPE_ERROR, listName)
        return msg

    # search list names using regular expression
    def searchList(self, dbName, expression):
        if(self.isValidType(dbName)):
            searchResult = self.database.searchList(dbName, expression)
            msg = self.makeMessage("Search List Success", dbHandler.LIST_SEARCH_SUCCESS, searchResult)
        else:
            msg = self.makeMessage("Element Type Error", dbHandler.ELEM_TYPE_ERROR, dbName)
        return msg

    # get all list names in the given database
    def searchAllList(self, dbName):
        if(self.isValidType(dbName)):
            searchResult = self.database.searchAllList(dbName)
            msg = self.makeMessage("Search All List Success", dbHandler.LIST_SEARCH_SUCCESS, searchResult)
        else:
            msg = self.makeMessage("Element Type Error", dbHandler.ELEM_TYPE_ERROR, dbName)
        return msg

    # add a customized database
    def addDatabase(self, dbName):
        if(self.isValidType(dbName)):
            result = self.database.addDb(dbName)
            if(result == NoSqlDb.DB_EXISTED):
                msg = self.makeMessage("Database Already Exists", dbHandler.DB_EXISTED, dbName)
            elif(result == NoSqlDb.DB_CREATE_SUCCESS):
                msg = self.makeMessage("Database Create Success", dbHandler.DB_CREATE_SUCCESS, dbName)
            elif(result == NoSqlDb.DB_SAVE_LOCK):
                msg = self.makeMessage("Database Is Locked", dbHandler.DB_SAVE_LOCKED, dbName)
        else:
            msg = self.makeMessage("Database Name Type Error", dbHandler.ELEM_TYPE_ERROR, dbName)
        return msg

    # get all database names
    def getAllDatabase(self):
        dbNameSet = self.database.getAllDatabase()
        msg = self.makeMessage("Database Get Success", dbHandler.DB_GET_SUCCESS, dbNameSet)
        return msg

    # delete the given database
    def delDatabase(self, dbName):
        if(self.isValidType(dbName)):
            result = self.database.delDatabase(dbName)
            if(result == NoSqlDb.DB_DELETE_SUCCESS):
                msg = self.makeMessage("Database Delete Success", dbHandler.DB_DELETE_SUCCESS, dbName)
            elif(result == NoSqlDb.DB_SAVE_LOCK):
                msg = self.makeMessage("Database Save Locked", dbHandler.DB_SAVE_LOCKED, dbName)
            elif(result == NoSqlDb.DB_NOT_EXISTED):
                msg = self.makeMessage("Database Does Not Exist", dbHandler.DB_NOT_EXIST, dbName)
        else:
            msg = self.makeMessage("Database Name Type Error", dbHandler.ELEM_TYPE_ERROR, dbName)
        return msg

    # save the data into file
    def saveDb(self):
        result = self.database.saveDb()
        if(result == NoSqlDb.DB_SAVE_SUCCESS):
            msg = self.makeMessage("Database Save Success", dbHandler.DB_SAVE_SUCCESS, time.time())
        elif(result == NoSqlDb.DB_SAVE_LOCK):
            msg = self.makeMessage("Database Save Locked", dbHandler.DB_SAVE_LOCKED, time.time())
        return msg


if __name__ == "__main__":
    pass
