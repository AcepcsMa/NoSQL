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

    # create an element in the db
    def createElem(self, elemName, value, dbName):
        if(self.isValidType(elemName)
           and self.isValidType(value)
           and self.isValidType(dbName)): # check the type of elem name and elem value
            if(self.database.isElemExist(dbName, elemName) is False):
                self.database.createElem(elemName, value, dbName)
                self.msg["msg"] = "Make Element Success"
                self.msg["typeCode"] = dbHandler.CREATE_ELEM_SUCCESS
                self.msg["data"] = elemName
                return self.msg

            else:   # this elem already exists in the db
                self.msg["msg"] = "Element Already Exists"
                self.msg["typeCode"] = dbHandler.ELEM_ALREADY_EXIST
                self.msg["data"] = elemName
                return self.msg

        else:   # the type of elem name or elem value is invalid
            self.msg["msg"] = "Element Type Error"
            self.msg["typeCode"] = dbHandler.ELEM_TYPE_ERROR
            self.msg["data"] = elemName
            return self.msg

    # update the value of an elem in the db
    def updateElem(self, elemName, value, dbName):
        if(self.database.isElemExist(dbName, elemName) is False):
            self.msg["msg"] = "Element Does Not Exist"
            self.msg["typeCode"] = dbHandler.ELEM_NOT_EXIST
            self.msg["data"] = elemName
            return self.msg

        else:   # find the elem in the db
            if(self.isValidType(elemName)
               and self.isValidType(value)
               and self.isValidType(dbName)):
                self.database.updateElem(elemName, value, dbName)
                self.msg["msg"] = "Element Update Success"
                self.msg["typeCode"] = dbHandler.UPDATE_ELEM_SUCCESS
                self.msg["data"] = elemName
                return self.msg
            else:
                self.msg["msg"] = "Element Type Error"
                self.msg["typeCode"] = dbHandler.ELEM_TYPE_ERROR
                self.msg["data"] = elemName
                return self.msg

    # get the value of existed elem
    def getElem(self, elemName, dbName):
        if(self.isValidType(elemName)
           and self.isValidType(dbName)):
            if (self.database.isElemExist(dbName, elemName) is False):
                self.msg["msg"] = "Element Does Not Exist"
                self.msg["typeCode"] = dbHandler.ELEM_NOT_EXIST
                self.msg["data"] = elemName
                return self.msg
            else:
                self.msg["msg"] = "Element Get Success"
                self.msg["typeCode"] = dbHandler.GET_ELEM_SUCCESS
                self.msg["data"] = self.database.getElem(elemName, dbName)
                #print (self.msg["data"])
                return self.msg

        else:
            self.msg["msg"] = "Element Type Error"
            self.msg["typeCode"] = dbHandler.ELEM_TYPE_ERROR
            self.msg["data"] = elemName
            return self.msg

    # search element using regular expression
    def searchElem(self, expression, dbName):
        searchResult = self.database.searchElem(expression, dbName)
        self.msg["msg"] = "Element Search Success"
        self.msg["typeCode"] = dbHandler.ELEM_SEARCH_SUCCESS
        self.msg["data"] = searchResult
        return self.msg

    # get all element names in the db
    def searchAllElem(self, dbName):
        self.msg["msg"] = "All Elements Search Success"
        self.msg["typeCode"] = dbHandler.ELEM_SEARCH_SUCCESS
        self.msg["data"] = self.database.searchAllElem(dbName)
        return self.msg

    # increase the value of an element
    def increaseElem(self, elemName, dbName):
        if(self.isValidType(elemName) and self.isValidType(dbName)):
            if(self.database.isElemExist(dbName, elemName) is False):
                self.msg["msg"] = "Element Does Not Exist"
                self.msg["typeCode"] = dbHandler.ELEM_NOT_EXIST
                self.msg["data"] = elemName
                return self.msg
            else:
                if(self.isInt(self.database.getElem(elemName, dbName))): # check if the element can be increased
                    result = self.database.increaseElem(elemName, dbName)
                    if(result == NoSqlDb.ELEM_INCREASE_SUCCESS):
                        self.msg["msg"] = "Element Increase Success"
                        self.msg["typeCode"] = dbHandler.ELEM_INCR_SUCCESS
                        self.msg["data"] = self.database.getElem(elemName, dbName)
                        return self.msg
                    elif(result == NoSqlDb.ELEM_LOCKED):
                        self.msg["msg"] = "Element Is Locked"
                        self.msg["typeCode"] = dbHandler.ELEM_IS_LOCKED
                        self.msg["data"] = self.database.getElem(elemName, dbName)
                else:
                    self.msg["msg"] = "Element Type Error"
                    self.msg["typeCode"] = dbHandler.ELEM_TYPE_ERROR
                    self.msg["data"] = elemName
                    return self.msg

    # decrease the value of an element
    def decreaseElem(self, elemName, dbName):
        if(self.isValidType(elemName) and self.isValidType(dbName)):
            if(self.database.isElemExist(dbName, elemName) is False):
                self.msg["msg"] = "Element Does Not Exist"
                self.msg["typeCode"] = dbHandler.ELEM_NOT_EXIST
                self.msg["data"] = elemName
                return self.msg
            else:
                if(self.isInt(self.database.getElem(elemName, dbName))): # check if the element can be increased
                    result = self.database.decreaseElem(elemName, dbName)
                    if(result == NoSqlDb.ELEM_DECREASE_SUCCESS):
                        self.msg["msg"] = "Element Decrease Success"
                        self.msg["typeCode"] = dbHandler.ELEM_DECR_SUCCESS
                        self.msg["data"] = self.database.getElem(elemName, dbName)
                        return self.msg
                    elif(result == NoSqlDb.ELEM_LOCKED):
                        self.msg["msg"] = "Element Is Locked"
                        self.msg["typeCode"] = dbHandler.ELEM_IS_LOCKED
                        self.msg["data"] = self.database.getElem(elemName, dbName)
                        return self.msg
                else:
                    self.msg["msg"] = "Element Type Error"
                    self.msg["typeCode"] = dbHandler.ELEM_TYPE_ERROR
                    self.msg["data"] = elemName
                    return self.msg

    # delete an element in the database
    def deleteElem(self, elemName, dbName):
        if(self.isValidType(elemName) and self.isValidType(dbName)):
            if (self.database.isElemExist(dbName, elemName) is False):
                self.msg["msg"] = "Element Does Not Exist"
                self.msg["typeCode"] = dbHandler.ELEM_NOT_EXIST
                self.msg["data"] = elemName
            else:
                result = self.database.deleteElem(elemName, dbName)
                if(result == NoSqlDb.ELEM_LOCKED):
                    self.msg["msg"] = "Element Is Locked"
                    self.msg["typeCode"] = dbHandler.ELEM_IS_LOCKED
                    self.msg["data"] = elemName
                elif(result == NoSqlDb.ELEM_DELETE_SUCCESS):
                    self.msg["msg"] = "Element Delete Success"
                    self.msg["typeCode"] = dbHandler.ELEM_DELETE_SUCCESS
                    self.msg["data"] = elemName
        else:
            self.msg["msg"] = "Element Type Error"
            self.msg["typeCode"] = dbHandler.ELEM_TYPE_ERROR
            self.msg["data"] = elemName
        return self.msg

    # create a list in the database
    def createList(self, listName, dbName):
        if(self.isValidType(listName) and self.isValidType(dbName)):
            if(self.database.isListExist(dbName, listName) is False):
                self.database.createList(listName, dbName)
                self.msg["msg"] = "Make List Success"
                self.msg["typeCode"] = dbHandler.CREATE_LIST_SUCCESS
                self.msg["data"] = listName
                return self.msg
            else:
                self.msg["msg"] = "List Already Exists"
                self.msg["typeCode"] = dbHandler.LIST_ALREADY_EXIST
                self.msg["data"] = listName
                return self.msg
        else:  # the type of elem name or elem value is invalid
            self.msg["msg"] = "Element Type Error"
            self.msg["typeCode"] = dbHandler.ELEM_TYPE_ERROR
            self.msg["data"] = listName
            return self.msg

    def getList(self, listName, dbName):
        if(self.isValidType(listName) and self.isValidType(dbName)):
            if(self.database.isListExist(dbName, listName) is True):
                listValue = self.database.getList(listName, dbName)
                self.msg["msg"] = "Get List Success"
                self.msg["typeCode"] = dbHandler.GET_LIST_SUCCESS
                self.msg["data"] = listValue
            else:
                self.msg["msg"] = "List Does Not Exist"
                self.msg["typeCode"] = dbHandler.LIST_NOT_EXIST
                self.msg["data"] = listName
        else:
            self.msg["msg"] = "Element Type Error"
            self.msg["typeCode"] = dbHandler.ELEM_TYPE_ERROR
            self.msg["data"] = listName
        return self.msg

    # insert a value into the given list
    def insertList(self, listName, value, dbName):
        if(self.isValidType(listName)
           and self.isValidType(value)
           and self.isValidType(dbName)):
            # if list exists, execute the insertion
            if(self.database.isListExist(dbName, listName) is True):
                result = self.database.insertList(listName, value, dbName)
                if(result == NoSqlDb.LIST_LOCKED):
                    self.msg["msg"] = "List Is Locked"
                    self.msg["typeCode"] = dbHandler.LIST_IS_LOCKED
                    self.msg["data"] = listName
                elif(result == NoSqlDb.LIST_INSERT_SUCCESS):
                    self.msg["msg"] = "List Insert Success"
                    self.msg["typeCode"] = dbHandler.LIST_INSERT_SUCCESS
                    self.msg["data"] = listName
            else:
                self.msg["msg"] = "List Does Not Exist"
                self.msg["typeCode"] = dbHandler.LIST_NOT_EXIST
                self.msg["data"] = listName
        else:
            self.msg["msg"] = "Element Type Error"
            self.msg["typeCode"] = dbHandler.ELEM_TYPE_ERROR
            self.msg["data"] = listName
        return self.msg

    # delete a list in the database
    def deleteList(self, listName, dbName):
        if(self.isValidType(listName) and self.isValidType(dbName)):
            if(self.database.isListExist(dbName, listName) is True):
                result = self.database.deleteList(listName, dbName)
                if(result == NoSqlDb.LIST_DELETE_SUCCESS):
                    self.msg["msg"] = "List Delete Success"
                    self.msg["typeCode"] = dbHandler.LIST_DELETE_SUCCESS
                    self.msg["data"] = listName
                elif(result == NoSqlDb.LIST_LOCKED):
                    self.msg["msg"] = "List Is Locked"
                    self.msg["typeCode"] = dbHandler.LIST_IS_LOCKED
                    self.msg["data"] = listName
            else:
                self.msg["msg"] = "List Does Not Exist"
                self.msg["typeCode"] = dbHandler.LIST_NOT_EXIST
                self.msg["data"] = listName
        else:
            self.msg["msg"] = "Element Type Error"
            self.msg["typeCode"] = dbHandler.ELEM_TYPE_ERROR
            self.msg["data"] = listName
        return self.msg

    # remove a value from the given list
    def rmFromList(self, dbName, listName, value):
        if(self.isValidType(dbName)
           and self.isValidType(listName)
           and self.isValidType(value)):
            # if list exists, execute the removal
            if(self.database.isListExist(dbName, listName) is True):
                result = self.database.rmFromList(dbName, listName, value)
                if(result == NoSqlDb.LIST_NOT_CONTAIN_VALUE):
                    self.msg["msg"] = "List Does Not Contain This Value"
                    self.msg["typeCode"] = dbHandler.LIST_NOT_CONTAIN_VALUE
                    self.msg["data"] = listName
                elif(result == NoSqlDb.LIST_REMOVE_SUCCESS):
                    self.msg["msg"] = "List Remove Value Success"
                    self.msg["typeCode"] = dbHandler.LIST_REMOVE_SUCCESS
                    self.msg["data"] = listName
                elif(result == NoSqlDb.LIST_LOCKED):
                    self.msg["msg"] = "List Is Locked"
                    self.msg["typeCode"] = dbHandler.LIST_IS_LOCKED
                    self.msg["data"] = listName

            else:   # if list does not exist
                self.msg["msg"] = "List Does Not Exist"
                self.msg["typeCode"] = dbHandler.LIST_NOT_EXIST
                self.msg["data"] = listName
        else:
            self.msg["msg"] = "Element Type Error"
            self.msg["typeCode"] = dbHandler.ELEM_TYPE_ERROR
            self.msg["data"] = listName
        return self.msg

    # search list names using regular expression
    def searchList(self, dbName, expression):
        if(self.isValidType(dbName)):
            searchResult = self.database.searchList(dbName, expression)
            self.msg["msg"] = "Search List Success"
            self.msg["typeCode"] = dbHandler.LIST_SEARCH_SUCCESS
            self.msg["data"] = searchResult
        else:
            self.msg["msg"] = "Element Type Error"
            self.msg["typeCode"] = dbHandler.ELEM_TYPE_ERROR
            self.msg["data"] = dbName
        return self.msg

    # get all list names in the given database
    def searchAllList(self, dbName):
        if(self.isValidType(dbName)):
            searchResult = self.database.searchAllList(dbName)
            self.msg["msg"] = "Search All List Success"
            self.msg["typeCode"] = dbHandler.LIST_SEARCH_SUCCESS
            self.msg["data"] = searchResult
        else:
            self.msg["msg"] = "Element Type Error"
            self.msg["typeCode"] = dbHandler.ELEM_TYPE_ERROR
            self.msg["data"] = dbName
        return self.msg

    # add a customized database
    def addDatabase(self, dbName):
        if(self.isValidType(dbName)):
            result = self.database.addDb(dbName)
            if(result == NoSqlDb.DB_EXISTED):
                self.msg["msg"] = "Database Already Exists"
                self.msg["typeCode"] = dbHandler.DB_EXISTED
                self.msg["data"] = dbName
            elif(result == NoSqlDb.DB_CREATE_SUCCESS):
                self.msg["msg"] = "Database Create Success"
                self.msg["typeCode"] = dbHandler.DB_CREATE_SUCCESS
                self.msg["data"] = dbName
            elif(result == NoSqlDb.DB_SAVE_LOCK):
                self.msg["msg"] = "Database Is Locked"
                self.msg["typeCode"] = dbHandler.DB_SAVE_LOCKED
                self.msg["data"] = dbName
        else:
            self.msg["msg"] = "Database Name Type Error"
            self.msg["typeCode"] = dbHandler.ELEM_TYPE_ERROR
            self.msg["data"] = dbName
        return self.msg

    # get all database names
    def getAllDatabase(self):
        dbNameSet = self.database.getAllDatabase()
        self.msg["msg"] = "Database Get Success"
        self.msg["typeCode"] = dbHandler.DB_GET_SUCCESS
        self.msg["data"] = dbNameSet
        return self.msg

    # delete the given database
    def delDatabase(self, dbName):
        if(self.isValidType(dbName)):
            result = self.database.delDatabase(dbName)
            if(result == NoSqlDb.DB_DELETE_SUCCESS):
                self.msg["msg"] = "Database Delete Success"
                self.msg["typeCode"] = dbHandler.DB_DELETE_SUCCESS
                self.msg["data"] = dbName
            elif(result == NoSqlDb.DB_SAVE_LOCK):
                self.msg["msg"] = "Database Save Locked"
                self.msg["typeCode"] = dbHandler.DB_SAVE_LOCKED
                self.msg["data"] = dbName
            elif(result == NoSqlDb.DB_NOT_EXISTED):
                self.msg["msg"] = "Database Does Not Exist"
                self.msg["typeCode"] = dbHandler.DB_NOT_EXIST
                self.msg["data"] = dbName
        else:
            self.msg["msg"] = "Database Name Type Error"
            self.msg["typeCode"] = dbHandler.ELEM_TYPE_ERROR
            self.msg["data"] = dbName
        return self.msg

    # save the data into file
    def saveDb(self):
        result = self.database.saveDb()

        if(result == NoSqlDb.DB_SAVE_SUCCESS):
            self.msg["msg"] = "Database Save Success"
            self.msg["typeCode"] = dbHandler.DB_SAVE_SUCCESS
            self.msg["data"] = time.time()
        elif(result == NoSqlDb.DB_SAVE_LOCK):
            self.msg["msg"] = "Database Save Locked"
            self.msg["typeCode"] = dbHandler.DB_SAVE_LOCKED
            self.msg["data"] = time.time()
        return self.msg



if __name__ == "__main__":
    pass
