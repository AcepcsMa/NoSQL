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
                self.database.createElem(dbName, elemName)
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
