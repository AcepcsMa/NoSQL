__author__ = 'Ma Haoxiang'

# import
from response import responseCode
from decorator import *

class elemHandler:
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

    # create an element in the db
    @validTypeCheck
    def createElem(self, dbName, elemName, elemValue):
        if(self.isValidType(elemValue)): # check the type of elem name and elem value
            if(self.database.isElemExist(dbName, elemName) is False):
                result = self.database.createElem(dbName, elemName, elemValue)
                msg = self.makeMessage(responseCode.detail[result],result,elemName)
            else:   # this elem already exists in the db
                msg = self.makeMessage("Element Already Exists", responseCode.ELEM_ALREADY_EXIST, elemName)
        else:   # the type of elem name or elem value is invalid
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, elemName)
        return msg

    # update the value of an elem in the db
    @validTypeCheck
    def updateElem(self, dbName, elemName, elemValue):
        if(self.isValidType(elemValue)):
            if(self.database.isElemExist(dbName, elemName)):
                if(self.database.isExpired(dbName, elemName, "ELEM") is False):
                    result = self.database.updateElem(elemName, elemValue, dbName)
                    msg = self.makeMessage(responseCode.detail[result], result, elemName)
                else:
                    msg = self.makeMessage("Elem Is Expired", responseCode.ELEM_EXPIRED, elemName)
            else:
                msg = self.makeMessage("Element Does Not Exist", responseCode.ELEM_NOT_EXIST, elemName)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, elemName)
        return msg

    # get the value of existed elem
    @validTypeCheck
    def getElem(self, dbName, elemName):
        if(self.database.isDbExist(dbName)):
            if (self.database.isElemExist(dbName, elemName) is False):
                msg = self.makeMessage("Element Does Not Exist", responseCode.ELEM_NOT_EXIST, elemName)
            else:
                if(self.database.isExpired(dbName, elemName, "ELEM") is False):
                    msg = self.makeMessage("Element Get Success", responseCode.ELEM_GET_SUCCESS, self.database.getElem(elemName, dbName))
                else:
                    msg = self.makeMessage("Elem Is Expired", responseCode.ELEM_EXPIRED, elemName)
        else:
            msg = self.makeMessage("Database Does Not Exist", responseCode.DB_NOT_EXIST, dbName)
        return msg

    # search element using regular expression
    def searchElem(self, dbName, expression):
        if(self.isValidType(dbName)):
            searchResult = self.database.searchByRE(dbName, expression, "ELEM")
            msg = self.makeMessage("Search Element Success", responseCode.ELEM_SEARCH_SUCCESS, searchResult)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, dbName)
        return msg

    # get all element names in the db
    def searchAllElem(self, dbName):
        msg = self.makeMessage("All Elements Search Success", responseCode.ELEM_SEARCH_SUCCESS, self.database.searchAllElem(dbName))
        return msg

    # increase the value of an element
    @validTypeCheck
    def increaseElem(self, dbName, elemName):
        if(self.database.isElemExist(dbName, elemName) is False):
            msg = self.makeMessage("Element Does Not Exist", responseCode.ELEM_NOT_EXIST, elemName)
        else:
            if(self.database.isExpired(dbName, elemName, "ELEM") is False):
                if(self.isInt(self.database.getElem(elemName, dbName))): # check if the element can be increased
                    result = self.database.increaseElem(elemName, dbName)
                    msg = self.makeMessage(responseCode.detail[result], result, elemName)
                else:
                    msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, elemName)
            else:
                msg = self.makeMessage("Elem Is Expired", responseCode.ELEM_EXPIRED, elemName)
        return msg

    # decrease the value of an element
    @validTypeCheck
    def decreaseElem(self, dbName, elemName):
        if(self.database.isElemExist(dbName, elemName) is False):
            msg = self.makeMessage("Element Does Not Exist", responseCode.ELEM_NOT_EXIST, elemName)
        else:
            if(self.database.isExpired(dbName, elemName, "ELEM") is False):
                if(self.isInt(self.database.getElem(elemName, dbName))): # check if the element can be increased
                    result = self.database.decreaseElem(elemName, dbName)
                    msg = self.makeMessage(responseCode.detail[result], result, elemName)
                else:
                    msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, elemName)
            else:
                msg = self.makeMessage("Elem Is Expired", responseCode.ELEM_EXPIRED, elemName)
        return msg

    # delete an element in the database
    @validTypeCheck
    def deleteElem(self, dbName, elemName):
        if (self.database.isElemExist(dbName, elemName) is False):
            msg = self.makeMessage("Element Does Not Exist", responseCode.ELEM_NOT_EXIST, elemName)
        else:
            result = self.database.deleteElem(elemName, dbName)
            msg = self.makeMessage(responseCode.detail[result], result, elemName)
        return msg

    # set TTL for an element
    @validTypeCheck
    def setTTL(self, dbName, elemName, ttl):
        if(self.database.isElemExist(dbName, elemName) is False):
            msg = self.makeMessage("Element Does Not Exist", responseCode.ELEM_NOT_EXIST, elemName)
        else:
            result = self.database.setElemTTL(dbName, elemName, ttl)
            msg = self.makeMessage(responseCode.detail[result], result, elemName)
        return msg

    # clear TTL for an element
    @validTypeCheck
    def clearTTL(self, dbName, elemName):
        if(self.database.isElemExist(dbName, elemName) is False):
            msg = self.makeMessage("Element Does Not Exist", responseCode.ELEM_NOT_EXIST, elemName)
        else:
            result = self.database.clearElemTTL(dbName, elemName)
            msg = self.makeMessage(responseCode.detail[result], result, elemName)
        return msg

    @validTypeCheck
    def showTTL(self, dbName, keyName):
        if(self.database.isDbExist(dbName)):
            code, result = self.database.showTTL(dbName, keyName, "ELEM")
            msg = self.makeMessage(responseCode.detail[code], code, result)
        else:
            msg = self.makeMessage("Database Does Not Exist", responseCode.DB_NOT_EXIST, dbName)
        return msg
