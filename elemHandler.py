__author__ = 'Marco'

# import
from response import responseCode
from db import NoSqlDb

class elemHandler:
    def __init__(self, database):
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
                msg = self.makeMessage("Make Element Success", responseCode.CREATE_ELEM_SUCCESS, elemName)

            else:   # this elem already exists in the db
                msg = self.makeMessage("Element Already Exists", responseCode.ELEM_ALREADY_EXIST, elemName)

        else:   # the type of elem name or elem value is invalid
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, elemName)
        return msg

    # update the value of an elem in the db
    def updateElem(self, elemName, value, dbName):
        if(self.database.isElemExist(dbName, elemName) is False):
            msg = self.makeMessage("Element Does Not Exist", responseCode.ELEM_NOT_EXIST, elemName)

        else:   # find the elem in the db
            if(self.isValidType(elemName)
               and self.isValidType(value)
               and self.isValidType(dbName)):
                self.database.updateElem(elemName, value, dbName)
                msg = self.makeMessage("Element Update Success", responseCode.UPDATE_ELEM_SUCCESS, elemName)
            else:
                msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, elemName)
        return msg

    # get the value of existed elem
    def getElem(self, elemName, dbName):
        if(self.isValidType(elemName)
           and self.isValidType(dbName)):
            if (self.database.isElemExist(dbName, elemName) is False):
                msg = self.makeMessage("Element Does Not Exist", responseCode.ELEM_NOT_EXIST, elemName)
            else:
                msg = self.makeMessage("Element Get Success", responseCode.GET_ELEM_SUCCESS, self.database.getElem(elemName, dbName))

        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, elemName)
        return msg

    # search element using regular expression
    def searchElem(self, expression, dbName):
        searchResult = self.database.searchElem(expression, dbName)
        msg = self.makeMessage("Element Search Success", responseCode.ELEM_SEARCH_SUCCESS, searchResult)
        return msg

    # get all element names in the db
    def searchAllElem(self, dbName):
        msg = self.makeMessage("All Elements Search Success", responseCode.ELEM_SEARCH_SUCCESS, self.database.searchAllElem(dbName))
        return msg

    # increase the value of an element
    def increaseElem(self, elemName, dbName):
        if(self.isValidType(elemName) and self.isValidType(dbName)):
            if(self.database.isElemExist(dbName, elemName) is False):
                msg = self.makeMessage("Element Does Not Exist", responseCode.ELEM_NOT_EXIST, elemName)
            else:
                if(self.isInt(self.database.getElem(elemName, dbName))): # check if the element can be increased
                    result = self.database.increaseElem(elemName, dbName)
                    if(result == NoSqlDb.ELEM_INCREASE_SUCCESS):
                        data = self.database.getElem(elemName, dbName)
                        msg = self.makeMessage("Element Increase Success", responseCode.ELEM_INCR_SUCCESS, data)
                    elif(result == NoSqlDb.ELEM_LOCKED):
                        data = self.database.getElem(elemName, dbName)
                        msg = self.makeMessage("Element Is Locked", responseCode.ELEM_IS_LOCKED, data)
                else:
                    msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, elemName)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, elemName)
        return msg

    # decrease the value of an element
    def decreaseElem(self, elemName, dbName):
        if(self.isValidType(elemName) and self.isValidType(dbName)):
            if(self.database.isElemExist(dbName, elemName) is False):
                msg = self.makeMessage("Element Does Not Exist", responseCode.ELEM_NOT_EXIST, elemName)
            else:
                if(self.isInt(self.database.getElem(elemName, dbName))): # check if the element can be increased
                    result = self.database.decreaseElem(elemName, dbName)
                    if(result == NoSqlDb.ELEM_DECREASE_SUCCESS):
                        data = self.database.getElem(elemName, dbName)
                        msg = self.makeMessage("Element Decrease Success", responseCode.ELEM_DECR_SUCCESS, data)
                    elif(result == NoSqlDb.ELEM_LOCKED):
                        data = self.database.getElem(elemName, dbName)
                        msg = self.makeMessage("Element Is Locked", responseCode.ELEM_IS_LOCKED, data)
                else:
                    msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, elemName)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, elemName)
        return msg

    # delete an element in the database
    def deleteElem(self, elemName, dbName):
        if(self.isValidType(elemName) and self.isValidType(dbName)):
            if (self.database.isElemExist(dbName, elemName) is False):
                msg = self.makeMessage("Element Does Not Exist", responseCode.ELEM_NOT_EXIST, elemName)
            else:
                result = self.database.deleteElem(elemName, dbName)
                if(result == NoSqlDb.ELEM_LOCKED):
                    msg = self.makeMessage("Element Is Locked", responseCode.ELEM_IS_LOCKED, elemName)
                elif(result == NoSqlDb.ELEM_DELETE_SUCCESS):
                    msg = self.makeMessage("Element Delete Success", responseCode.ELEM_DELETE_SUCCESS, elemName)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, elemName)
        return msg
