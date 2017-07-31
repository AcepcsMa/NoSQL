__author__ = 'Marco'

# import
from response import responseCode
from db import NoSqlDb

class setHandler:
    def __init__(self, database):
        self.database = database

    # check if the type of elem is valid (string or int)
    def isValidType(self, elem):
        if('str' in str(type(elem)) or 'int' in str(type(elem))):
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

    # create a set
    def createSet(self, dbName, setName):
        if(self.isValidType(dbName) and self.isValidType(setName)):
            if(self.database.isSetExist(dbName, setName) is False):
                result = self.database.createSet(dbName, setName)
                if(result == NoSqlDb.SET_CREATE_SUCCESS):
                    msg = self.makeMessage("Set Create Success", responseCode.SET_CREATE_SUCCESS, setName)
                else:
                    msg = self.makeMessage("Database Error", responseCode.DB_ERROR, dbName)
            else:
                msg = self.makeMessage("Set Already Exists", responseCode.SET_ALREADY_EXIST, setName)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, setName)
        return msg

    # get set value
    def getSet(self, dbName, setName):
        if(self.isValidType(dbName) and self.isValidType(setName)):
            if(self.database.isSetExist(dbName, setName) is True):
                setValue = self.database.getSet(dbName, setName)
                msg = self.makeMessage("Set Get Success", responseCode.SET_GET_SUCCESS, setValue)
            else:
                msg = self.makeMessage("Set Does Not Exist", responseCode.SET_NOT_EXIST, setName)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, setName)
        return msg

    # insert a value into the given set
    def insertSet(self, dbName, setName, setValue):
        if(self.isValidType(dbName) and self.isValidType(setName)):
            if(self.database.isSetExist(dbName, setName)):
                result = self.database.insertSet(dbName, setName, setValue)
                if(result == NoSqlDb.SET_LOCKED):
                    msg = self.makeMessage("Set Is Locked", responseCode.SET_IS_LOCKED, setName)
                elif(result == NoSqlDb.SET_VALUE_ALREADY_EXIST):
                    msg = self.makeMessage("Set Value Already Exists", responseCode.SET_VALUE_ALREADY_EXIST, setValue)
                elif(result == NoSqlDb.SET_INSERT_SUCCESS):
                    msg = self.makeMessage("Set Insert Success", responseCode.SET_INSERT_SUCCESS, setName)
                else:
                    msg = self.makeMessage("Database Error", responseCode.DB_ERROR, dbName)
            else:
                msg = self.makeMessage("Set Does Not Exist", responseCode.SET_NOT_EXIST, setName)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, setName)
        return msg

    # remove the given value from a set
    def rmFromSet(self, dbName, setName, setValue):
        if(self.isValidType(dbName) and self.isValidType(setName)):
            if(self.database.isSetExist(dbName, setName)):
                result = self.database.rmFromSet(dbName, setName, setValue)
                if(result == NoSqlDb.SET_LOCKED):
                    msg = self.makeMessage("Set Is Locked", responseCode.SET_IS_LOCKED, setName)
                elif(result == NoSqlDb.SET_VALUE_NOT_EXISTED):
                    msg = self.makeMessage("Set Value Does Not Exist", responseCode.SET_VALUE_NOT_EXIST, setValue)
                elif(result == NoSqlDb.SET_REMOVE_SUCCESS):
                    msg = self.makeMessage("Set Remove Success", responseCode.SET_REMOVE_SUCCESS, setValue)
                else:
                    msg = self.makeMessage("Database Error", responseCode.DB_ERROR, dbName)
            else:
                msg = self.makeMessage("Set Does Not Exist", responseCode.SET_NOT_EXIST, setName)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, setName)
        return msg

    # clear the given set
    def clearSet(self, dbName, setName):
        if(self.isValidType(dbName) and self.isValidType(setName)):
            if(self.database.isSetExist(dbName, setName)):
                result = self.database.clearSet(dbName, setName)
                if(result == NoSqlDb.SET_LOCKED):
                    msg = self.makeMessage("Set Is Locked", responseCode.SET_IS_LOCKED, setName)
                elif(result == NoSqlDb.SET_CLEAR_SUCCESS):
                    msg = self.makeMessage("Set Clear Success", responseCode.SET_CLEAR_SUCCESS, setName)
                else:
                    msg = self.makeMessage("Database Error", responseCode.DB_ERROR, dbName)
            else:
                msg = self.makeMessage("Set Does Not Exist", responseCode.SET_NOT_EXIST, setName)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, setName)
        return msg

    def deleteSet(self, dbName, setName):
        if (self.isValidType(dbName) and self.isValidType(setName)):
            if (self.database.isSetExist(dbName, setName)):
                result = self.database.deleteSet(dbName, setName)
                if (result == NoSqlDb.SET_LOCKED):
                    msg = self.makeMessage("Set Is Locked", responseCode.SET_IS_LOCKED, setName)
                elif (result == NoSqlDb.SET_DELETE_SUCCESS):
                    msg = self.makeMessage("Set Delete Success", responseCode.SET_CLEAR_SUCCESS, setName)
                else:
                    msg = self.makeMessage("Database Error", responseCode.DB_ERROR, dbName)
            else:
                msg = self.makeMessage("Set Does Not Exist", responseCode.SET_NOT_EXIST, setName)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, setName)
        return msg