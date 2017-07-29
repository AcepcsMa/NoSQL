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
