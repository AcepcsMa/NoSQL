__author__ = 'Ma Haoxiang'

# import
import time
# from db import NoSqlDb
from response import responseCode

class dbHandler:
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

    # add a customized database
    def addDatabase(self, dbName):
        if(self.isValidType(dbName)):
            result = self.database.addDb(dbName)
            if(result == responseCode.DB_EXISTED):
                msg = self.makeMessage("Database Already Exists", responseCode.DB_EXISTED, dbName)
            elif(result == responseCode.DB_CREATE_SUCCESS):
                msg = self.makeMessage("Database Create Success", responseCode.DB_CREATE_SUCCESS, dbName)
            elif(result == responseCode.DB_SAVE_LOCKED):
                msg = self.makeMessage("Database Is Locked", responseCode.DB_SAVE_LOCKED, dbName)
            else:
                msg = self.makeMessage("Database Error", responseCode.DB_ERROR, dbName)
        else:
            msg = self.makeMessage("Database Name Type Error", responseCode.ELEM_TYPE_ERROR, dbName)
        return msg

    # get all database names
    def getAllDatabase(self):
        dbNameSet = self.database.getAllDatabase()
        msg = self.makeMessage("Database Get Success", responseCode.DB_GET_SUCCESS, dbNameSet)
        return msg

    # delete the given database
    def delDatabase(self, dbName):
        if(self.isValidType(dbName)):
            result = self.database.delDatabase(dbName)
            if(result == responseCode.DB_DELETE_SUCCESS):
                msg = self.makeMessage("Database Delete Success", responseCode.DB_DELETE_SUCCESS, dbName)
            elif(result == responseCode.DB_SAVE_LOCKED):
                msg = self.makeMessage("Database Save Locked", responseCode.DB_SAVE_LOCKED, dbName)
            elif(result == responseCode.DB_NOT_EXIST):
                msg = self.makeMessage("Database Does Not Exist", responseCode.DB_NOT_EXIST, dbName)
            else:
                msg = self.makeMessage("Database Error", responseCode.DB_ERROR, dbName)
        else:
            msg = self.makeMessage("Database Name Type Error", responseCode.ELEM_TYPE_ERROR, dbName)
        return msg

    # save the data into file
    def saveDb(self):
        result = self.database.saveDb()
        if(result == responseCode.DB_SAVE_SUCCESS):
            msg = self.makeMessage("Database Save Success", responseCode.DB_SAVE_SUCCESS, time.time())
        elif(result == responseCode.DB_SAVE_LOCKED):
            msg = self.makeMessage("Database Save Locked", responseCode.DB_SAVE_LOCKED, time.time())
        else:
            msg = self.makeMessage("Database Error", responseCode.DB_ERROR,"");
        return msg


if __name__ == "__main__":
    pass
