__author__ = 'Ma Haoxiang'

# import
import time
from Response import responseCode

class DbHandler:
    def __init__(self, database):
        self.database = database

    # check if the type of elem is valid (string or int)
    def isValidType(self, elem):
        return isinstance(elem, str) is False and isinstance(elem, int) is False

    # check if the type of an elem is INT
    def isInt(self, elem):
        return isinstance(elem, int)

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
        if self.isValidType(dbName):
            result = self.database.addDb(dbName)
            msg = self.makeMessage(responseCode.detail[result], result, dbName)
        else:
            msg = self.makeMessage(responseCode.detail[responseCode.ELEM_TYPE_ERROR],
                                   responseCode.ELEM_TYPE_ERROR,
                                   dbName)
        return msg

    # get all database names
    def getAllDatabase(self):
        dbNameSet = self.database.getAllDatabase()
        msg = self.makeMessage(responseCode.detail[responseCode.DB_GET_SUCCESS],
                               responseCode.DB_GET_SUCCESS,
                               dbNameSet)
        return msg

    # delete the given database
    def delDatabase(self, dbName):
        if self.isValidType(dbName):
            result = self.database.delDatabase(dbName)
            msg = self.makeMessage(responseCode.detail[result], result, dbName)
        else:
            msg = self.makeMessage(responseCode.detail[responseCode.ELEM_TYPE_ERROR],
                                   responseCode.ELEM_TYPE_ERROR,
                                   dbName)
        return msg

    # save the data into file
    def saveDb(self):
        result = self.database.saveDb()
        msg = self.makeMessage(responseCode.detail[result], result, time.time())
        return msg
