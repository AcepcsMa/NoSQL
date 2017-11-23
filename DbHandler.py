__author__ = 'Ma Haoxiang'

# import
import time
from Response import responseCode
from Utils import Utils

class DbHandler(object):

    def __init__(self, database):
        self.database = database

    # add a customized database
    def addDatabase(self, dbName):
        if Utils.isValidType(dbName):
            result = self.database.addDb(dbName)
            msg = Utils.makeMessage(responseCode.detail[result], result, dbName)
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.ELEM_TYPE_ERROR],
                                   responseCode.ELEM_TYPE_ERROR,
                                   dbName)
        return msg

    # get all database names
    def getAllDatabase(self):
        dbNameSet = self.database.getAllDatabase()
        msg = Utils.makeMessage(responseCode.detail[responseCode.DB_GET_SUCCESS],
                               responseCode.DB_GET_SUCCESS,
                               dbNameSet)
        return msg

    # delete the given database
    def delDatabase(self, dbName):
        if Utils.isValidType(dbName):
            result = self.database.delDatabase(dbName)
            msg = Utils.makeMessage(responseCode.detail[result], result, dbName)
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.ELEM_TYPE_ERROR],
                                   responseCode.ELEM_TYPE_ERROR,
                                   dbName)
        return msg

    # save the data into file
    def saveDb(self):
        result = self.database.saveDb()
        msg = Utils.makeMessage(responseCode.detail[result], result, time.time())
        return msg
