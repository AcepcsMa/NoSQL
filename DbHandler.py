__author__ = 'Ma Haoxiang'

# import
import time
from Response import responseCode
from Utils import Utils

class DbHandler(object):

    def __init__(self, database):
        self.database = database

    # add a customized database
    def addDatabase(self, adminKey, dbName):
        if Utils.isValidType(dbName):
            code = self.database.addDb(adminKey=adminKey,
                                         dbName=dbName)
        else:
            code = responseCode.ELEM_TYPE_ERROR
        msg = Utils.makeMessage(responseCode.detail[code],
                               code,
                               dbName)
        return msg

    # get all database names
    def getAllDatabase(self, adminKey):
        code, result = self.database.getAllDatabase(adminKey=adminKey)
        msg = Utils.makeMessage(responseCode.detail[code],
                                code,
                                result)
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

    # set db's password
    def setDbPassword(self, adminKey, dbName, password):
        if self.database.isDbExist(dbName) is False:
            return Utils.makeMessage(responseCode.detail[responseCode.DB_NOT_EXIST],
                                    responseCode.DB_NOT_EXIST,
                                    dbName)

        result = self.database.setDbPassword(adminKey, dbName, password)
        msg = Utils.makeMessage(responseCode.detail[result],
                                result,
                                dbName)
        return msg

    # change db's password
    def changeDbPassword(self, adminKey, dbName, originalPwd, newPwd):
        if self.database.isDbExist(dbName) is False:
            return Utils.makeMessage(responseCode.detail[responseCode.DB_NOT_EXIST],
                                     responseCode.DB_NOT_EXIST,
                                     dbName)

        result = self.database.changeDbPassword(adminKey, dbName, originalPwd, newPwd)
        msg = Utils.makeMessage(responseCode.detail[result],
                                result,
                                dbName)
        return msg

    # remove db's password
    def removeDbPassword(self, adminKey, dbName):
        if self.database.isDbExist(dbName) is False:
            return Utils.makeMessage(responseCode.detail[responseCode.DB_NOT_EXIST],
                                     responseCode.DB_NOT_EXIST,
                                     dbName)

        result = self.database.removeDbPassword(adminKey, dbName)
        msg = Utils.makeMessage(responseCode.detail[result],
                                result,
                                dbName)
        return msg
