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
            code = self.database.delDatabase(dbName)
        else:
            code = responseCode.ELEM_TYPE_ERROR
        msg = Utils.makeMessage(responseCode.detail[code],
                                code,
                                dbName)
        return msg

    # save the data into file
    def saveDb(self):
        code = self.database.saveDb()
        return Utils.makeMessage(responseCode.detail[code],
                                 code,
                                 time.time())

    # set db's password
    def setDbPassword(self, adminKey, dbName, password):
        if self.database.isDbExist(dbName) is False:
            return Utils.makeMessage(responseCode.detail[responseCode.DB_NOT_EXIST],
                                    responseCode.DB_NOT_EXIST,
                                    dbName)

        code = self.database.setDbPassword(adminKey, dbName, password)
        return Utils.makeMessage(responseCode.detail[code],
                                code,
                                dbName)

    # change db's password
    def changeDbPassword(self, adminKey, dbName, originalPwd, newPwd):
        if self.database.isDbExist(dbName) is False:
            return Utils.makeMessage(responseCode.detail[responseCode.DB_NOT_EXIST],
                                     responseCode.DB_NOT_EXIST,
                                     dbName)

        code = self.database.changeDbPassword(adminKey, dbName, originalPwd, newPwd)
        return Utils.makeMessage(responseCode.detail[code],
                                 code,
                                 dbName)

    # remove db's password
    def removeDbPassword(self, adminKey, dbName):
        if self.database.isDbExist(dbName) is False:
            return Utils.makeMessage(responseCode.detail[responseCode.DB_NOT_EXIST],
                                     responseCode.DB_NOT_EXIST,
                                     dbName)

        code = self.database.removeDbPassword(adminKey, dbName)
        return Utils.makeMessage(responseCode.detail[code],
                                code,
                                dbName)
