__author__ = 'Ma Haoxiang'

# import
from Decorator import *
from Utils import Utils

class ElemHandler(object):

    def __init__(self, database):
        self.database = database

    # create an element in the db
    @validTypeCheck
    def createElem(self, dbName, keyName, value, password=None):
        if self.database.isDbExist(dbName) is False:
            return Utils.makeMessage(responseCode.detail[responseCode.DB_NOT_EXIST],
                                   responseCode.DB_NOT_EXIST,
                                   dbName)

        if Utils.isValidType(value): # check the type of elem name and elem value
            if self.database.isExist("ELEM", dbName, keyName) is False:
                result = self.database.createElem(dbName=dbName,
                                                  keyName=keyName,
                                                  value=value,
                                                  password=password)
                code = result
            else:
                code = responseCode.ELEM_ALREADY_EXIST
        else:   # the type of elem name or elem value is invalid
            code = responseCode.ELEM_TYPE_ERROR
        msg = Utils.makeMessage(responseCode.detail[code],
                                code,
                                keyName)
        return msg

    # update the value of an elem in the db
    @validTypeCheck
    def updateElem(self, dbName, keyName, value, password=None):
        if Utils.isValidType(value) is False:
            return Utils.makeMessage(responseCode.detail[responseCode.ELEM_TYPE_ERROR],
                                    responseCode.ELEM_TYPE_ERROR,
                                    keyName)

        if self.database.isExist("ELEM", dbName, keyName):
            if self.database.isExpired("ELEM", dbName, keyName) is False:
                result = self.database.updateElem(dbName=dbName,
                                                  keyName=keyName,
                                                  value=value,
                                                  password=password)
            else:
                result = responseCode.ELEM_EXPIRED
        else:
            result = responseCode.ELEM_NOT_EXIST
        msg = Utils.makeMessage(responseCode.detail[result],
                                        result,
                                        keyName)
        return msg

    # get the value of existed elem
    @validTypeCheck
    def getElem(self, dbName, keyName, password):
        if self.database.isDbExist(dbName):
            if self.database.isExist("ELEM", dbName, keyName) is False:
                code = responseCode.ELEM_NOT_EXIST
                result = keyName
            else:
                if self.database.isExpired("ELEM", dbName, keyName) is False:
                    result = self.database.getElem(dbName=dbName,
                                                   keyName=keyName,
                                                   password=password)
                    code = result if result == responseCode.DB_PASSWORD_ERROR else result[0]
                    result = dbName if result == responseCode.DB_PASSWORD_ERROR else result[1]
                else:
                    code = responseCode.ELEM_EXPIRED
                    result = keyName
        else:
            code = responseCode.DB_NOT_EXIST
            result = dbName
        msg = Utils.makeMessage(responseCode.detail[code],
                                code,
                                result)
        return msg

    # search element using regular expression
    def searchElem(self, dbName, expression, password=None):
        if Utils.isValidType(dbName):
            searchResult = self.database.searchByRE(dbName=dbName,
                                                    expression=expression,
                                                    dataType="ELEM",
                                                    password=password)
            code = searchResult if searchResult == responseCode.DB_PASSWORD_ERROR \
                                else responseCode.ELEM_SEARCH_SUCCESS
            result = dbName if searchResult == responseCode.DB_PASSWORD_ERROR \
                            else searchResult
        else:
            code = responseCode.ELEM_TYPE_ERROR
            result = dbName
        msg = Utils.makeMessage(responseCode.detail[code],
                                code,
                                result)
        return msg

    # get all element names in the db
    def searchAllElem(self, dbName, password=None):
        searchResult = self.database.searchAllElem(dbName=dbName,
                                                   password=password)
        code = searchResult if searchResult == responseCode.DB_PASSWORD_ERROR \
            else responseCode.ELEM_SEARCH_SUCCESS
        result = dbName if searchResult == responseCode.DB_PASSWORD_ERROR \
            else searchResult
        msg = Utils.makeMessage(responseCode.detail[code],
                               code,
                               result)
        return msg

    # increase the value of an element
    @validTypeCheck
    def increaseElem(self, dbName, keyName, password=None):
        if self.database.isExist("ELEM", dbName, keyName) is False:
            code = responseCode.ELEM_NOT_EXIST
        else:
            if self.database.isExpired("ELEM", dbName, keyName) is False:
                if Utils.isInt(self.database.getElem(keyName, dbName)): # check if the element can be increased
                    code = self.database.increaseElem(dbName=dbName,
                                                      keyName=keyName,
                                                      password=password)
                else:
                    code = responseCode.ELEM_TYPE_ERROR
            else:
                code = responseCode.ELEM_EXPIRED
        msg = Utils.makeMessage(responseCode.detail[code],
                                code,
                                keyName)
        return msg

    # decrease the value of an element
    @validTypeCheck
    def decreaseElem(self, dbName, keyName, password=None):
        if self.database.isExist("ELEM", dbName, keyName) is False:
            code = responseCode.ELEM_NOT_EXIST
        else:
            if self.database.isExpired("ELEM", dbName, keyName) is False:
                if Utils.isInt(self.database.getElem(keyName, dbName)): # check if the element can be increased
                    code = self.database.decreaseElem(dbName=dbName,
                                                      keyName=keyName,
                                                      password=password)
                else:
                    code = responseCode.ELEM_TYPE_ERROR
            else:
                code = responseCode.ELEM_EXPIRED
        msg = Utils.makeMessage(responseCode.detail[code],
                                code,
                                keyName)
        return msg

    # delete an element in the database
    @validTypeCheck
    def deleteElem(self, dbName, keyName, password=None):
        if self.database.isExist("ELEM", dbName, keyName) is False:
            msg = Utils.makeMessage(responseCode.detail[responseCode.ELEM_NOT_EXIST],
                                    responseCode.ELEM_NOT_EXIST,
                                    keyName)
        else:
            result = self.database.deleteElem(dbName=dbName,
                                              keyName=keyName,
                                              password=password)
            msg = Utils.makeMessage(responseCode.detail[result],
                                    result,
                                    keyName)
        return msg
