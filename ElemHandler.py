__author__ = 'Ma Haoxiang'

# import
from Decorator import *
from Utils import Utils

class ElemHandler(object):

    def __init__(self, database):
        self.database = database

    # create an element in the db
    @validTypeCheck
    def createElem(self, dbName, keyName, value):
        if self.database.isDbExist(dbName) is False:
            msg = Utils.makeMessage(responseCode.detail[responseCode.DB_NOT_EXIST],
                                   responseCode.DB_NOT_EXIST,
                                   dbName)
            return msg

        if Utils.isValidType(value): # check the type of elem name and elem value
            if self.database.isExist("ELEM", dbName, keyName) is False:
                result = self.database.createElem(dbName, keyName, value)
                msg = Utils.makeMessage(responseCode.detail[result],
                                       result,
                                        keyName)
            else:
                msg = Utils.makeMessage(responseCode.detail[responseCode.ELEM_ALREADY_EXIST],
                                       responseCode.ELEM_ALREADY_EXIST,
                                        keyName)
        else:   # the type of elem name or elem value is invalid
            msg = Utils.makeMessage(responseCode.detail[responseCode.ELEM_TYPE_ERROR],
                                   responseCode.ELEM_TYPE_ERROR,
                                    keyName)
        return msg

    # update the value of an elem in the db
    @validTypeCheck
    def updateElem(self, dbName, keyName, value):
        if Utils.isValidType(value) is False:
            msg = Utils.makeMessage(responseCode.detail[responseCode.ELEM_TYPE_ERROR],
                                   responseCode.ELEM_TYPE_ERROR,
                                    keyName)
            return msg

        if self.database.isExist("ELEM", dbName, keyName):
            if self.database.isExpired("ELEM", dbName, keyName) is False:
                result = self.database.updateElem(dbName, keyName, value)
                msg = Utils.makeMessage(responseCode.detail[result], result, keyName)
            else:
                msg = Utils.makeMessage(responseCode.detail[responseCode.ELEM_EXPIRED],
                                       responseCode.ELEM_EXPIRED,
                                        keyName)
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.ELEM_NOT_EXIST],
                                   responseCode.ELEM_NOT_EXIST,
                                    keyName)
        return msg

    # get the value of existed elem
    @validTypeCheck
    def getElem(self, dbName, keyName):
        if self.database.isDbExist(dbName):
            if self.database.isExist("ELEM", dbName, keyName) is False:
                msg = Utils.makeMessage(responseCode.detail[responseCode.ELEM_NOT_EXIST],
                                       responseCode.ELEM_NOT_EXIST,
                                        keyName)
            else:
                if self.database.isExpired("ELEM", dbName, keyName) is False:
                    msg = Utils.makeMessage(responseCode.detail[responseCode.ELEM_GET_SUCCESS],
                                           responseCode.ELEM_GET_SUCCESS,
                                           self.database.getElem(keyName, dbName))
                else:
                    msg = Utils.makeMessage(responseCode.detail[responseCode.ELEM_EXPIRED],
                                           responseCode.ELEM_EXPIRED,
                                            keyName)
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.DB_NOT_EXIST],
                                   responseCode.DB_NOT_EXIST,
                                   dbName)
        return msg

    # search element using regular expression
    def searchElem(self, dbName, expression):
        if Utils.isValidType(dbName):
            searchResult = self.database.searchByRE(dbName, expression, "ELEM")
            msg = Utils.makeMessage(responseCode.detail[responseCode.ELEM_SEARCH_SUCCESS],
                                   responseCode.ELEM_SEARCH_SUCCESS,
                                   searchResult)
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.ELEM_TYPE_ERROR],
                                   responseCode.ELEM_TYPE_ERROR,
                                   dbName)
        return msg

    # get all element names in the db
    def searchAllElem(self, dbName):
        msg = Utils.makeMessage(responseCode.detail[responseCode.ELEM_SEARCH_SUCCESS],
                               responseCode.ELEM_SEARCH_SUCCESS,
                               self.database.searchAllElem(dbName))
        return msg

    # increase the value of an element
    @validTypeCheck
    def increaseElem(self, dbName, keyName):
        if self.database.isExist("ELEM", dbName, keyName) is False:
            msg = Utils.makeMessage(responseCode.detail[responseCode.ELEM_NOT_EXIST],
                                    responseCode.ELEM_NOT_EXIST,
                                    keyName)
        else:
            if self.database.isExpired("ELEM", dbName, keyName) is False:
                if Utils.isInt(self.database.getElem(keyName, dbName)): # check if the element can be increased
                    result = self.database.increaseElem(dbName, keyName)
                    msg = Utils.makeMessage(responseCode.detail[result],
                                            result,
                                            keyName)
                else:
                    msg = Utils.makeMessage(responseCode.detail[responseCode.ELEM_TYPE_ERROR],
                                            responseCode.ELEM_TYPE_ERROR,
                                            keyName)
            else:
                msg = Utils.makeMessage(responseCode.detail[responseCode.ELEM_EXPIRED],
                                        responseCode.ELEM_EXPIRED,
                                        keyName)
        return msg

    # decrease the value of an element
    @validTypeCheck
    def decreaseElem(self, dbName, keyName):
        if self.database.isExist("ELEM", dbName, keyName) is False:
            msg = Utils.makeMessage(responseCode.detail[responseCode.ELEM_NOT_EXIST],
                                    responseCode.ELEM_NOT_EXIST,
                                    keyName)
        else:
            if self.database.isExpired("ELEM", dbName, keyName) is False:
                if Utils.isInt(self.database.getElem(keyName, dbName)): # check if the element can be increased
                    result = self.database.decreaseElem(dbName, keyName)
                    msg = Utils.makeMessage(responseCode.detail[result], result, keyName)
                else:
                    msg = Utils.makeMessage(responseCode.detail[responseCode.ELEM_TYPE_ERROR],
                                            responseCode.ELEM_TYPE_ERROR,
                                            keyName)
            else:
                msg = Utils.makeMessage(responseCode.detail[responseCode.ELEM_EXPIRED],
                                        responseCode.ELEM_EXPIRED,
                                        keyName)
        return msg

    # delete an element in the database
    @validTypeCheck
    def deleteElem(self, dbName, keyName):
        if self.database.isExist("ELEM", dbName, keyName) is False:
            msg = Utils.makeMessage(responseCode.detail[responseCode.ELEM_NOT_EXIST],
                                    responseCode.ELEM_NOT_EXIST,
                                    keyName)
        else:
            result = self.database.deleteElem(keyName, dbName)
            msg = Utils.makeMessage(responseCode.detail[result],
                                    result,
                                    keyName)
        return msg

    @validTypeCheck
    def showTTL(self, dbName, keyName):
        if self.database.isDbExist(dbName):
            code, result = self.database.showTTL(dbName, keyName, "ELEM")
            msg = Utils.makeMessage(responseCode.detail[code],
                                   code,
                                   result)
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.DB_NOT_EXIST],
                                   responseCode.DB_NOT_EXIST,
                                   dbName)
        return msg
