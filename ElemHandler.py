__author__ = 'Ma Haoxiang'

# import
from Decorator import *
from Utils import Utils

class ElemHandler(object):

    def __init__(self, database):
        self.database = database

    # create an element in the db
    @validTypeCheck
    def createElem(self, dbName, elemName, elemValue):
        if self.database.isDbExist(dbName) is False:
            msg = Utils.makeMessage(responseCode.detail[responseCode.DB_NOT_EXIST],
                                   responseCode.DB_NOT_EXIST,
                                   dbName)
            return msg

        if Utils.isValidType(elemValue): # check the type of elem name and elem value
            if self.database.isExist("ELEM", dbName, elemName) is False:
                result = self.database.createElem(dbName, elemName, elemValue)
                msg = Utils.makeMessage(responseCode.detail[result],
                                       result,
                                       elemName)
            else:
                msg = Utils.makeMessage(responseCode.detail[responseCode.ELEM_ALREADY_EXIST],
                                       responseCode.ELEM_ALREADY_EXIST,
                                       elemName)
        else:   # the type of elem name or elem value is invalid
            msg = Utils.makeMessage(responseCode.detail[responseCode.ELEM_TYPE_ERROR],
                                   responseCode.ELEM_TYPE_ERROR,
                                   elemName)
        return msg

    # update the value of an elem in the db
    @validTypeCheck
    def updateElem(self, dbName, elemName, elemValue):
        if Utils.isValidType(elemValue) is False:
            msg = Utils.makeMessage(responseCode.detail[responseCode.ELEM_TYPE_ERROR],
                                   responseCode.ELEM_TYPE_ERROR,
                                   elemName)
            return msg

        if self.database.isExist("ELEM", dbName, elemName):
            if self.database.isExpired("ELEM", dbName, elemName) is False:
                result = self.database.updateElem(dbName, elemName, elemValue)
                msg = Utils.makeMessage(responseCode.detail[result], result, elemName)
            else:
                msg = Utils.makeMessage(responseCode.detail[responseCode.ELEM_EXPIRED],
                                       responseCode.ELEM_EXPIRED,
                                       elemName)
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.ELEM_NOT_EXIST],
                                   responseCode.ELEM_NOT_EXIST,
                                   elemName)
        return msg

    # get the value of existed elem
    @validTypeCheck
    def getElem(self, dbName, elemName):
        if self.database.isDbExist(dbName):
            if self.database.isExist("ELEM", dbName, elemName) is False:
                msg = Utils.makeMessage(responseCode.detail[responseCode.ELEM_NOT_EXIST],
                                       responseCode.ELEM_NOT_EXIST,
                                       elemName)
            else:
                if self.database.isExpired("ELEM", dbName, elemName) is False:
                    msg = Utils.makeMessage(responseCode.detail[responseCode.ELEM_GET_SUCCESS],
                                           responseCode.ELEM_GET_SUCCESS,
                                           self.database.getElem(elemName, dbName))
                else:
                    msg = Utils.makeMessage(responseCode.detail[responseCode.ELEM_EXPIRED],
                                           responseCode.ELEM_EXPIRED,
                                           elemName)
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
    def increaseElem(self, dbName, elemName):
        if self.database.isExist("ELEM", dbName, elemName) is False:
            msg = Utils.makeMessage(responseCode.detail[responseCode.ELEM_NOT_EXIST],
                                   responseCode.ELEM_NOT_EXIST,
                                   elemName)
        else:
            if self.database.isExpired("ELEM", dbName, elemName) is False:
                if Utils.isInt(self.database.getElem(elemName, dbName)): # check if the element can be increased
                    result = self.database.increaseElem(elemName, dbName)
                    msg = Utils.makeMessage(responseCode.detail[result],
                                           result,
                                           elemName)
                else:
                    msg = Utils.makeMessage(responseCode.detail[responseCode.ELEM_TYPE_ERROR],
                                           responseCode.ELEM_TYPE_ERROR,
                                           elemName)
            else:
                msg = Utils.makeMessage(responseCode.detail[responseCode.ELEM_EXPIRED],
                                       responseCode.ELEM_EXPIRED,
                                       elemName)
        return msg

    # decrease the value of an element
    @validTypeCheck
    def decreaseElem(self, dbName, elemName):
        if self.database.isExist("ELEM", dbName, elemName) is False:
            msg = Utils.makeMessage(responseCode.detail[responseCode.ELEM_NOT_EXIST],
                                   responseCode.ELEM_NOT_EXIST,
                                   elemName)
        else:
            if self.database.isExpired("ELEM", dbName, elemName) is False:
                if Utils.isInt(self.database.getElem(elemName, dbName)): # check if the element can be increased
                    result = self.database.decreaseElem(elemName, dbName)
                    msg = Utils.makeMessage(responseCode.detail[result], result, elemName)
                else:
                    msg = Utils.makeMessage(responseCode.detail[responseCode.ELEM_TYPE_ERROR],
                                           responseCode.ELEM_TYPE_ERROR,
                                           elemName)
            else:
                msg = Utils.makeMessage(responseCode.detail[responseCode.ELEM_EXPIRED],
                                       responseCode.ELEM_EXPIRED,
                                       elemName)
        return msg

    # delete an element in the database
    @validTypeCheck
    def deleteElem(self, dbName, elemName):
        if self.database.isExist("ELEM", dbName, elemName) is False:
            msg = Utils.makeMessage(responseCode.detail[responseCode.ELEM_NOT_EXIST],
                                   responseCode.ELEM_NOT_EXIST,
                                   elemName)
        else:
            result = self.database.deleteElem(elemName, dbName)
            msg = Utils.makeMessage(responseCode.detail[result],
                                   result,
                                   elemName)
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
