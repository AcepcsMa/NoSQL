__author__ = 'Ma Haoxiang'

# import
from Decorator import *
from Utils import Utils

class SetHandler(object):

    def __init__(self, database):
        self.database = database

    # create a set
    @validTypeCheck
    def createSet(self, dbName, keyName, password=None):
        if self.database.isDbExist(dbName) is False:
            return Utils.makeMessage(responseCode.detail[responseCode.DB_NOT_EXIST],
                                    responseCode.DB_NOT_EXIST,
                                    dbName)

        if self.database.isExist("SET", dbName, keyName) is False:
            code = self.database.createSet(dbName=dbName,
                                           keyName=keyName,
                                           password=password)
        else:
            code = responseCode.SET_ALREADY_EXIST
        msg = Utils.makeMessage(responseCode.detail[code],
                                code,
                                keyName)
        return msg

    # get set value
    @validTypeCheck
    def getSet(self, dbName, keyName, password=None):
        if self.database.isExist("SET", dbName, keyName) is True:
            if self.database.isExpired("SET", dbName, keyName) is False:
                result = self.database.getSet(dbName=dbName,
                                              keyName=keyName,
                                              password=password)
                code = responseCode.SET_GET_SUCCESS
            else:
                code, result = responseCode.SET_EXPIRED, keyName
        else:
            code, result = responseCode.SET_NOT_EXIST, keyName
        msg = Utils.makeMessage(responseCode.detail[code],
                                code,
                                result)
        return msg

    @validTypeCheck
    def getSetRandom(self, dbName, keyName, numRand, password=None):
        if numRand <= 0:
            return Utils.makeMessage(responseCode.detail[responseCode.INVALID_NUMBER],
                                   responseCode.INVALID_NUMBER,
                                   numRand)

        if self.database.isExist("SET", dbName, keyName) is True:
            if self.database.isExpired("SET", dbName, keyName) is False:
                code, result = self.database.getSetRandom(dbName=dbName, keyName=keyName,
                                                          numRand=numRand, password=password)
            else:
                code, result = responseCode.LIST_EXPIRED, keyName
        else:
            code, result = responseCode.LIST_NOT_EXIST, keyName
        msg = Utils.makeMessage(responseCode.detail[code],
                                code,
                                result)
        return msg

    # insert a value into the given set
    @validTypeCheck
    def insertSet(self, dbName, keyName, setValue):
        if self.database.isExist("SET", dbName, keyName):
            if self.database.isExpired("SET", dbName, keyName) is False:
                result = self.database.insertSet(dbName, keyName, setValue)
                msg = Utils.makeMessage(responseCode.detail[result],
                                        result,
                                        keyName)
            else:
                msg = Utils.makeMessage(responseCode.detail[responseCode.SET_EXPIRED],
                                        responseCode.SET_EXPIRED,
                                        keyName)
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.SET_NOT_EXIST],
                                    responseCode.SET_NOT_EXIST,
                                    keyName)
        return msg

    # remove the given value from a set
    @validTypeCheck
    def rmFromSet(self, dbName, keyName, setValue):
        if self.database.isExist("SET", dbName, keyName):
            if self.database.isExpired("SET", dbName, keyName) is False:
                result = self.database.rmFromSet(dbName, keyName, setValue)
                msg = Utils.makeMessage(responseCode.detail[result],
                                        result,
                                        keyName)
            else:
                msg = Utils.makeMessage(responseCode.detail[responseCode.SET_EXPIRED],
                                        responseCode.SET_EXPIRED,
                                        keyName)
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.SET_NOT_EXIST],
                                    responseCode.SET_NOT_EXIST,
                                    keyName)
        return msg

    # clear the given set
    @validTypeCheck
    def clearSet(self, dbName, keyName):
        if self.database.isExist("SET", dbName, keyName):
            if self.database.isExpired("SET", dbName, keyName) is False:
                result = self.database.clearSet(dbName, keyName)
                msg = Utils.makeMessage(responseCode.detail[result],
                                        result,
                                        keyName)
            else:
                msg = Utils.makeMessage(responseCode.detail[responseCode.SET_EXPIRED],
                                        responseCode.SET_EXPIRED,
                                        keyName)
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.SET_NOT_EXIST],
                                    responseCode.SET_NOT_EXIST,
                                    keyName)
        return msg

    # delete the given set
    @validTypeCheck
    def deleteSet(self, dbName, keyName):
        if self.database.isExist("SET", dbName, keyName):
            if self.database.isExpired("SET", dbName, keyName) is False:
                result = self.database.deleteSet(dbName, keyName)
                msg = Utils.makeMessage(responseCode.detail[result],
                                        result,
                                        keyName)
            else:
                msg = Utils.makeMessage(responseCode.detail[responseCode.SET_EXPIRED],
                                        responseCode.SET_EXPIRED,
                                        keyName)
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.SET_NOT_EXIST],
                                    responseCode.SET_NOT_EXIST,
                                    keyName)
        return msg

    # search set names using regular expression
    def searchSet(self, dbName, expression):
        if self.database.isDbExist(dbName) is False:
            return Utils.makeMessage(responseCode.detail[responseCode.DB_NOT_EXIST],
                                   responseCode.DB_NOT_EXIST,
                                   dbName)

        if Utils.isValidType(dbName):
            searchResult = self.database.searchByRE(dbName, expression, "SET")
            msg = Utils.makeMessage(responseCode.detail[responseCode.SET_SEARCH_SUCCESS],
                                   responseCode.SET_SEARCH_SUCCESS,
                                   searchResult)
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.ELEM_TYPE_ERROR],
                                   responseCode.ELEM_TYPE_ERROR,
                                   dbName)
        return msg

    # return all set names in the given database
    def searchAllSet(self, dbName):
        if Utils.isValidType(dbName):
            if self.database.isDbExist(dbName):
                searchResult = self.database.searchAllSet(dbName)
                msg = Utils.makeMessage(responseCode.detail[responseCode.SET_SEARCH_SUCCESS],
                                       responseCode.SET_SEARCH_SUCCESS,
                                       searchResult)
            else:
                msg = Utils.makeMessage(responseCode.detail[responseCode.DB_NOT_EXIST],
                                       responseCode.DB_NOT_EXIST,
                                       dbName)
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.ELEM_TYPE_ERROR],
                                   responseCode.ELEM_TYPE_ERROR,
                                   dbName)
        return msg

    # set union operation
    def unionSet(self, dbName, setName1, setName2):
        if Utils.isValidType(dbName, setName1, setName2) is False:
            return Utils.makeMessage(responseCode.detail[responseCode.ELEM_TYPE_ERROR],
                                   responseCode.ELEM_TYPE_ERROR,
                                   dbName)

        if self.database.isExist("SET", dbName, setName1, setName2):
            if self.database.isExpired("SET", dbName,setName1, setName2) is False:
                unionResult = [None]
                result = self.database.unionSet(dbName, setName1, setName2, unionResult)
                msg = Utils.makeMessage(responseCode.detail[result],
                                       result,
                                       unionResult[1])
            else:
                msg = Utils.makeMessage(responseCode.detail[responseCode.SET_EXPIRED],
                                       responseCode.SET_EXPIRED,
                                       "{} or {}".format(setName1, setName2))
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.SET_NOT_EXIST],
                                   responseCode.SET_NOT_EXIST,
                                   "{0} or {1}".format(setName1, setName2))
        return msg

    # set intersect operation
    def intersectSet(self, dbName, setName1, setName2):
        if Utils.isValidType(dbName, setName1, setName2) is False:
            return Utils.makeMessage(responseCode.detail[responseCode.ELEM_TYPE_ERROR],
                                   responseCode.ELEM_TYPE_ERROR,
                                   dbName)

        if self.database.isExist("SET", dbName, setName1, setName2):
            if self.database.isExpired("SET", dbName, setName1, setName2) is False:
                intersectResult = [None]
                result = self.database.intersectSet(dbName, setName1, setName2, intersectResult)
                msg = Utils.makeMessage(responseCode.detail[result],
                                       result,
                                       intersectResult[1])
            else:
                msg = Utils.makeMessage(responseCode.detail[responseCode.SET_EXPIRED],
                                       responseCode.SET_EXPIRED,
                                       "{} or {}".format(setName1, setName2))
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.SET_NOT_EXIST],
                                   responseCode.SET_NOT_EXIST,
                                   "{} or {}".format(setName1, setName2))
        return msg

    # set difference operation
    def diffSet(self, dbName, setName1, setName2):
        if Utils.isValidType(dbName, setName1, setName2) is False:
            return Utils.makeMessage(responseCode.detail[responseCode.ELEM_TYPE_ERROR],
                                   responseCode.ELEM_TYPE_ERROR,
                                   dbName)

        if self.database.isExist("SET", dbName, setName1, setName2):
            if self.database.isExpired("SET", dbName, setName1, setName2) is False:
                diffResult = [None]
                result = self.database.diffSet(dbName, setName1, setName2, diffResult)
                msg = Utils.makeMessage(responseCode.detail[result],
                                       result,
                                       diffResult[1])
            else:
                msg = Utils.makeMessage(responseCode.detail[responseCode.SET_EXPIRED],
                                       responseCode.SET_EXPIRED,
                                       "{} or {}".format(setName1, setName2))
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.SET_NOT_EXIST],
                                   responseCode.SET_NOT_EXIST,
                                   "{} or {}".format(setName1, setName2))
        return msg

    # replace the existed set with a new set
    @validTypeCheck
    def replaceSet(self, dbName, keyName, value):
        if Utils.isSet(value) is False:
            return Utils.makeMessage(responseCode.detail[responseCode.ELEM_TYPE_ERROR],
                                     responseCode.ELEM_TYPE_ERROR,
                                     keyName)

        if self.database.isExist("SET", dbName, keyName):
            if self.database.isExpired("SET", dbName, keyName) is False:
                result = self.database.replaceSet(dbName, keyName, value)
                msg = Utils.makeMessage(responseCode.detail[result],
                                        result,
                                        keyName)
            else:
                msg = Utils.makeMessage(responseCode.detail[responseCode.SET_EXPIRED],
                                        responseCode.SET_EXPIRED,
                                        keyName)
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.SET_NOT_EXIST],
                                    responseCode.SET_NOT_EXIST,
                                    keyName)
        return msg

    # show TTL for a set
    @validTypeCheck
    def showTTL(self, dbName, keyName):
        if self.database.isDbExist(dbName):
            code, result = self.database.showTTL(dbName, keyName, "SET")
            msg = Utils.makeMessage(responseCode.detail[code],
                                   code,
                                   result)
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.DB_NOT_EXIST],
                                   responseCode.DB_NOT_EXIST,
                                   dbName)
        return msg

    @validTypeCheck
    def getSize(self, dbName, keyName):
        if self.database.isExist("SET", dbName, keyName) is False:
            msg = Utils.makeMessage(responseCode.detail[responseCode.SET_NOT_EXIST],
                                    responseCode.SET_NOT_EXIST,
                                    keyName)
        else:
            code, result = self.database.getSize(dbName, keyName, "SET")
            msg = Utils.makeMessage(responseCode.detail[code],
                                    code,
                                    result)
        return msg
