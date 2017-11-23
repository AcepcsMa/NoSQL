__author__ = 'Ma Haoxiang'

# import
from Decorator import *
from Utils import Utils

class SetHandler(object):

    def __init__(self, database):
        self.database = database

    # create a set
    @validTypeCheck
    def createSet(self, dbName, setName):
        if self.database.isDbExist(dbName) is False:
            msg = Utils.makeMessage(responseCode.detail[responseCode.DB_NOT_EXIST],
                                   responseCode.DB_NOT_EXIST,
                                   dbName)
            return msg

        if self.database.isExist("SET", dbName, setName) is False:
            result = self.database.createSet(dbName, setName)
            msg = Utils.makeMessage(responseCode.detail[result],
                                   result,
                                   setName)
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.SET_ALREADY_EXIST],
                                   responseCode.SET_ALREADY_EXIST,
                                   setName)
        return msg

    # get set value
    @validTypeCheck
    def getSet(self, dbName, setName):
        if self.database.isExist("SET", dbName, setName) is True:
            if self.database.isExpired("SET", dbName, setName) is False:
                setValue = self.database.getSet(dbName, setName)
                msg = Utils.makeMessage(responseCode.detail[responseCode.SET_GET_SUCCESS],
                                       responseCode.SET_GET_SUCCESS,
                                       setValue)
            else:
                msg = Utils.makeMessage(responseCode.detail[responseCode.SET_EXPIRED],
                                       responseCode.SET_EXPIRED,
                                       setName)
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.SET_NOT_EXIST],
                                   responseCode.SET_NOT_EXIST,
                                   setName)
        return msg

    @validTypeCheck
    def getSetRandom(self, dbName, setName, numRand):
        if numRand <= 0:
            return Utils.makeMessage(responseCode.detail[responseCode.INVALID_NUMBER],
                                   responseCode.INVALID_NUMBER,
                                   numRand)

        if self.database.isExist("SET", dbName, setName) is True:
            if self.database.isExpired("SET", dbName, setName) is False:
                code, listValue = self.database.getSetRandom(dbName, setName, numRand)
                msg = Utils.makeMessage(responseCode.detail[code],
                                       code,
                                       listValue)
            else:
                msg = Utils.makeMessage(responseCode.detail[responseCode.SET_EXPIRED],
                                       responseCode.LIST_EXPIRED,
                                       setName)
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.SET_NOT_EXIST],
                                   responseCode.LIST_NOT_EXIST,
                                   setName)
        return msg

    # insert a value into the given set
    @validTypeCheck
    def insertSet(self, dbName, setName, setValue):
        if self.database.isExist("SET", dbName, setName):
            if self.database.isExpired("SET", dbName, setName) is False:
                result = self.database.insertSet(dbName, setName, setValue)
                msg = Utils.makeMessage(responseCode.detail[result],
                                       result,
                                       setName)
            else:
                msg = Utils.makeMessage(responseCode.detail[responseCode.SET_EXPIRED],
                                       responseCode.SET_EXPIRED,
                                       setName)
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.SET_NOT_EXIST],
                                   responseCode.SET_NOT_EXIST,
                                   setName)
        return msg

    # remove the given value from a set
    @validTypeCheck
    def rmFromSet(self, dbName, setName, setValue):
        if self.database.isExist("SET", dbName, setName):
            if self.database.isExpired("SET", dbName, setName) is False:
                result = self.database.rmFromSet(dbName, setName, setValue)
                msg = Utils.makeMessage(responseCode.detail[result],
                                       result,
                                       setName)
            else:
                msg = Utils.makeMessage(responseCode.detail[responseCode.SET_EXPIRED],
                                       responseCode.SET_EXPIRED,
                                       setName)
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.SET_NOT_EXIST],
                                   responseCode.SET_NOT_EXIST,
                                   setName)
        return msg

    # clear the given set
    @validTypeCheck
    def clearSet(self, dbName, setName):
        if self.database.isExist("SET", dbName, setName):
            if self.database.isExpired("SET", dbName, setName) is False:
                result = self.database.clearSet(dbName, setName)
                msg = Utils.makeMessage(responseCode.detail[result],
                                       result,
                                       setName)
            else:
                msg = Utils.makeMessage(responseCode.detail[responseCode.SET_EXPIRED],
                                       responseCode.SET_EXPIRED,
                                       setName)
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.SET_NOT_EXIST],
                                   responseCode.SET_NOT_EXIST,
                                   setName)
        return msg

    # delete the given set
    @validTypeCheck
    def deleteSet(self, dbName, setName):
        if self.database.isExist("SET", dbName, setName):
            if self.database.isExpired("SET", dbName, setName) is False:
                result = self.database.deleteSet(dbName, setName)
                msg = Utils.makeMessage(responseCode.detail[result],
                                       result,
                                       setName)
            else:
                msg = Utils.makeMessage(responseCode.detail[responseCode.SET_EXPIRED],
                                       responseCode.SET_EXPIRED,
                                       setName)
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.SET_NOT_EXIST],
                                   responseCode.SET_NOT_EXIST,
                                   setName)
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
    def replaceSet(self, dbName, setName, setValue):
        if self.isSet(setValue) is False:
            return Utils.makeMessage(responseCode.detail[responseCode.ELEM_TYPE_ERROR],
                                   responseCode.ELEM_TYPE_ERROR,
                                   setName)

        if self.database.isExist("SET", dbName, setName):
            if self.database.isExpired("SET", dbName, setName) is False:
                result = self.database.replaceSet(dbName, setName, setValue)
                msg = Utils.makeMessage(responseCode.detail[result],
                                       result,
                                       setName)
            else:
                msg = Utils.makeMessage(responseCode.detail[responseCode.SET_EXPIRED],
                                       responseCode.SET_EXPIRED,
                                       setName)
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.SET_NOT_EXIST],
                                   responseCode.SET_NOT_EXIST,
                                   setName)
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
    def getSize(self, dbName, setName):
        if self.database.isExist("SET", dbName, setName) is False:
            msg = Utils.makeMessage(responseCode.detail[responseCode.SET_NOT_EXIST],
                                   responseCode.SET_NOT_EXIST,
                                   setName)
        else:
            code, result = self.database.getSize(dbName, setName, "SET")
            msg = Utils.makeMessage(responseCode.detail[code],
                                   code,
                                   result)
        return msg
