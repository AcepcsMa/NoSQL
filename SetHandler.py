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
    def insertSet(self, dbName, keyName, setValue, password=None):
        if self.database.isExist("SET", dbName, keyName):
            if self.database.isExpired("SET", dbName, keyName) is False:
                code = self.database.insertSet(dbName=dbName, keyName=keyName,
                                               value=setValue, password=password)
            else:
                code = responseCode.SET_EXPIRED
        else:
            code = responseCode.SET_NOT_EXIST
        msg = Utils.makeMessage(responseCode.detail[code],
                                code,
                                keyName)
        return msg

    # remove the given value from a set
    @validTypeCheck
    def rmFromSet(self, dbName, keyName, setValue, password=None):
        if self.database.isExist("SET", dbName, keyName):
            if self.database.isExpired("SET", dbName, keyName) is False:
                code = self.database.rmFromSet(dbName=dbName, keyName=keyName,
                                               value=setValue, password=password)
            else:
                code = responseCode.SET_EXPIRED
        else:
            code = responseCode.SET_NOT_EXIST
        msg = Utils.makeMessage(responseCode.detail[code],
                                code,
                                keyName)
        return msg

    # clear the given set
    @validTypeCheck
    def clearSet(self, dbName, keyName, password=None):
        if self.database.isExist("SET", dbName, keyName):
            if self.database.isExpired("SET", dbName, keyName) is False:
                code = self.database.clearSet(dbName=dbName,
                                              keyName=keyName,
                                              password=password)
            else:
                code = responseCode.SET_EXPIRED
        else:
            code = responseCode.SET_NOT_EXIST
        msg = Utils.makeMessage(responseCode.detail[code],
                                code,
                                keyName)
        return msg

    # delete the given set
    @validTypeCheck
    def deleteSet(self, dbName, keyName, password=None):
        if self.database.isExist("SET", dbName, keyName):
            if self.database.isExpired("SET", dbName, keyName) is False:
                code = self.database.deleteSet(dbName=dbName,
                                               keyName=keyName,
                                               password=password)
            else:
                code = responseCode.SET_EXPIRED
        else:
            code = responseCode.SET_NOT_EXIST
        msg = Utils.makeMessage(responseCode.detail[code],
                                code,
                                keyName)
        return msg

    # search set names using regular expression
    def searchSet(self, dbName, expression, password=None):
        if self.database.isDbExist(dbName) is False:
            return Utils.makeMessage(responseCode.detail[responseCode.DB_NOT_EXIST],
                                     responseCode.DB_NOT_EXIST,
                                     dbName)

        if Utils.isValidType(dbName):
            result = self.database.searchByRE(dbName=dbName, expression=expression,
                                              dataType="SET", password=password)
            code = responseCode.SET_SEARCH_SUCCESS
        else:
            code, result = responseCode.ELEM_TYPE_ERROR, dbName
        msg = Utils.makeMessage(responseCode.detail[code],
                                code,
                                result)
        return msg

    # return all set names in the given database
    def searchAllSet(self, dbName, password=None):
        if Utils.isValidType(dbName):
            if self.database.isDbExist(dbName):
                result = self.database.searchAllSet(dbName=dbName,
                                                    password=password)
                code = responseCode.SET_SEARCH_SUCCESS
            else:
                code, result = responseCode.DB_NOT_EXIST, dbName
        else:
            code, result = responseCode.ELEM_TYPE_ERROR, dbName
        msg = Utils.makeMessage(responseCode.detail[code],
                                code,
                                result)
        return msg

    # set union operation
    def unionSet(self, dbName, setName1, setName2, password=None):
        if Utils.isValidType(dbName, setName1, setName2) is False:
            return Utils.makeMessage(responseCode.detail[responseCode.ELEM_TYPE_ERROR],
                                     responseCode.ELEM_TYPE_ERROR,
                                     dbName)

        if self.database.isExist("SET", dbName, setName1, setName2):
            if self.database.isExpired("SET", dbName,setName1, setName2) is False:
                unionResult = [None]
                code = self.database.unionSet(dbName=dbName, keyName1=setName1,
                                              keyName2=setName2, unionResult=unionResult,
                                              password=password)
                result = unionResult[1]
            else:
                code, result = responseCode.SET_EXPIRED, "{} or {}".format(setName1, setName2)
        else:
            code, result = responseCode.SET_NOT_EXIST, "{} or {}".format(setName1, setName2)
        msg = Utils.makeMessage(responseCode.detail[code],
                               code,
                               result)
        return msg

    # set intersect operation
    def intersectSet(self, dbName, setName1, setName2, password=None):
        if Utils.isValidType(dbName, setName1, setName2) is False:
            return Utils.makeMessage(responseCode.detail[responseCode.ELEM_TYPE_ERROR],
                                   responseCode.ELEM_TYPE_ERROR,
                                   dbName)

        if self.database.isExist("SET", dbName, setName1, setName2):
            if self.database.isExpired("SET", dbName, setName1, setName2) is False:
                intersectResult = [None]
                code = self.database.intersectSet(dbName=dbName, setName1=setName1,
                                                  setName2=setName2, intersectResult=intersectResult,
                                                  password=password)
                result = intersectResult[1]
            else:
                code, result = responseCode.SET_EXPIRED, "{} or {}".format(setName1, setName2)
        else:
            code, result = responseCode.SET_NOT_EXIST, "{} or {}".format(setName1, setName2)
        msg = Utils.makeMessage(responseCode.detail[code],
                               code,
                               result)
        return msg

    # set difference operation
    def diffSet(self, dbName, setName1, setName2, password=None):
        if Utils.isValidType(dbName, setName1, setName2) is False:
            return Utils.makeMessage(responseCode.detail[responseCode.ELEM_TYPE_ERROR],
                                   responseCode.ELEM_TYPE_ERROR,
                                   dbName)

        if self.database.isExist("SET", dbName, setName1, setName2):
            if self.database.isExpired("SET", dbName, setName1, setName2) is False:
                diffResult = [None]
                code = self.database.diffSet(dbName=dbName, setName1=setName1,
                                             setName2=setName2, diffResult=diffResult,
                                             password=password)
                result = diffResult[1]
            else:
                code, result = responseCode.SET_EXPIRED, "{} or {}".format(setName1, setName2)
        else:
            code, result = responseCode.SET_NOT_EXIST, "{} or {}".format(setName1, setName2)
        msg = Utils.makeMessage(responseCode.detail[code],
                               code,
                               result)
        return msg

    # replace the existed set with a new set
    @validTypeCheck
    def replaceSet(self, dbName, keyName, value, password=None):
        if Utils.isSet(value) is False:
            return Utils.makeMessage(responseCode.detail[responseCode.ELEM_TYPE_ERROR],
                                     responseCode.ELEM_TYPE_ERROR,
                                     keyName)

        if self.database.isExist("SET", dbName, keyName):
            if self.database.isExpired("SET", dbName, keyName) is False:
                code = self.database.replaceSet(dbName=dbName, keyName=keyName,
                                                value=value, password=password)
            else:
                code = responseCode.SET_EXPIRED
        else:
            code = responseCode.SET_NOT_EXIST
        msg = Utils.makeMessage(responseCode.detail[code],
                                code,
                                keyName)
        return msg

    @validTypeCheck
    def getSize(self, dbName, keyName, password=None):
        if self.database.isExist("SET", dbName, keyName) is False:
            code, result = responseCode.SET_NOT_EXIST, keyName
        else:
            code, result = self.database.getSize(dbName=dbName, keyName=keyName,
                                                 type="SET", password=password)
        msg = Utils.makeMessage(responseCode.detail[code],
                                code,
                                result)
        return msg
