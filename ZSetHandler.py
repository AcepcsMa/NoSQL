__author__ = 'Ma Haoxiang'

# import
from Decorator import *
from Utils import Utils

class ZSetHandler(object):

    def __init__(self, database):
        self.database = database

    @validTypeCheck
    def createZSet(self, dbName, keyName, password=None):
        if self.database.isDbExist(dbName) is False:
            return Utils.makeMessage(responseCode.detail[responseCode.DB_NOT_EXIST],
                                   responseCode.DB_NOT_EXIST,
                                   dbName)

        if self.database.isExist("ZSET", dbName, keyName) is False:
            code = self.database.createZSet(dbName=dbName,
                                            keyName=keyName,
                                            password=password)
        else:
            code = responseCode.ZSET_ALREADY_EXIST
        msg = Utils.makeMessage(responseCode.detail[code],
                                code,
                                keyName)
        return msg

    @validTypeCheck
    def getZSet(self, dbName, keyName, password=None):
        if self.database.isExist("ZSET", dbName, keyName) is True:
            if self.database.isExpired("ZSET", dbName, keyName) is False:
                code, result = responseCode.ZSET_GET_SUCCESS, \
                               self.database.getZSet(dbName=dbName,
                                                     keyName=keyName,
                                                     password=password)
            else:
                code, result = responseCode.ZSET_EXPIRED, keyName
        else:
            code, result = responseCode.ZSET_NOT_EXIST, keyName
        msg = Utils.makeMessage(responseCode.detail[code],
                                code,
                                result)
        return msg

    @validTypeCheck
    def insertZSet(self, dbName, keyName, value, score, password=None):
        # score must be int type
        if isinstance(score, int) is False:
            return Utils.makeMessage(responseCode.detail[responseCode.ELEM_TYPE_ERROR],
                                   responseCode.ELEM_TYPE_ERROR,
                                   score)

        if self.database.isExist("ZSET", dbName, keyName):
            if self.database.isExpired("ZSET", dbName, keyName) is False:
                code = self.database.insertZSet(dbName=dbName, keyName=keyName,
                                                value=value, score=score,
                                                password=password)
            else:
                code = responseCode.ZSET_EXPIRED
        else:
            code = responseCode.ZSET_NOT_EXIST
        msg = Utils.makeMessage(responseCode.detail[code],
                                code,
                                keyName)
        return msg

    @validTypeCheck
    def rmFromZSet(self, dbName, keyName, value, password=None):
        if self.database.isExist("ZSET", dbName, keyName):
            if self.database.isExpired("ZSET", dbName, keyName) is False:
                code = self.database.rmFromZSet(dbName=dbName, keyName=keyName,
                                                value=value, password=password)
            else:
                code = responseCode.ZSET_EXPIRED
        else:
            code = responseCode.ZSET_NOT_EXIST
        msg = Utils.makeMessage(responseCode.detail[code],
                                code,
                                keyName)
        return msg

    @validTypeCheck
    def clearZSet(self, dbName, keyName, password=None):
        if self.database.isExist("ZSET", dbName, keyName):
            if self.database.isExpired("ZSET", dbName, keyName) is False:
                code = self.database.clearZSet(dbName=dbName,
                                               keyName=keyName,
                                               password=password)
            else:
                code = responseCode.ZSET_EXPIRED
        else:
            code = responseCode.ZSET_NOT_EXIST
        msg = Utils.makeMessage(responseCode.detail[code],
                                code,
                                keyName)
        return msg

    @validTypeCheck
    def deleteZSet(self, dbName, keyName, password=None):
        if self.database.isExist("ZSET", dbName, keyName):
            if self.database.isExpired("ZSET", dbName, keyName) is False:
                code = self.database.deleteZSet(dbName=dbName,
                                                keyName=keyName,
                                                password=password)
            else:
                code = responseCode.ZSET_EXPIRED
        else:
            code = responseCode.ZSET_NOT_EXIST
        msg = Utils.makeMessage(responseCode.detail[code],
                                code,
                                keyName)
        return msg

    def searchZSet(self, dbName, expression, password=None):
        if self.database.isDbExist(dbName) is False:
            return Utils.makeMessage(responseCode.detail[responseCode.DB_NOT_EXIST],
                                   responseCode.DB_NOT_EXIST,
                                   dbName)

        if Utils.isValidType(dbName):
            searchResult = self.database.searchByRE(dbName=dbName, expression=expression,
                                                    dataType="ZSET", password=password)
            msg = Utils.makeMessage(responseCode.detail[responseCode.ZSET_SEARCH_SUCCESS], 
                                   responseCode.ZSET_SEARCH_SUCCESS, 
                                   searchResult)
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.ELEM_TYPE_ERROR],
                                   responseCode.ELEM_TYPE_ERROR,
                                   dbName)
        return msg

    def searchAllZSet(self, dbName, password=None):
        if Utils.isValidType(dbName):
            if self.database.isDbExist(dbName):
                code, result = responseCode.ZSET_SEARCH_SUCCESS, \
                               self.database.searchAllZSet(dbName=dbName,
                                                           password=password)
            else:
                code, result = responseCode.DB_NOT_EXIST, dbName
        else:
            code, result = responseCode.ELEM_TYPE_ERROR, dbName
        msg = Utils.makeMessage(responseCode.detail[code],
                                code,
                                result)
        return msg

    @validTypeCheck
    def findMin(self, dbName, keyName, password=None):
        if self.database.isExist("ZSET", dbName, keyName):
            if self.database.isExpired("ZSET", dbName, keyName) is False:
                code, result = responseCode.ZSET_FIND_MIN_SUCCESS, \
                               list(self.database.findMinFromZSet(dbName=dbName,
                                                                  keyName=keyName,
                                                                  password=password))
            else:
                code, result = responseCode.ZSET_EXPIRED, keyName
        else:
            code, result = responseCode.ZSET_NOT_EXIST, keyName
        msg = Utils.makeMessage(responseCode.detail[code],
                                code,
                                result)
        return msg

    @validTypeCheck
    def findMax(self, dbName, keyName, password=None):
        if self.database.isExist("ZSET", dbName, keyName):
            if self.database.isExpired("ZSET", dbName, keyName) is False:
                code, result = responseCode.ZSET_FIND_MAX_SUCCESS, \
                               list(self.database.findMaxFromZSet(dbName=dbName,
                                                                  keyName=keyName,
                                                                  password=password))
            else:
                code, result = responseCode.ZSET_EXPIRED, keyName
        else:
            code, result = responseCode.ZSET_NOT_EXIST, keyName
        msg = Utils.makeMessage(responseCode.detail[code],
                                code,
                                result)
        return msg

    @validTypeCheck
    def getScore(self, dbName, keyName, value, password=None):
        if self.database.isExist("ZSET", dbName, keyName):
            if self.database.isExpired("ZSET", dbName, keyName) is False:
                code, result = responseCode.ZSET_GET_SCORE_SUCCESS, \
                               self.database.getScoreFromZSet(dbName=dbName, keyName=keyName,
                                                              valueName=value, password=password)
            else:
                code, result = responseCode.ZSET_EXPIRED, keyName
        else:
            code, result = responseCode.ZSET_NOT_EXIST, keyName
        msg = Utils.makeMessage(responseCode.detail[code],
                                code,
                                keyName)
        return msg

    @validTypeCheck
    def getValuesByRange(self, dbName, keyName, start, end, password=None):
        if start >= end:
            return Utils.makeMessage("Score Range Error",
                                     responseCode.ZSET_SCORE_RANGE_ERROR,
                                     "{}-{}".format(start,end))

        if self.database.isExist("ZSET", dbName, keyName):
            if self.database.isExpired("ZSET", dbName, keyName) is False:
                code, result = responseCode.ZSET_GET_VALUES_SUCCESS, \
                               self.database.getValuesByRange(dbName=dbName, keyName=keyName,
                                                              start=start, end=end,
                                                              password=password)
            else:
                code, result = responseCode.ZSET_EXPIRED, keyName
        else:
            code, result = responseCode.ZSET_NOT_EXIST, keyName
        msg = Utils.makeMessage(responseCode.detail[code],
                                code,
                                keyName)
        return msg

    @validTypeCheck
    def getSize(self, dbName, keyName, password=None):
        if(self.database.isExist("ZSET", dbName, keyName)):
            if(self.database.isExpired("ZSET", dbName, keyName) is False):
                code, result = self.database.getSize(dbName=dbName, keyName=keyName,
                                                     type="ZSET", password=password)
            else:
                code, result = responseCode.ZSET_EXPIRED, keyName
        else:
            code, result = responseCode.ZSET_NOT_EXIST, keyName
        msg = Utils.makeMessage(responseCode.detail[code],
                                code,
                                result)
        return msg

    def getRank(self, dbName, keyName, value, password=None):
        if(self.database.isExist("ZSET", dbName, keyName)):
            if(self.database.isExpired("ZSET", dbName, keyName) is False):
                code = responseCode.ZSET_GET_RANK_SUCCESS
                result = self.database.getRank(dbName=dbName, keyName=keyName,
                                               value=value, password=password)
            else:
                code, result = responseCode.ZSET_EXPIRED, keyName
        else:
            code, result = responseCode.ZSET_NOT_EXIST, keyName
        msg = Utils.makeMessage(responseCode.detail[code],
                                code,
                                keyName)
        return msg

    @validTypeCheck
    def rmByScore(self, dbName, keyName, start, end, password=None):
        if(start >= end):
            return Utils.makeMessage("Score Range Error",
                                    responseCode.ZSET_SCORE_RANGE_ERROR,
                                    "{}-{}".format(start, end))

        if(self.database.isExist("ZSET", dbName, keyName)):
            if(self.database.isExpired("ZSET", dbName, keyName) is False):
                result = self.database.rmByScore(dbName=dbName, keyName=keyName,
                                                 start=start, end=end,
                                                 password=password)
                code, result = result[0], result[1]
            else:
                code, result = responseCode.ZSET_EXPIRED, keyName
        else:
            code, result = responseCode.ZSET_NOT_EXIST, keyName
        msg = Utils.makeMessage(responseCode.detail[code],
                                code,
                                result)
        return msg