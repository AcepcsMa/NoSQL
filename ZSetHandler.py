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
            code = self.database.createZSet(dbName, keyName)
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
    def findMin(self, dbName, keyName):
        if self.database.isExist("ZSET", dbName, keyName):
            if self.database.isExpired("ZSET", dbName, keyName) is False:
                result = self.database.findMinFromZSet(dbName, keyName)
                msg = Utils.makeMessage(responseCode.detail[responseCode.ZSET_FIND_MIN_SUCCESS],
                                        responseCode.ZSET_FIND_MIN_SUCCESS,
                                        list(result))
            else:
                msg = Utils.makeMessage(responseCode.detail[responseCode.ZSET_EXPIRED],
                                        responseCode.ZSET_EXPIRED,
                                        keyName)
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.ZSET_NOT_EXIST],
                                    responseCode.ZSET_NOT_EXIST,
                                    keyName)
        return msg

    @validTypeCheck
    def findMax(self, dbName, keyName):
        if self.database.isExist("ZSET", dbName, keyName):
            if self.database.isExpired("ZSET", dbName, keyName) is False:
                result = self.database.findMaxFromZSet(dbName, keyName)
                msg = Utils.makeMessage(responseCode.detail[responseCode.ZSET_FIND_MAX_SUCCESS],
                                        responseCode.ZSET_FIND_MAX_SUCCESS,
                                        list(result))
            else:
                msg = Utils.makeMessage(responseCode.detail[responseCode.ZSET_EXPIRED],
                                        responseCode.ZSET_EXPIRED,
                                        keyName)
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.ZSET_NOT_EXIST],
                                    responseCode.ZSET_NOT_EXIST,
                                    keyName)
        return msg

    @validTypeCheck
    def getScore(self, dbName, keyName, value):
        if(self.database.isExist("ZSET", dbName, keyName)):
            if (self.database.isExpired("ZSET", dbName, keyName) is False):
                result = self.database.getScoreFromZSet(dbName, keyName, value)
                msg = Utils.makeMessage("Get Score Success", responseCode.ZSET_GET_SCORE_SUCCESS, result)
            else:
                msg = Utils.makeMessage(responseCode.detail[responseCode.ZSET_EXPIRED],
                                        responseCode.ZSET_EXPIRED,
                                        keyName)
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.ZSET_NOT_EXIST],
                                    responseCode.ZSET_NOT_EXIST,
                                    keyName)
        return msg

    @validTypeCheck
    def getValuesByRange(self, dbName, keyName, start, end):
        if(start >= end):
            msg = Utils.makeMessage("Score Range Error", responseCode.ZSET_SCORE_RANGE_ERROR, "{}-{}".format(start,end))
            return msg

        if(self.database.isExist("ZSET", dbName, keyName)):
            if (self.database.isExpired("ZSET", dbName, keyName) is False):
                result = self.database.getValuesByRange(dbName, keyName, start, end)
                msg = Utils.makeMessage("Get Values Success",
                                        responseCode.ZSET_GET_VALUES_SUCCESS,
                                        result)
            else:
                msg = Utils.makeMessage(responseCode.detail[responseCode.ZSET_EXPIRED],
                                        responseCode.ZSET_EXPIRED,
                                        keyName)
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.ZSET_NOT_EXIST],
                                    responseCode.ZSET_NOT_EXIST,
                                    keyName)
        return msg

    @validTypeCheck
    def getSize(self, dbName, keyName):
        if(self.database.isExist("ZSET", dbName, keyName)):
            if(self.database.isExpired("ZSET", dbName, keyName) is False):
                code, result = self.database.getSize(dbName, keyName, "ZSET")
                msg = Utils.makeMessage(responseCode.detail[code], code, result)
            else:
                msg = Utils.makeMessage(responseCode.detail[responseCode.ZSET_EXPIRED],
                                        responseCode.ZSET_EXPIRED,
                                        keyName)
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.ZSET_NOT_EXIST],
                                    responseCode.ZSET_NOT_EXIST,
                                    keyName)
        return msg

    def getRank(self, dbName, keyName, value):
        if(self.database.isExist("ZSET", dbName, keyName)):
            if(self.database.isExpired("ZSET", dbName, keyName) is False):
                result = self.database.getRank(dbName, keyName, value)
                msg = Utils.makeMessage("Get ZSet Rank Success",
                                        responseCode.ZSET_GET_RANK_SUCCESS,
                                        result)
            else:
                msg = Utils.makeMessage(responseCode.detail[responseCode.ZSET_EXPIRED],
                                        responseCode.ZSET_EXPIRED,
                                        keyName)
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.ZSET_NOT_EXIST],
                                    responseCode.ZSET_NOT_EXIST,
                                    keyName)
        return msg

    @validTypeCheck
    def rmByScore(self, dbName, keyName, start, end):
        if(start >= end):
            msg = Utils.makeMessage("Score Range Error",
                                    responseCode.ZSET_SCORE_RANGE_ERROR,
                                    "{}-{}".format(start, end))
            return msg

        if(self.database.isExist("ZSET", dbName, keyName)):
            if(self.database.isExpired("ZSET", dbName, keyName) is False):
                result = self.database.rmByScore(dbName, keyName, start, end)
                code = result[0]
                removeCount = result[1]
                msg = Utils.makeMessage(responseCode.detail[code],
                                        code,
                                        removeCount)
            else:
                msg = Utils.makeMessage(responseCode.detail[responseCode.ZSET_EXPIRED],
                                        responseCode.ZSET_EXPIRED,
                                        keyName)
        else:
            msg = Utils.makeMessage(responseCode.detail[responseCode.ZSET_NOT_EXIST],
                                    responseCode.ZSET_NOT_EXIST,
                                    keyName)
        return msg

    @validTypeCheck
    def showTTL(self, dbName, keyName, password=None):
        if (self.database.isDbExist(dbName)):
            code, result = self.database.showTTL(dbName=dbName, keyName=keyName,
                                                 dataType="ZSET", password=password)
        else:
            code, result = responseCode.DB_NOT_EXIST, dbName
        msg = Utils.makeMessage(responseCode.detail[code],
                                code,
                                result)
        return msg
