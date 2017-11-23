__author__ = 'Ma Haoxiang'

# import
from Decorator import *


class ZSetHandler(object):

    def __init__(self, database):
        self.database = database

    # check if the type of elem is valid (string or int)
    def isValidType(self, *elems):
        for elem in elems:
            if isinstance(elem,str) is False and isinstance(elem,int) is False:
                return False
        return True

    # make the response message
    def makeMessage(self, msg, typeCode, data):
        message = {
            "msg":msg,
            "typeCode":typeCode,
            "data":data
        }
        return message

    @validTypeCheck
    def createZSet(self, dbName, zsetName):
        if self.database.isDbExist(dbName) is False:
            msg = self.makeMessage(responseCode.detail[responseCode.DB_NOT_EXIST],
                                   responseCode.DB_NOT_EXIST,
                                   dbName)
            return msg

        if self.database.isExist("ZSET", dbName, zsetName) is False:
            result = self.database.createZSet(dbName, zsetName)
            msg = self.makeMessage(responseCode.detail[result], result, zsetName)
        else:
            msg = self.makeMessage(responseCode.detail[responseCode.ZSET_ALREADY_EXIST],
                                   responseCode.ZSET_ALREADY_EXIST,
                                   zsetName)
        return msg

    @validTypeCheck
    def getZSet(self, dbName, zsetName):
        if self.database.isExist("ZSET", dbName, zsetName) is True:
            if self.database.isExpired("ZSET", dbName, zsetName) is False:
                zsetValue = self.database.getZSet(dbName, zsetName)
                msg = self.makeMessage(responseCode.detail[responseCode.ZSET_GET_SUCCESS],
                                       responseCode.ZSET_GET_SUCCESS,
                                       zsetValue)
            else:
                msg = self.makeMessage(responseCode.detail[responseCode.ZSET_EXPIRED],
                                       responseCode.ZSET_EXPIRED,
                                       zsetName)
        else:
            msg = self.makeMessage(responseCode.detail[responseCode.ZSET_NOT_EXIST],
                                   responseCode.ZSET_NOT_EXIST,
                                   zsetName)
        return msg

    @validTypeCheck
    def insertZSet(self, dbName, zsetName, value, score):
        # score must be int type
        if isinstance(score, int) is False:
            msg = self.makeMessage(responseCode.detail[responseCode.ELEM_TYPE_ERROR],
                                   responseCode.ELEM_TYPE_ERROR,
                                   score)
            return msg

        if self.database.isExist("ZSET", dbName, zsetName):
            if self.database.isExpired("ZSET", dbName, zsetName) is False:
                result = self.database.insertZSet(dbName, zsetName, value, score)
                msg = self.makeMessage(responseCode.detail[result],
                                       result,
                                       zsetName)
            else:
                msg = self.makeMessage(responseCode.detail[responseCode.ZSET_EXPIRED],
                                       responseCode.ZSET_EXPIRED,
                                       zsetName)
        else:
            msg = self.makeMessage(responseCode.detail[responseCode.ZSET_NOT_EXIST],
                                   responseCode.ZSET_NOT_EXIST,
                                   zsetName)
        return msg

    @validTypeCheck
    def rmFromZSet(self, dbName, zsetName, value):
        if self.database.isExist("ZSET", dbName, zsetName):
            if self.database.isExpired("ZSET", dbName, zsetName) is False:
                result = self.database.rmFromZSet(dbName, zsetName, value)
                msg = self.makeMessage(responseCode.detail[result],
                                       result,
                                       zsetName)
            else:
                msg = self.makeMessage(responseCode.detail[responseCode.ZSET_EXPIRED],
                                       responseCode.ZSET_EXPIRED,
                                       zsetName)
        else:
            msg = self.makeMessage(responseCode.detail[responseCode.ZSET_NOT_EXIST],
                                   responseCode.ZSET_NOT_EXIST,
                                   zsetName)
        return msg

    @validTypeCheck
    def clearZSet(self, dbName, zsetName):
        if self.database.isExist("ZSET", dbName, zsetName):
            if self.database.isExpired("ZSET", dbName, zsetName) is False:
                result = self.database.clearZSet(dbName, zsetName)
                msg = self.makeMessage(responseCode.detail[result],
                                       result,
                                       zsetName)
            else:
                msg = self.makeMessage(responseCode.detail[responseCode.ZSET_EXPIRED],
                                       responseCode.ZSET_EXPIRED,
                                       zsetName)
        else:
            msg = self.makeMessage(responseCode.detail[responseCode.ZSET_NOT_EXIST],
                                   responseCode.ZSET_NOT_EXIST,
                                   zsetName)
        return msg

    @validTypeCheck
    def deleteZSet(self, dbName, zsetName):
        if self.database.isExist("ZSET", dbName, zsetName):
            if self.database.isExpired("ZSET", dbName, zsetName) is False:
                result = self.database.deleteZSet(dbName, zsetName)
                msg = self.makeMessage(responseCode.detail[result],
                                       result,
                                       zsetName)
            else:
                msg = self.makeMessage(responseCode.detail[responseCode.ZSET_EXPIRED],
                                       responseCode.ZSET_EXPIRED,
                                       zsetName)
        else:
            msg = self.makeMessage(responseCode.detail[responseCode.ZSET_NOT_EXIST],
                                   responseCode.ZSET_NOT_EXIST,
                                   zsetName)
        return msg

    def searchZSet(self, dbName, expression):
        if self.database.isDbExist(dbName) is False:
            return self.makeMessage(responseCode.detail[responseCode.DB_NOT_EXIST],
                                   responseCode.DB_NOT_EXIST,
                                   dbName)

        if self.isValidType(dbName):
            searchResult = self.database.searchByRE(dbName, expression, "ZSET")
            msg = self.makeMessage(responseCode.detail[responseCode.ZSET_SEARCH_SUCCESS], 
                                   responseCode.ZSET_SEARCH_SUCCESS, 
                                   searchResult)
        else:
            msg = self.makeMessage(responseCode.detail[responseCode.ELEM_TYPE_ERROR],
                                   responseCode.ELEM_TYPE_ERROR,
                                   dbName)
        return msg

    def searchAllZSet(self, dbName):
        if self.isValidType(dbName):
            if self.database.isDbExist(dbName):
                searchResult = self.database.searchAllZSet(dbName)
                msg = self.makeMessage(responseCode.detail[responseCode.ZSET_SEARCH_SUCCESS],
                                       responseCode.ZSET_SEARCH_SUCCESS,
                                       searchResult)
            else:
                msg = self.makeMessage(responseCode.detail[responseCode.DB_NOT_EXIST],
                                       responseCode.DB_NOT_EXIST,
                                       dbName)
        else:
            msg = self.makeMessage(responseCode.detail[responseCode.ELEM_TYPE_ERROR],
                                   responseCode.ELEM_TYPE_ERROR,
                                   dbName)
        return msg

    @validTypeCheck
    def findMin(self, dbName, zsetName):
        if self.database.isExist("ZSET", dbName, zsetName):
            if self.database.isExpired("ZSET", dbName, zsetName) is False:
                result = self.database.findMinFromZSet(dbName, zsetName)
                msg = self.makeMessage(responseCode.detail[responseCode.ZSET_FIND_MIN_SUCCESS],
                                       responseCode.ZSET_FIND_MIN_SUCCESS,
                                       list(result))
            else:
                msg = self.makeMessage(responseCode.detail[responseCode.ZSET_EXPIRED],
                                       responseCode.ZSET_EXPIRED,
                                       zsetName)
        else:
            msg = self.makeMessage(responseCode.detail[responseCode.ZSET_NOT_EXIST],
                                   responseCode.ZSET_NOT_EXIST,
                                   zsetName)
        return msg

    @validTypeCheck
    def findMax(self, dbName, zsetName):
        if self.database.isExist("ZSET", dbName, zsetName):
            if self.database.isExpired("ZSET", dbName, zsetName) is False:
                result = self.database.findMaxFromZSet(dbName, zsetName)
                msg = self.makeMessage(responseCode.detail[responseCode.ZSET_FIND_MAX_SUCCESS],
                                       responseCode.ZSET_FIND_MAX_SUCCESS,
                                       list(result))
            else:
                msg = self.makeMessage(responseCode.detail[responseCode.ZSET_EXPIRED],
                                       responseCode.ZSET_EXPIRED,
                                       zsetName)
        else:
            msg = self.makeMessage(responseCode.detail[responseCode.ZSET_NOT_EXIST],
                                   responseCode.ZSET_NOT_EXIST,
                                   zsetName)
        return msg

    @validTypeCheck
    def getScore(self, dbName, zsetName, valueName):
        if(self.database.isExist("ZSET", dbName, zsetName)):
            if (self.database.isExpired("ZSET", dbName, zsetName) is False):
                result = self.database.getScoreFromZSet(dbName, zsetName, valueName)
                msg = self.makeMessage("Get Score Success", responseCode.ZSET_GET_SCORE_SUCCESS, result)
            else:
                msg = self.makeMessage(responseCode.detail[responseCode.ZSET_EXPIRED], responseCode.ZSET_EXPIRED, zsetName)
        else:
            msg = self.makeMessage(responseCode.detail[responseCode.ZSET_NOT_EXIST], responseCode.ZSET_NOT_EXIST, zsetName)
        return msg

    @validTypeCheck
    def getValuesByRange(self, dbName, zsetName, start, end):
        if(start >= end):
            msg = self.makeMessage("Score Range Error", responseCode.ZSET_SCORE_RANGE_ERROR, "{}-{}".format(start,end))
            return msg

        if(self.database.isExist("ZSET", dbName, zsetName)):
            if (self.database.isExpired("ZSET", dbName, zsetName) is False):
                result = self.database.getValuesByRange(dbName, zsetName, start, end)
                msg = self.makeMessage("Get Values Success", responseCode.ZSET_GET_VALUES_SUCCESS, result)
            else:
                msg = self.makeMessage(responseCode.detail[responseCode.ZSET_EXPIRED], responseCode.ZSET_EXPIRED, zsetName)
        else:
            msg = self.makeMessage(responseCode.detail[responseCode.ZSET_NOT_EXIST], responseCode.ZSET_NOT_EXIST, zsetName)
        return msg

    def getSize(self, dbName, zsetName):
        if(self.database.isExist("ZSET", dbName, zsetName)):
            if(self.database.isExpired("ZSET", dbName, zsetName) is False):
                code, result = self.database.getSize(dbName, zsetName, "ZSET")
                msg = self.makeMessage(responseCode.detail[code], code, result)
            else:
                msg = self.makeMessage(responseCode.detail[responseCode.ZSET_EXPIRED], responseCode.ZSET_EXPIRED, zsetName)
        else:
            msg = self.makeMessage(responseCode.detail[responseCode.ZSET_NOT_EXIST], responseCode.ZSET_NOT_EXIST, zsetName)
        return msg

    def getRank(self, dbName, zsetName, value):
        if(self.database.isExist("ZSET", dbName, zsetName)):
            if(self.database.isExpired("ZSET", dbName, zsetName) is False):
                result = self.database.getRank(dbName, zsetName, value)
                msg = self.makeMessage("Get ZSet Rank Success", responseCode.ZSET_GET_RANK_SUCCESS, result)
            else:
                msg = self.makeMessage(responseCode.detail[responseCode.ZSET_EXPIRED], responseCode.ZSET_EXPIRED, zsetName)
        else:
            msg = self.makeMessage(responseCode.detail[responseCode.ZSET_NOT_EXIST], responseCode.ZSET_NOT_EXIST, zsetName)
        return msg

    @validTypeCheck
    def rmByScore(self, dbName, zsetName, start, end):
        if(start >= end):
            msg = self.makeMessage("Score Range Error", responseCode.ZSET_SCORE_RANGE_ERROR, "{}-{}".format(start, end))
            return msg

        if(self.database.isExist("ZSET", dbName, zsetName)):
            if(self.database.isExpired("ZSET", dbName, zsetName) is False):
                result = self.database.rmByScore(dbName, zsetName, start, end)
                code = result[0]
                removeCount = result[1]
                msg = self.makeMessage(responseCode.detail[code], code, removeCount)
            else:
                msg = self.makeMessage(responseCode.detail[responseCode.ZSET_EXPIRED], responseCode.ZSET_EXPIRED, zsetName)
        else:
            msg = self.makeMessage(responseCode.detail[responseCode.ZSET_NOT_EXIST], responseCode.ZSET_NOT_EXIST, zsetName)
        return msg

    @validTypeCheck
    def setTTL(self, dbName, zsetName, ttl):
        if(self.database.isExist("ZSET", dbName, zsetName) is False):
            msg = self.makeMessage(responseCode.detail[responseCode.ZSET_NOT_EXIST], responseCode.ZSET_NOT_EXIST, zsetName)
        else:
            result = self.database.setZSetTTL(dbName, zsetName, ttl)
            msg = self.makeMessage(responseCode.detail[result], result, zsetName)
        return msg

    @validTypeCheck
    def clearTTL(self, dbName, zsetName):
        if (self.database.isExist("ZSET", dbName, zsetName) is False):
            msg = self.makeMessage(responseCode.detail[responseCode.ZSET_NOT_EXIST], responseCode.ZSET_NOT_EXIST, zsetName)
        else:
            result = self.database.clearZSetTTL(dbName, zsetName)
            msg = self.makeMessage(responseCode.detail[result], result, zsetName)
        return msg

    @validTypeCheck
    def showTTL(self, dbName, keyName):
        if (self.database.isDbExist(dbName)):
            code, result = self.database.showTTL(dbName, keyName, "ZSET")
            msg = self.makeMessage(responseCode.detail[code], code, result)
        else:
            msg = self.makeMessage(responseCode.detail[responseCode.DB_NOT_EXIST], responseCode.DB_NOT_EXIST, dbName)
        return msg
