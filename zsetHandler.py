__author__ = 'Ma Haoxiang'

# import
from decorator import *


class zsetHandler:
    def __init__(self, database):
        self.database = database

    # check if the type of elem is valid (string or int)
    def isValidType(self, *elems):
        for elem in elems:
            if('str' not in str(type(elem)) and 'int' not in str(type(elem))):
                return False
        return True

    # check if the type of an elem is SET
    def isSet(self, elem):
        return "set" in str(type(elem))

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
        if (self.database.isDbExist(dbName) is False):
            msg = self.makeMessage("Database Does Not Exist", responseCode.DB_NOT_EXIST, dbName)
            return msg

        if (self.database.isZSetExist(dbName, zsetName) is False):
            result = self.database.createZSet(dbName, zsetName)
            msg = self.makeMessage(responseCode.detail[result], result, zsetName)
        else:
            msg = self.makeMessage("ZSet Already Exists", responseCode.ZSET_ALREADY_EXIST, zsetName)
        return msg

    @validTypeCheck
    def getZSet(self, dbName, zsetName):
        if (self.database.isZSetExist(dbName, zsetName) is True):
            if (self.database.isExpired("ZSET", dbName, zsetName) is False):
                zsetValue = self.database.getZSet(dbName, zsetName)
                msg = self.makeMessage("ZSet Get Success", responseCode.ZSET_GET_SUCCESS, zsetValue)
            else:
                msg = self.makeMessage("ZSet Is Expired", responseCode.ZSET_EXPIRED, zsetName)
        else:
            msg = self.makeMessage("ZSet Does Not Exist", responseCode.ZSET_NOT_EXIST, zsetName)
        return msg

    @validTypeCheck
    def insertZSet(self, dbName, zsetName, value, score):

        # score must be int type
        if(isinstance(score,int) is False):
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, score)
            return msg

        if (self.database.isZSetExist(dbName, zsetName)):
            if (self.database.isExpired("ZSET", dbName, zsetName) is False):
                result = self.database.insertZSet(dbName, zsetName, value, score)
                msg = self.makeMessage(responseCode.detail[result], result, zsetName)
            else:
                msg = self.makeMessage("ZSet Is Expired", responseCode.ZSET_EXPIRED, zsetName)
        else:
            msg = self.makeMessage("ZSet Does Not Exist", responseCode.ZSET_NOT_EXIST, zsetName)
        return msg

    @validTypeCheck
    def rmFromZSet(self, dbName, zsetName, value):
        if (self.database.isZSetExist(dbName, zsetName)):
            if (self.database.isExpired("ZSET", dbName, zsetName) is False):
                result = self.database.rmFromZSet(dbName, zsetName, value)
                msg = self.makeMessage(responseCode.detail[result], result, zsetName)
            else:
                msg = self.makeMessage("ZSet Is Expired", responseCode.ZSET_EXPIRED, zsetName)
        else:
            msg = self.makeMessage("ZSet Does Not Exist", responseCode.ZSET_NOT_EXIST, zsetName)
        return msg

    @validTypeCheck
    def clearZSet(self, dbName, zsetName):
        if (self.database.isZSetExist(dbName, zsetName)):
            if (self.database.isExpired("ZSET", dbName, zsetName) is False):
                result = self.database.clearZSet(dbName, zsetName)
                msg = self.makeMessage(responseCode.detail[result], result, zsetName)
            else:
                msg = self.makeMessage("ZSet Is Expired", responseCode.ZSET_EXPIRED, zsetName)
        else:
            msg = self.makeMessage("ZSet Does Not Exist", responseCode.ZSET_NOT_EXIST, zsetName)
        return msg

    @validTypeCheck
    def deleteZSet(self, dbName, zsetName):
        if (self.database.isZSetExist(dbName, zsetName)):
            if (self.database.isExpired("ZSET", dbName, zsetName) is False):
                result = self.database.deleteZSet(dbName, zsetName)
                msg = self.makeMessage(responseCode.detail[result], result, zsetName)
            else:
                msg = self.makeMessage("ZSet Is Expired", responseCode.ZSET_EXPIRED, zsetName)
        else:
            msg = self.makeMessage("ZSet Does Not Exist", responseCode.ZSET_NOT_EXIST, zsetName)
        return msg

    def searchZSet(self, dbName, expression):
        if (self.database.isDbExist(dbName) is False):
            msg = self.makeMessage("Database Does Not Exist", responseCode.DB_NOT_EXIST, dbName)
            return msg

        if (self.isValidType(dbName)):
            searchResult = self.database.searchByRE(dbName, expression, "ZSET")
            msg = self.makeMessage("Search ZSet Success", responseCode.ZSET_SEARCH_SUCCESS, searchResult)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, dbName)
        return msg

    def searchAllZSet(self, dbName):
        if (self.isValidType(dbName)):
            if (self.database.isDbExist(dbName)):
                searchResult = self.database.searchAllZSet(dbName)
                msg = self.makeMessage("Search ZSet Success", responseCode.ZSET_SEARCH_SUCCESS, searchResult)
            else:
                msg = self.makeMessage("Database Does Not Exist", responseCode.DB_NOT_EXIST, dbName)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, dbName)
        return msg

    @validTypeCheck
    def findMin(self, dbName, zsetName):
        if (self.database.isZSetExist(dbName, zsetName)):
            if (self.database.isExpired("ZSET", dbName, zsetName) is False):
                result = self.database.findMinFromZSet(dbName, zsetName)
                msg = self.makeMessage("Find Min Element Success", responseCode.ZSET_FIND_MIN_SUCCESS, list(result))
            else:
                msg = self.makeMessage("ZSet Is Expired", responseCode.ZSET_EXPIRED, zsetName)
        else:
            msg = self.makeMessage("ZSet Does Not Exist", responseCode.ZSET_NOT_EXIST, zsetName)
        return msg

    @validTypeCheck
    def findMax(self, dbName, zsetName):
        if(self.database.isZSetExist(dbName, zsetName)):
            if (self.database.isExpired("ZSET", dbName, zsetName) is False):
                result = self.database.findMaxFromZSet(dbName, zsetName)
                msg = self.makeMessage("Find Max Element Success", responseCode.ZSET_FIND_MAX_SUCCESS, list(result))
            else:
                msg = self.makeMessage("ZSet Is Expired", responseCode.ZSET_EXPIRED, zsetName)
        else:
            msg = self.makeMessage("ZSet Does Not Exist", responseCode.ZSET_NOT_EXIST, zsetName)
        return msg

    @validTypeCheck
    def getScore(self, dbName, zsetName, valueName):
        if(self.database.isZSetExist(dbName, zsetName)):
            if (self.database.isExpired("ZSET", dbName, zsetName) is False):
                result = self.database.getScoreFromZSet(dbName, zsetName, valueName)
                msg = self.makeMessage("Get Score Success", responseCode.ZSET_GET_SCORE_SUCCESS, result)
            else:
                msg = self.makeMessage("ZSet Is Expired", responseCode.ZSET_EXPIRED, zsetName)
        else:
            msg = self.makeMessage("ZSet Does Not Exist", responseCode.ZSET_NOT_EXIST, zsetName)
        return msg

    @validTypeCheck
    def getValues(self, dbName, zsetName, start, end):
        if(start >= end):
            msg = self.makeMessage("Score Range Error", responseCode.ZSET_SCORE_RANGE_ERROR, "{}-{}".format(start,end))
            return msg

        if(self.database.isZSetExist(dbName, zsetName)):
            if (self.database.isExpired("ZSET", dbName, zsetName) is False):
                result = self.database.getValues(dbName, zsetName, start, end)
                msg = self.makeMessage("Get Values Success", responseCode.ZSET_GET_VALUES_SUCCESS, result)
            else:
                msg = self.makeMessage("ZSet Is Expired", responseCode.ZSET_EXPIRED, zsetName)
        else:
            msg = self.makeMessage("ZSet Does Not Exist", responseCode.ZSET_NOT_EXIST, zsetName)
        return msg