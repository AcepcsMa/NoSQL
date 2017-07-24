__author__ = 'Marco'

# import
from response import responseCode
from db import NoSqlDb

class hashHandler:
    def __init__(self, database):
        self.database = database

    # check if the type of elem is valid (string or int)
    def isValidType(self, elem):
        if('str' in str(type(elem)) or 'int' in str(type(elem))):
            return True
        else:
            return False

    # check if the type of an elem is INT
    def isInt(self, elem):
        return "int" in str(type(elem))

    # check if the type of an elem is DICT
    def isDict(self, elem):
        return "dict" in str(type(elem))

    # make the response message
    def makeMessage(self, msg, typeCode, data):
        message = {
            "msg":msg,
            "typeCode":typeCode,
            "data":data
        }
        return message

    # create a hash
    def createHash(self, dbName, hashName):
        if(self.isValidType(dbName) and self.isValidType(hashName)):
            result = self.database.createHash(dbName, hashName)
            if(result == NoSqlDb.HASH_CREATE_SUCCESS):
                msg = self.makeMessage("Hash Create Success", responseCode.HASH_CREATE_SUCCESS, hashName)
            elif(result == NoSqlDb.HASH_EXISTED):
                msg = self.makeMessage("Hash Already Exists", responseCode.HASH_EXISTED, hashName)
            else:
                msg = self.makeMessage("Database Error", responseCode.DB_ERROR, dbName)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, hashName)
        return msg

    # get hash value
    def getHash(self, dbName, hashName):
        if(self.isValidType(dbName) and self.isValidType(hashName)):
            if(self.database.isHashExist(dbName, hashName) is True):
                hashValue = self.database.getHash(dbName, hashName)
                msg = self.makeMessage("Hash Get Success", responseCode.HASH_GET_SUCCESS, hashValue)
            else:
                msg = self.makeMessage("Hash Does Not Exist", responseCode.HASH_NOT_EXISTED, hashName)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, hashName)
        return msg

    def insertHash(self, dbName, hashName, keyName, value):
        if(self.isValidType(dbName) and self.isValidType(hashName)):
            if(self.database.isHashExist(dbName, hashName)):
                result = self.database.insertHash(dbName, hashName, keyName, value)
                if(result == NoSqlDb.HASH_LOCKED):
                    msg = self.makeMessage("Hash Is Locked", responseCode.HASH_IS_LOCKED, hashName)
                elif(result == NoSqlDb.HASH_INSERT_SUCCESS):
                    msg = self.makeMessage("Hash Insert Success", responseCode.HASH_INSERT_SUCCESS, hashName)
                else:
                    msg = self.makeMessage("Database Error", responseCode.DB_ERROR, dbName)
            else:
                msg = self.makeMessage("Hash Does Not Exist", responseCode.HASH_NOT_EXISTED, hashName)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, hashName)
        return msg

    def isKeyExist(self, dbName, hashName, keyName):
        if(self.isValidType(dbName) and self.isValidType(hashName)):
            if(self.database.isHashExist(dbName, hashName)):
                result = self.database.isKeyExist(dbName, hashName, keyName)
                if(result == True):
                    msg = self.makeMessage("Hash Key Exists", responseCode.HASH_KEY_EXIST, keyName)
                elif(result == False):
                    msg = self.makeMessage("Hash Key Does Not Exist", responseCode.HASH_KEY_NOT_EXIST, keyName)
                else:
                    msg = self.makeMessage("Database Error", responseCode.DB_ERROR, dbName)
            else:
                msg = self.makeMessage("Hash Does Not Exist", responseCode.HASH_NOT_EXISTED, hashName)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, hashName)
        return msg

    def deleteHash(self, dbName, hashName):
        if(self.isValidType(dbName) and self.isValidType(hashName)):
            if(self.database.isHashExist(dbName, hashName)):
                result = self.database.deleteHash(dbName, hashName)
                if(result == NoSqlDb.HASH_LOCKED):
                    msg = self.makeMessage("Hash Is Locked", responseCode.HASH_IS_LOCKED, hashName)
                elif(result == NoSqlDb.HASH_DELETE_SUCCESS):
                    msg = self.makeMessage("Hash Delete Success", responseCode.HASH_DELETE_SUCCESS, hashName)
                else:
                    msg = self.makeMessage("Database Error", responseCode.DB_ERROR, dbName)
            else:
                msg = self.makeMessage("Hash Does Not Exist", responseCode.HASH_NOT_EXISTED, hashName)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, hashName)
        return msg

    def rmFromHash(self, dbName, hashName, keyName):
        if(self.isValidType(dbName) and self.isValidType(hashName)):
            if(self.database.isKeyExist(dbName, hashName, keyName) is True):
                result = self.database.rmFromHash(dbName, hashName, keyName)
                if(result == NoSqlDb.HASH_LOCKED):
                    msg = self.makeMessage("Hash Is Locked", responseCode.HASH_IS_LOCKED, hashName)
                elif(result == NoSqlDb.HASH_REMOVE_SUCCESS):
                    msg = self.makeMessage("Hash Key Remove Success", responseCode.HASH_REMOVE_SUCCESS, keyName)
                else:
                    msg = self.makeMessage("Database Error", responseCode.DB_ERROR, dbName)
            else:
                msg = self.makeMessage("Hash Key Does Not Exist", responseCode.HASH_KEY_NOT_EXIST, keyName)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, hashName)
        return msg

    def clearHash(self, dbName, hashName):
        if(self.isValidType(dbName) and self.isValidType(hashName)):
            if(self.database.isHashExist(dbName, hashName) is True):
                result = self.database.clearHash(dbName, hashName)
                if(result == NoSqlDb.HASH_LOCKED):
                    msg = self.makeMessage("Hash Is Locked", responseCode.HASH_IS_LOCKED, hashName)
                elif(result == NoSqlDb.HASH_CLEAR_SUCCESS):
                    msg = self.makeMessage("Hash Clear Success", responseCode.HASH_CLEAR_SUCCESS, hashName)
                else:
                    msg = self.makeMessage("Database Error", responseCode.DB_ERROR, dbName)
            else:
                msg = self.makeMessage("Hash Does Not Exist", responseCode.HASH_NOT_EXISTED, hashName)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, hashName)
        return msg

    def replaceHash(self, dbName, hashName, hashValue):
        if(self.isValidType(dbName) and self.isValidType(hashName)
           and self.isDict(hashValue)):
            if(self.database.isHashExist(dbName, hashName) is True):
                result = self.database.replaceHash(dbName, hashName, hashValue)
                if(result == NoSqlDb.HASH_LOCKED):
                    msg = self.makeMessage("Hash Is Locked", responseCode.HASH_IS_LOCKED, hashName)
                elif(result == NoSqlDb.HASH_REPLACE_SUCCESS):
                    msg = self.makeMessage("Hash Replace Success", responseCode.HASH_REPLACE_SUCCESS, hashName)
                else:
                    msg = self.makeMessage("Database Error", responseCode.DB_ERROR, dbName)
            else:
                msg = self.makeMessage("Hash Does Not Exist", responseCode.HASH_NOT_EXISTED, hashName)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, hashName)
        return msg

    def mergeHashs(self, dbName, hashName1, hashName2, resultHashName=None, mergeMode=0):
        if (resultHashName is not None):
            if (self.database.isHashExist(dbName, resultHashName) is True):
                msg = self.makeMessage("Merge Result Exists", responseCode.MERGE_RESULT_EXIST, resultHashName)
                return msg

        if (self.database.isHashExist(dbName, hashName1)
            and self.database.isHashExist(dbName, hashName2)):
            result = self.database.mergeHashs(dbName, hashName1, hashName2, resultHashName, mergeMode)
            if (result == NoSqlDb.HASH_LOCKED):
                msg = self.makeMessage("Hash Is Locked", responseCode.HASH_IS_LOCKED, resultHashName)
            elif (result == NoSqlDb.HASH_MERGE_SUCCESS):
                msg = self.makeMessage("Hash Merge Success", responseCode.HASH_MERGE_SUCCESS, resultHashName)
            else:
                msg = self.makeMessage("Database Error", responseCode.DB_ERROR, dbName)
        else:
            msg = self.makeMessage("Hash Does Not Exist", responseCode.HASH_NOT_EXISTED,
                                   "{} or {}".format(hashName1, hashName2))
        return msg

    def searchHash(self, dbName, expression):
        if(self.isValidType(dbName)):
            searchResult = self.database.searchByRE(dbName, expression, "HASH")
            msg = self.makeMessage("Search Hash Success", responseCode.HASH_SEARCH_SUCCESS, searchResult)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, dbName)
        return msg

    def searchAllHash(self, dbName):
        if(self.isValidType(dbName)):
            if(self.database.isDbExist(dbName)):
                searchResult = self.database.searchAllHash(dbName)
                msg = self.makeMessage("Search Hash Success", responseCode.HASH_SEARCH_SUCCESS, searchResult)
            else:
                msg = self.makeMessage("Database Does Not Exist", responseCode.DB_NOT_EXIST, dbName)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, dbName)
        return msg
