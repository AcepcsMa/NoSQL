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
        if("int" in str(type(elem))):
            return True
        else:
            return False

    # make the response message
    def makeMessage(self, msg, typeCode, data):
        message = {
            "msg":msg,
            "typeCode":typeCode,
            "data":data
        }
        return message

    # create a hash
    def createHash(self, dbName, hashName, hashValue):
        if(self.isValidType(dbName) and self.isValidType(hashName)):
            result = self.database.createHash(dbName, hashName, hashValue)
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
