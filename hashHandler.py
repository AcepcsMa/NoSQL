__author__ = 'Ma Haoxiang'

# import
from response import responseCode

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
            if(result == responseCode.HASH_CREATE_SUCCESS):
                msg = self.makeMessage("Hash Create Success", responseCode.HASH_CREATE_SUCCESS, hashName)
            elif(result == responseCode.HASH_EXISTED):
                msg = self.makeMessage("Hash Already Exists", responseCode.HASH_EXISTED, hashName)
            elif(result == responseCode.KEY_NAME_INVALID):
                msg = self.makeMessage("Hash Name Is Invalid", responseCode.KEY_NAME_INVALID, hashName)
            else:
                msg = self.makeMessage("Database Error", responseCode.DB_ERROR, dbName)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, hashName)
        return msg

    # get hash value
    def getHash(self, dbName, hashName):
        if(self.isValidType(dbName) and self.isValidType(hashName)):
            if(self.database.isHashExist(dbName, hashName) is True):
                if(self.database.isExpired(dbName, hashName, "HASH") is False):
                    hashValue = self.database.getHash(dbName, hashName)
                    msg = self.makeMessage("Hash Get Success", responseCode.HASH_GET_SUCCESS, hashValue)
                else:
                    msg = self.makeMessage("Hash Is Expired", responseCode.HASH_EXPIRED, hashName)
            else:
                msg = self.makeMessage("Hash Does Not Exist", responseCode.HASH_NOT_EXISTED, hashName)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, hashName)
        return msg

    # insert a key-value data into the given hash
    def insertHash(self, dbName, hashName, keyName, value):
        if(self.isValidType(dbName) and self.isValidType(hashName)):
            if(self.database.isHashExist(dbName, hashName) is True):
                if(self.database.isExpired(dbName, hashName, "HASH") is False):
                    result = self.database.insertHash(dbName, hashName, keyName, value)
                    if(result == responseCode.HASH_IS_LOCKED):
                        msg = self.makeMessage("Hash Is Locked", responseCode.HASH_IS_LOCKED, hashName)
                    elif(result == responseCode.HASH_INSERT_SUCCESS):
                        msg = self.makeMessage("Hash Insert Success", responseCode.HASH_INSERT_SUCCESS, hashName)
                    else:
                        msg = self.makeMessage("Database Error", responseCode.DB_ERROR, dbName)
                else:
                    msg = self.makeMessage("Hash Is Expired", responseCode.HASH_EXPIRED, hashName)
            else:
                msg = self.makeMessage("Hash Does Not Exist", responseCode.HASH_NOT_EXISTED, hashName)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, hashName)
        return msg

    # check if a key exists in the given hash
    def isKeyExist(self, dbName, hashName, keyName):
        if(self.isValidType(dbName) and self.isValidType(hashName)):
            if(self.database.isHashExist(dbName, hashName)):
                if(self.database.isExpired(dbName, hashName, "HASH") is False):
                    result = self.database.isKeyExist(dbName, hashName, keyName)
                    if(result == True):
                        msg = self.makeMessage("Hash Key Exists", responseCode.HASH_KEY_EXIST, keyName)
                    elif(result == False):
                        msg = self.makeMessage("Hash Key Does Not Exist", responseCode.HASH_KEY_NOT_EXIST, keyName)
                    else:
                        msg = self.makeMessage("Database Error", responseCode.DB_ERROR, dbName)
                else:
                    msg = self.makeMessage("Hash Is Expired", responseCode.HASH_EXPIRED, hashName)
            else:
                msg = self.makeMessage("Hash Does Not Exist", responseCode.HASH_NOT_EXISTED, hashName)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, hashName)
        return msg

    # delete the given hash
    def deleteHash(self, dbName, hashName):
        if(self.isValidType(dbName) and self.isValidType(hashName)):
            if(self.database.isHashExist(dbName, hashName)):
                result = self.database.deleteHash(dbName, hashName)
                if(result == responseCode.HASH_IS_LOCKED):
                    msg = self.makeMessage("Hash Is Locked", responseCode.HASH_IS_LOCKED, hashName)
                elif(result == responseCode.HASH_DELETE_SUCCESS):
                    msg = self.makeMessage("Hash Delete Success", responseCode.HASH_DELETE_SUCCESS, hashName)
                else:
                    msg = self.makeMessage("Database Error", responseCode.DB_ERROR, dbName)
            else:
                msg = self.makeMessage("Hash Does Not Exist", responseCode.HASH_NOT_EXISTED, hashName)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, hashName)
        return msg

    # remove a key-value data from the given hash
    def rmFromHash(self, dbName, hashName, keyName):
        if(self.isValidType(dbName) and self.isValidType(hashName)):
            if(self.database.isKeyExist(dbName, hashName, keyName) is True):
                if(self.database.isExpired(dbName, hashName, "HASH") is False):
                    result = self.database.rmFromHash(dbName, hashName, keyName)
                    if(result == responseCode.HASH_IS_LOCKED):
                        msg = self.makeMessage("Hash Is Locked", responseCode.HASH_IS_LOCKED, hashName)
                    elif(result == responseCode.HASH_REMOVE_SUCCESS):
                        msg = self.makeMessage("Hash Key Remove Success", responseCode.HASH_REMOVE_SUCCESS, keyName)
                    else:
                        msg = self.makeMessage("Database Error", responseCode.DB_ERROR, dbName)
                else:
                    msg = self.makeMessage("Hash Is Expired", responseCode.HASH_EXPIRED, hashName)
            else:
                msg = self.makeMessage("Hash Key Does Not Exist", responseCode.HASH_KEY_NOT_EXIST, keyName)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, hashName)
        return msg

    # clear the entire hash
    def clearHash(self, dbName, hashName):
        if(self.isValidType(dbName) and self.isValidType(hashName)):
            if(self.database.isHashExist(dbName, hashName) is True):
                if(self.database.isExpired(dbName, hashName, "HASH") is False):
                    result = self.database.clearHash(dbName, hashName)
                    if(result == responseCode.HASH_IS_LOCKED):
                        msg = self.makeMessage("Hash Is Locked", responseCode.HASH_IS_LOCKED, hashName)
                    elif(result == responseCode.HASH_CLEAR_SUCCESS):
                        msg = self.makeMessage("Hash Clear Success", responseCode.HASH_CLEAR_SUCCESS, hashName)
                    else:
                        msg = self.makeMessage("Database Error", responseCode.DB_ERROR, dbName)
                else:
                    msg = self.makeMessage("Hash Is Expired", responseCode.HASH_EXPIRED, hashName)
            else:
                msg = self.makeMessage("Hash Does Not Exist", responseCode.HASH_NOT_EXISTED, hashName)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, hashName)
        return msg

    # replace the existed hash with a new value
    def replaceHash(self, dbName, hashName, hashValue):
        if(self.isValidType(dbName) and self.isValidType(hashName)
           and self.isDict(hashValue)):
            if(self.database.isHashExist(dbName, hashName) is True):
                if(self.database.isExpired(dbName, hashName, "HASH") is False):
                    result = self.database.replaceHash(dbName, hashName, hashValue)
                    if(result == responseCode.HASH_IS_LOCKED):
                        msg = self.makeMessage("Hash Is Locked", responseCode.HASH_IS_LOCKED, hashName)
                    elif(result == responseCode.HASH_REPLACE_SUCCESS):
                        msg = self.makeMessage("Hash Replace Success", responseCode.HASH_REPLACE_SUCCESS, hashName)
                    else:
                        msg = self.makeMessage("Database Error", responseCode.DB_ERROR, dbName)
                else:
                    msg = self.makeMessage("Hash Is Expired", responseCode.HASH_EXPIRED, hashName)
            else:
                msg = self.makeMessage("Hash Does Not Exist", responseCode.HASH_NOT_EXISTED, hashName)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, hashName)
        return msg

    # merge two hashs
    def mergeHashs(self, dbName, hashName1, hashName2, resultHashName=None, mergeMode=0):
        if (resultHashName is not None):
            if (self.database.isHashExist(dbName, resultHashName) is True):
                msg = self.makeMessage("Merge Result Exists", responseCode.MERGE_RESULT_EXIST, resultHashName)
                return msg

        if (self.database.isHashExist(dbName, hashName1)
            and self.database.isHashExist(dbName, hashName2)):
            if(self.database.isHashExpired(dbName, hashName1) is False
               and self.database.isHashExpired(dbName, hashName2) is False):
                result = self.database.mergeHashs(dbName, hashName1, hashName2, resultHashName, mergeMode)
                if (result == responseCode.HASH_IS_LOCKED):
                    msg = self.makeMessage("Hash Is Locked", responseCode.HASH_IS_LOCKED, resultHashName)
                elif (result == responseCode.HASH_MERGE_SUCCESS):
                    msg = self.makeMessage("Hash Merge Success", responseCode.HASH_MERGE_SUCCESS, resultHashName)
                else:
                    msg = self.makeMessage("Database Error", responseCode.DB_ERROR, dbName)
            else:
                msg = self.makeMessage("Hash Is Expired", responseCode.HASH_EXPIRED, "{} or {}".format(hashName1, hashName2))
        else:
            msg = self.makeMessage("Hash Does Not Exist", responseCode.HASH_NOT_EXISTED,
                                   "{} or {}".format(hashName1, hashName2))
        return msg

    # search hash names using regular expression
    def searchHash(self, dbName, expression):
        if(self.isValidType(dbName)):
            searchResult = self.database.searchByRE(dbName, expression, "HASH")
            msg = self.makeMessage("Search Hash Success", responseCode.HASH_SEARCH_SUCCESS, searchResult)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, dbName)
        return msg

    # return all hash names in the given database
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

    # set TTL for a list
    def setTTL(self, dbName, hashName, ttl):
        if (self.isValidType(dbName) and self.isValidType(hashName)):
            if (self.database.isHashExist(dbName, hashName) is False):
                msg = self.makeMessage("Hash Does Not Exist", responseCode.HASH_NOT_EXISTED, hashName)
            else:
                result = self.database.setHashTTL(dbName, hashName, ttl)
                if (result == responseCode.HASH_IS_LOCKED):
                    msg = self.makeMessage("Hash Is Locked", responseCode.HASH_IS_LOCKED, hashName)
                elif (result == responseCode.HASH_TTL_SET_SUCCESS):
                    msg = self.makeMessage("Hash TTL Set Success", responseCode.HASH_TTL_SET_SUCCESS, hashName)
                else:
                    msg = self.makeMessage("Database Error", responseCode.DB_ERROR, dbName)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, hashName)
        return msg

    # clear TTL for a list
    def clearTTL(self, dbName, hashName):
        if (self.isValidType(dbName) and self.isValidType(hashName)):
            if (self.database.isHashExist(dbName, hashName) is False):
                msg = self.makeMessage("Hash Does Not Exist", responseCode.HASH_NOT_EXISTED, hashName)
            else:
                result = self.database.clearHashTTL(dbName, hashName)
                if (result == responseCode.HASH_IS_LOCKED):
                    msg = self.makeMessage("Hash Is Locked", responseCode.HASH_IS_LOCKED, hashName)
                elif (result == responseCode.HASH_TTL_CLEAR_SUCCESS):
                    msg = self.makeMessage("Hash TTL Clear Success", responseCode.HASH_TTL_CLEAR_SUCCESS,
                                           hashName)
                else:
                    msg = self.makeMessage("Database Error", responseCode.DB_ERROR, dbName)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, hashName)
        return msg

    def showTTL(self, dbName, keyName):
        if(self.isValidType(dbName) and self.isValidType(keyName)):
            if(self.database.isDbExist(dbName)):
                result = self.database.showTTL(dbName, keyName, "HASH")
                if(result == responseCode.TTL_NO_RECORD):
                    msg = self.makeMessage("TTL No Record", responseCode.TTL_NO_RECORD, keyName)
                elif(result == responseCode.TTL_EXPIRED):
                    msg = self.makeMessage("Hash TTL Expired", responseCode.HASH_EXPIRED, keyName)
                else:
                    msg = self.makeMessage("TTL Show Success", responseCode.TTL_SHOW_SUCCESS, result)
            else:
                msg = self.makeMessage("Database Does Not Exist", responseCode.DB_NOT_EXIST, dbName)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, dbName)
        return msg
