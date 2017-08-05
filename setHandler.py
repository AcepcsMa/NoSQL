__author__ = 'Marco'

# import
from response import responseCode
from db import NoSqlDb

class setHandler:
    def __init__(self, database):
        self.database = database

    # check if the type of elem is valid (string or int)
    def isValidType(self, elem):
        if('str' in str(type(elem)) or 'int' in str(type(elem))):
            return True
        else:
            return False

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

    # create a set
    def createSet(self, dbName, setName):
        if(self.isValidType(dbName) and self.isValidType(setName)):
            if(self.database.isSetExist(dbName, setName) is False):
                result = self.database.createSet(dbName, setName)
                if(result == NoSqlDb.SET_CREATE_SUCCESS):
                    msg = self.makeMessage("Set Create Success", responseCode.SET_CREATE_SUCCESS, setName)
                else:
                    msg = self.makeMessage("Database Error", responseCode.DB_ERROR, dbName)
            else:
                msg = self.makeMessage("Set Already Exists", responseCode.SET_ALREADY_EXIST, setName)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, setName)
        return msg

    # get set value
    def getSet(self, dbName, setName):
        if(self.isValidType(dbName) and self.isValidType(setName)):
            if(self.database.isSetExist(dbName, setName) is True):
                setValue = self.database.getSet(dbName, setName)
                msg = self.makeMessage("Set Get Success", responseCode.SET_GET_SUCCESS, setValue)
            else:
                msg = self.makeMessage("Set Does Not Exist", responseCode.SET_NOT_EXIST, setName)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, setName)
        return msg

    # insert a value into the given set
    def insertSet(self, dbName, setName, setValue):
        if(self.isValidType(dbName) and self.isValidType(setName)):
            if(self.database.isSetExist(dbName, setName)):
                result = self.database.insertSet(dbName, setName, setValue)
                if(result == NoSqlDb.SET_LOCKED):
                    msg = self.makeMessage("Set Is Locked", responseCode.SET_IS_LOCKED, setName)
                elif(result == NoSqlDb.SET_VALUE_ALREADY_EXIST):
                    msg = self.makeMessage("Set Value Already Exists", responseCode.SET_VALUE_ALREADY_EXIST, setValue)
                elif(result == NoSqlDb.SET_INSERT_SUCCESS):
                    msg = self.makeMessage("Set Insert Success", responseCode.SET_INSERT_SUCCESS, setName)
                else:
                    msg = self.makeMessage("Database Error", responseCode.DB_ERROR, dbName)
            else:
                msg = self.makeMessage("Set Does Not Exist", responseCode.SET_NOT_EXIST, setName)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, setName)
        return msg

    # remove the given value from a set
    def rmFromSet(self, dbName, setName, setValue):
        if(self.isValidType(dbName) and self.isValidType(setName)):
            if(self.database.isSetExist(dbName, setName)):
                result = self.database.rmFromSet(dbName, setName, setValue)
                if(result == NoSqlDb.SET_LOCKED):
                    msg = self.makeMessage("Set Is Locked", responseCode.SET_IS_LOCKED, setName)
                elif(result == NoSqlDb.SET_VALUE_NOT_EXISTED):
                    msg = self.makeMessage("Set Value Does Not Exist", responseCode.SET_VALUE_NOT_EXIST, setValue)
                elif(result == NoSqlDb.SET_REMOVE_SUCCESS):
                    msg = self.makeMessage("Set Remove Success", responseCode.SET_REMOVE_SUCCESS, setValue)
                else:
                    msg = self.makeMessage("Database Error", responseCode.DB_ERROR, dbName)
            else:
                msg = self.makeMessage("Set Does Not Exist", responseCode.SET_NOT_EXIST, setName)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, setName)
        return msg

    # clear the given set
    def clearSet(self, dbName, setName):
        if(self.isValidType(dbName) and self.isValidType(setName)):
            if(self.database.isSetExist(dbName, setName)):
                result = self.database.clearSet(dbName, setName)
                if(result == NoSqlDb.SET_LOCKED):
                    msg = self.makeMessage("Set Is Locked", responseCode.SET_IS_LOCKED, setName)
                elif(result == NoSqlDb.SET_CLEAR_SUCCESS):
                    msg = self.makeMessage("Set Clear Success", responseCode.SET_CLEAR_SUCCESS, setName)
                else:
                    msg = self.makeMessage("Database Error", responseCode.DB_ERROR, dbName)
            else:
                msg = self.makeMessage("Set Does Not Exist", responseCode.SET_NOT_EXIST, setName)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, setName)
        return msg

    def deleteSet(self, dbName, setName):
        if (self.isValidType(dbName) and self.isValidType(setName)):
            if (self.database.isSetExist(dbName, setName)):
                result = self.database.deleteSet(dbName, setName)
                if (result == NoSqlDb.SET_LOCKED):
                    msg = self.makeMessage("Set Is Locked", responseCode.SET_IS_LOCKED, setName)
                elif (result == NoSqlDb.SET_DELETE_SUCCESS):
                    msg = self.makeMessage("Set Delete Success", responseCode.SET_CLEAR_SUCCESS, setName)
                else:
                    msg = self.makeMessage("Database Error", responseCode.DB_ERROR, dbName)
            else:
                msg = self.makeMessage("Set Does Not Exist", responseCode.SET_NOT_EXIST, setName)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, setName)
        return msg

    # search set names using regular expression
    def searchSet(self, dbName, expression):
        if (self.isValidType(dbName)):
            searchResult = self.database.searchByRE(dbName, expression, "SET")
            msg = self.makeMessage("Search Set Success", responseCode.SET_SEARCH_SUCCESS, searchResult)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, dbName)
        return msg

    # return all set names in the given database
    def searchAllSet(self, dbName):
        if (self.isValidType(dbName)):
            if (self.database.isDbExist(dbName)):
                searchResult = self.database.searchAllSet(dbName)
                msg = self.makeMessage("Search Set Success", responseCode.SET_SEARCH_SUCCESS, searchResult)
            else:
                msg = self.makeMessage("Database Does Not Exist", responseCode.DB_NOT_EXIST, dbName)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, dbName)
        return msg

    def unionSet(self, dbName, setName1, setName2):
        if(self.isValidType(dbName)
           and self.isValidType(setName1)
           and self.isValidType(setName2)):
            if(self.database.isSetExist(dbName, setName1) and self.database.isSetExist(dbName, setName2)):
                unionResult = []
                result = self.database.unionSet(dbName, setName1, setName2, unionResult)
                if(result == NoSqlDb.SET_LOCKED):
                    msg = self.makeMessage("Set Is Locked", responseCode.SET_IS_LOCKED, "{0} or {1}".format(setName1, setName2))
                elif(result == NoSqlDb.SET_UNION_SUCCESS):
                    msg = self.makeMessage("Set Union Success", responseCode.SET_UNION_SUCCESS, unionResult[0])
                else:
                    msg = self.makeMessage("Database Error", responseCode.DB_ERROR, dbName)
            else:
                msg = self.makeMessage("Set Does Not Exist", responseCode.SET_NOT_EXIST, "{0} or {1}".format(setName1, setName2))
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, dbName)
        return msg

    def intersectSet(self, dbName, setName1, setName2):
        if (self.isValidType(dbName)
            and self.isValidType(setName1)
            and self.isValidType(setName2)):
            if (self.database.isSetExist(dbName, setName1) and self.database.isSetExist(dbName, setName2)):
                intersectResult = []
                result = self.database.intersectSet(dbName, setName1, setName2, intersectResult)
                if (result == NoSqlDb.SET_LOCKED):
                    msg = self.makeMessage("Set Is Locked", responseCode.SET_IS_LOCKED,
                                           "{0} or {1}".format(setName1, setName2))
                elif (result == NoSqlDb.SET_INTERSECT_SUCCESS):
                    msg = self.makeMessage("Set Intersect Success", responseCode.SET_INTERSECT_SUCCESS, intersectResult[0])
                else:
                    msg = self.makeMessage("Database Error", responseCode.DB_ERROR, dbName)
            else:
                msg = self.makeMessage("Set Does Not Exist", responseCode.SET_NOT_EXIST,
                                       "{0} or {1}".format(setName1, setName2))
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, dbName)
        return msg

    def diffSet(self, dbName, setName1, setName2):
        if (self.isValidType(dbName)
            and self.isValidType(setName1)
            and self.isValidType(setName2)):
            if (self.database.isSetExist(dbName, setName1) and self.database.isSetExist(dbName, setName2)):
                diffResult = []
                result = self.database.diffSet(dbName, setName1, setName2, diffResult)
                if (result == NoSqlDb.SET_LOCKED):
                    msg = self.makeMessage("Set Is Locked", responseCode.SET_IS_LOCKED,
                                           "{0} or {1}".format(setName1, setName2))
                elif (result == NoSqlDb.SET_DIFF_SUCCESS):
                    msg = self.makeMessage("Set Diff Success", responseCode.SET_DIFF_SUCCESS, diffResult[0])
                else:
                    msg = self.makeMessage("Database Error", responseCode.DB_ERROR, dbName)
            else:
                msg = self.makeMessage("Set Does Not Exist", responseCode.SET_NOT_EXIST,
                                       "{0} or {1}".format(setName1, setName2))
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, dbName)
        return msg

    # replace the existed set with a new set
    def replaceSet(self, dbName, setName, setValue):
        if (self.isValidType(dbName) and self.isValidType(setName)
            and self.isSet(setValue)):
            if (self.database.isSetExist(dbName, setName) is True):
                result = self.database.replaceSet(dbName, setName, setValue)
                if (result == NoSqlDb.SET_LOCKED):
                    msg = self.makeMessage("Set Is Locked", responseCode.SET_IS_LOCKED, setName)
                elif (result == NoSqlDb.SET_REPLACE_SUCCESS):
                    msg = self.makeMessage("Set Replace Success", responseCode.SET_REPLACE_SUCCESS, setName)
                else:
                    msg = self.makeMessage("Database Error", responseCode.DB_ERROR, dbName)
            else:
                msg = self.makeMessage("Set Does Not Exist", responseCode.SET_NOT_EXIST, setName)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, setName)
        return msg
