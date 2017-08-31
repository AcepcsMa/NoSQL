__author__ = 'Ma Haoxiang'

# import
from response import responseCode

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
                msg = self.makeMessage(responseCode.detail[result],result,setName)
            else:
                msg = self.makeMessage("Set Already Exists", responseCode.SET_ALREADY_EXIST, setName)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, setName)
        return msg

    # get set value
    def getSet(self, dbName, setName):
        if(self.isValidType(dbName) and self.isValidType(setName)):
            if(self.database.isSetExist(dbName, setName) is True):
                if(self.database.isExpired(dbName, setName, "SET") is False):
                    setValue = self.database.getSet(dbName, setName)
                    msg = self.makeMessage("Set Get Success", responseCode.SET_GET_SUCCESS, setValue)
                else:
                    msg = self.makeMessage("Set Is Expired", responseCode.SET_EXPIRED, setName)
            else:
                msg = self.makeMessage("Set Does Not Exist", responseCode.SET_NOT_EXIST, setName)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, setName)
        return msg

    # insert a value into the given set
    def insertSet(self, dbName, setName, setValue):
        if(self.isValidType(dbName) and self.isValidType(setName)):
            if(self.database.isSetExist(dbName, setName)):
                if(self.database.isExpired(dbName, setName, "SET") is False):
                    result = self.database.insertSet(dbName, setName, setValue)
                    msg = self.makeMessage(responseCode.detail[result], result, setName)
                else:
                    msg = self.makeMessage("Set Is Expired", responseCode.SET_EXPIRED, setName)
            else:
                msg = self.makeMessage("Set Does Not Exist", responseCode.SET_NOT_EXIST, setName)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, setName)
        return msg

    # remove the given value from a set
    def rmFromSet(self, dbName, setName, setValue):
        if(self.isValidType(dbName) and self.isValidType(setName)):
            if(self.database.isSetExist(dbName, setName)):
                if(self.database.isExpired(dbName, setName, "SET") is False):
                    result = self.database.rmFromSet(dbName, setName, setValue)
                    msg = self.makeMessage(responseCode.detail[result], result, setName)
                else:
                    msg = self.makeMessage("Set Is Expired", responseCode.SET_EXPIRED, setName)
            else:
                msg = self.makeMessage("Set Does Not Exist", responseCode.SET_NOT_EXIST, setName)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, setName)
        return msg

    # clear the given set
    def clearSet(self, dbName, setName):
        if(self.isValidType(dbName) and self.isValidType(setName)):
            if(self.database.isSetExist(dbName, setName)):
                if(self.database.isExpired(dbName, setName, "SET") is False):
                    result = self.database.clearSet(dbName, setName)
                    msg = self.makeMessage(responseCode.detail[result], result, setName)
                else:
                    msg = self.makeMessage("Set Is Expired", responseCode.SET_EXPIRED, setName)
            else:
                msg = self.makeMessage("Set Does Not Exist", responseCode.SET_NOT_EXIST, setName)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, setName)
        return msg

    # delete the given set
    def deleteSet(self, dbName, setName):
        if (self.isValidType(dbName) and self.isValidType(setName)):
            if (self.database.isSetExist(dbName, setName)):
                if(self.database.isExpired(dbName, setName, "SET") is False):
                    result = self.database.deleteSet(dbName, setName)
                    msg = self.makeMessage(responseCode.detail[result], result, setName)
                else:
                    msg = self.makeMessage("Set Is Expired", responseCode.SET_EXPIRED, setName)
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

    # set union operation
    def unionSet(self, dbName, setName1, setName2):
        if(self.isValidType(dbName)
           and self.isValidType(setName1)
           and self.isValidType(setName2)):
            if(self.database.isSetExist(dbName, setName1) and self.database.isSetExist(dbName, setName2)):
                if(self.database.isSetExpired(dbName,setName1) is False and self.database.isSetExpired(dbName, setName2) is False):
                    unionResult = []
                    result = self.database.unionSet(dbName, setName1, setName2, unionResult)
                    if(result == responseCode.SET_IS_LOCKED):
                        msg = self.makeMessage("Set Is Locked", responseCode.SET_IS_LOCKED, "{0} or {1}".format(setName1, setName2))
                    elif(result == responseCode.SET_UNION_SUCCESS):
                        msg = self.makeMessage("Set Union Success", responseCode.SET_UNION_SUCCESS, unionResult[0])
                    else:
                        msg = self.makeMessage("Database Error", responseCode.DB_ERROR, dbName)
                else:
                    msg = self.makeMessage("Set Is Expired", responseCode.SET_EXPIRED, "{} or {}".format(setName1, setName2))
            else:
                msg = self.makeMessage("Set Does Not Exist", responseCode.SET_NOT_EXIST, "{0} or {1}".format(setName1, setName2))
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, dbName)
        return msg

    # set intersect operation
    def intersectSet(self, dbName, setName1, setName2):
        if (self.isValidType(dbName)
            and self.isValidType(setName1)
            and self.isValidType(setName2)):
            if (self.database.isSetExist(dbName, setName1) and self.database.isSetExist(dbName, setName2)):
                if(self.database.isSetExpired(dbName,setName1) is False and self.database.isSetExpired(dbName, setName2) is False):
                    intersectResult = []
                    result = self.database.intersectSet(dbName, setName1, setName2, intersectResult)
                    if (result == responseCode.SET_IS_LOCKED):
                        msg = self.makeMessage("Set Is Locked", responseCode.SET_IS_LOCKED,
                                               "{0} or {1}".format(setName1, setName2))
                    elif (result == responseCode.SET_INTERSECT_SUCCESS):
                        msg = self.makeMessage("Set Intersect Success", responseCode.SET_INTERSECT_SUCCESS, intersectResult[0])
                    else:
                        msg = self.makeMessage("Database Error", responseCode.DB_ERROR, dbName)
                else:
                    msg = self.makeMessage("Set Is Expired", responseCode.SET_EXPIRED, "{} or {}".format(setName1, setName2))
            else:
                msg = self.makeMessage("Set Does Not Exist", responseCode.SET_NOT_EXIST,
                                       "{0} or {1}".format(setName1, setName2))
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, dbName)
        return msg

    # set difference operation
    def diffSet(self, dbName, setName1, setName2):
        if (self.isValidType(dbName)
            and self.isValidType(setName1)
            and self.isValidType(setName2)):
            if (self.database.isSetExist(dbName, setName1) and self.database.isSetExist(dbName, setName2)):
                if(self.database.isSetExpired(dbName,setName1) is False and self.database.isSetExpired(dbName, setName2) is False):
                    diffResult = []
                    result = self.database.diffSet(dbName, setName1, setName2, diffResult)
                    if(result == responseCode.SET_IS_LOCKED):
                        msg = self.makeMessage("Set Is Locked", responseCode.SET_IS_LOCKED,
                                               "{0} or {1}".format(setName1, setName2))
                    elif(result == responseCode.SET_DIFF_SUCCESS):
                        msg = self.makeMessage("Set Diff Success", responseCode.SET_DIFF_SUCCESS, diffResult[0])
                    else:
                        msg = self.makeMessage("Database Error", responseCode.DB_ERROR, dbName)
                else:
                    msg = self.makeMessage("Set Is Expired", responseCode.SET_EXPIRED, "{} or {}".format(setName1, setName2))
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
                if(self.database.isExpired(dbName, setName, "SET") is False):
                    result = self.database.replaceSet(dbName, setName, setValue)
                    msg = self.makeMessage(responseCode.detail[result], result, setName)
                else:
                    msg = self.makeMessage("Set Is Expired", responseCode.SET_EXPIRED, setName)
            else:
                msg = self.makeMessage("Set Does Not Exist", responseCode.SET_NOT_EXIST, setName)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, setName)
        return msg

    # set TTL for a set
    def setTTL(self, dbName, setName, ttl):
        if (self.isValidType(dbName) and self.isValidType(setName)):
            if (self.database.isSetExist(dbName, setName) is False):
                msg = self.makeMessage("Set Does Not Exist", responseCode.SET_NOT_EXIST, setName)
            else:
                result = self.database.setSetTTL(dbName, setName, ttl)
                msg = self.makeMessage(responseCode.detail[result], result, setName)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, setName)
        return msg

    # clear TTL for a set
    def clearTTL(self, dbName, setName):
        if (self.isValidType(dbName) and self.isValidType(setName)):
            if (self.database.isSetExist(dbName, setName) is False):
                msg = self.makeMessage("Set Does Not Exist", responseCode.SET_NOT_EXIST, setName)
            else:
                result = self.database.clearSetTTL(dbName, setName)
                msg = self.makeMessage(responseCode.detail[result], result, setName)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, setName)
        return msg

    def showTTL(self, dbName, keyName):
        if(self.isValidType(dbName) and self.isValidType(keyName)):
            if(self.database.isDbExist(dbName)):
                code, result = self.database.showTTL(dbName, keyName, "SET")
                msg = self.makeMessage(responseCode.detail[code], code, result)
            else:
                msg = self.makeMessage("Database Does Not Exist", responseCode.DB_NOT_EXIST, dbName)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, dbName)
        return msg
