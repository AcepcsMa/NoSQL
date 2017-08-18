__author__ = 'Ma Haoxiang'

# import
from response import responseCode

class elemHandler:
    def __init__(self, database):
        self.database = database

    # check if the type of elem is valid (string or int)
    def isValidType(self, elem):
        return 'str' in str(type(elem)) or 'int' in str(type(elem))

    # check if the type of an elem is INT
    def isInt(self, elem):
        return "int" in str(type(elem))

    # make the response message
    def makeMessage(self, msg, typeCode, data):
        message = {
            "msg":msg,
            "typeCode":typeCode,
            "data":data
        }
        return message

    # create an element in the db
    def createElem(self, dbName, elemName, elemValue):
        if(self.isValidType(elemName) and self.isValidType(elemValue) and self.isValidType(dbName)): # check the type of elem name and elem value
            if(self.database.isDbExist(dbName)):
                if(self.database.isElemExist(dbName, elemName) is False):
                    result = self.database.createElem(dbName, elemName, elemValue)
                    if(result == responseCode.KEY_NAME_INVALID):
                        msg = self.makeMessage("Elem Name Is Invalid", responseCode.KEY_NAME_INVALID, elemName)
                    elif(result == responseCode.ELEM_CREATE_SUCCESS):
                        msg = self.makeMessage("Make Element Success", responseCode.ELEM_CREATE_SUCCESS, elemName)
                    else:
                        msg = self.makeMessage("Database Error", responseCode.DB_ERROR, dbName)
                else:   # this elem already exists in the db
                    msg = self.makeMessage("Element Already Exists", responseCode.ELEM_ALREADY_EXIST, elemName)
            else:
                msg = self.makeMessage("Database Does Not Exist", responseCode.DB_NOT_EXIST, dbName)
        else:   # the type of elem name or elem value is invalid
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, elemName)
        return msg

    # update the value of an elem in the db
    def updateElem(self, dbName, elemName, elemValue):
        '''if(self.database.isElemExist(dbName, elemName) is False):
            msg = self.makeMessage("Element Does Not Exist", responseCode.ELEM_NOT_EXIST, elemName)

        elif(self.database.isExpired(dbName, elemName, "ELEM") is True):
            msg = self.makeMessage("Elem Is Expired", responseCode.ELEM_EXPIRED, elemName)

        else:   # find the elem in the db
            if(self.isValidType(elemName) and self.isValidType(elemValue) and self.isValidType(dbName)):
                result = self.database.updateElem(elemName, elemValue, dbName)
                if(result == responseCode.ELEM_IS_LOCKED):
                    msg = self.makeMessage("Element Is Locked", responseCode.ELEM_IS_LOCKED, elemName)
                elif(result == responseCode.ELEM_UPDATE_SUCCESS):
                    msg = self.makeMessage("Element Update Success", responseCode.ELEM_UPDATE_SUCCESS, elemName)
                else:
                    msg = self.makeMessage("Database Error", responseCode.DB_ERROR, dbName)
            else:
                msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, elemName)
        return msg'''
        if (self.isValidType(elemName) and self.isValidType(elemValue) and self.isValidType(dbName)):
            if(self.database.isDbExist(dbName)):
                if(self.database.isElemExist(dbName, elemName)):
                    if(self.database.isExpired(dbName, elemName, "ELEM") is False):
                        result = self.database.updateElem(elemName, elemValue, dbName)
                        if (result == responseCode.ELEM_IS_LOCKED):
                            msg = self.makeMessage("Element Is Locked", responseCode.ELEM_IS_LOCKED, elemName)
                        elif (result == responseCode.ELEM_UPDATE_SUCCESS):
                            msg = self.makeMessage("Element Update Success", responseCode.ELEM_UPDATE_SUCCESS, elemName)
                        else:
                            msg = self.makeMessage("Database Error", responseCode.DB_ERROR, dbName)
                    else:
                        msg = self.makeMessage("Elem Is Expired", responseCode.ELEM_EXPIRED, elemName)
                else:
                    msg = self.makeMessage("Element Does Not Exist", responseCode.ELEM_NOT_EXIST, elemName)
            else:
                msg = self.makeMessage("Database Does Not Exist", responseCode.DB_NOT_EXIST, dbName)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, elemName)
        return msg

    # get the value of existed elem
    def getElem(self, dbName, elemName):
        if(self.isValidType(elemName) and self.isValidType(dbName)):
            if(self.database.isDbExist(dbName)):
                if (self.database.isElemExist(dbName, elemName) is False):
                    msg = self.makeMessage("Element Does Not Exist", responseCode.ELEM_NOT_EXIST, elemName)
                else:
                    if(self.database.isExpired(dbName, elemName, "ELEM") is False):
                        msg = self.makeMessage("Element Get Success", responseCode.ELEM_GET_SUCCESS, self.database.getElem(elemName, dbName))
                    else:
                        msg = self.makeMessage("Elem Is Expired", responseCode.ELEM_EXPIRED, elemName)
            else:
                msg = self.makeMessage("Database Does Not Exist", responseCode.DB_NOT_EXIST, dbName)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, elemName)
        return msg

    # search element using regular expression
    def searchElem(self, dbName, expression):
        if(self.isValidType(dbName)):
            searchResult = self.database.searchByRE(dbName, expression, "ELEM")
            msg = self.makeMessage("Search Element Success", responseCode.ELEM_SEARCH_SUCCESS, searchResult)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, dbName)
        return msg

    # get all element names in the db
    def searchAllElem(self, dbName):
        msg = self.makeMessage("All Elements Search Success", responseCode.ELEM_SEARCH_SUCCESS, self.database.searchAllElem(dbName))
        return msg

    # increase the value of an element
    def increaseElem(self, dbName, elemName):
        if(self.isValidType(elemName) and self.isValidType(dbName)):
            if(self.database.isElemExist(dbName, elemName) is False):
                msg = self.makeMessage("Element Does Not Exist", responseCode.ELEM_NOT_EXIST, elemName)
            else:
                if(self.database.isExpired(dbName, elemName, "ELEM") is False):
                    if(self.isInt(self.database.getElem(elemName, dbName))): # check if the element can be increased
                        result = self.database.increaseElem(elemName, dbName)
                        if(result == responseCode.ELEM_INCR_SUCCESS):
                            data = self.database.getElem(elemName, dbName)
                            msg = self.makeMessage("Element Increase Success", responseCode.ELEM_INCR_SUCCESS, data)
                        elif(result == responseCode.ELEM_IS_LOCKED):
                            data = self.database.getElem(elemName, dbName)
                            msg = self.makeMessage("Element Is Locked", responseCode.ELEM_IS_LOCKED, data)
                        else:
                            msg = self.makeMessage("Database Error", responseCode.DB_ERROR, dbName)
                    else:
                        msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, elemName)
                else:
                    msg = self.makeMessage("Elem Is Expired", responseCode.ELEM_EXPIRED, elemName)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, elemName)
        return msg

    # decrease the value of an element
    def decreaseElem(self, dbName, elemName):
        if(self.isValidType(elemName) and self.isValidType(dbName)):
            if(self.database.isElemExist(dbName, elemName) is False):
                msg = self.makeMessage("Element Does Not Exist", responseCode.ELEM_NOT_EXIST, elemName)
            else:
                if(self.database.isExpired(dbName, elemName, "ELEM") is False):
                    if(self.isInt(self.database.getElem(elemName, dbName))): # check if the element can be increased
                        result = self.database.decreaseElem(elemName, dbName)
                        if(result == responseCode.ELEM_DECR_SUCCESS):
                            data = self.database.getElem(elemName, dbName)
                            msg = self.makeMessage("Element Decrease Success", responseCode.ELEM_DECR_SUCCESS, data)
                        elif(result == responseCode.ELEM_IS_LOCKED):
                            data = self.database.getElem(elemName, dbName)
                            msg = self.makeMessage("Element Is Locked", responseCode.ELEM_IS_LOCKED, data)
                        else:
                            msg = self.makeMessage("Database Error", responseCode.DB_ERROR, dbName)
                    else:
                        msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, elemName)
                else:
                    msg = self.makeMessage("Elem Is Expired", responseCode.ELEM_EXPIRED, elemName)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, elemName)
        return msg

    # delete an element in the database
    def deleteElem(self, dbName, elemName):
        if(self.isValidType(elemName) and self.isValidType(dbName)):
            if (self.database.isElemExist(dbName, elemName) is False):
                msg = self.makeMessage("Element Does Not Exist", responseCode.ELEM_NOT_EXIST, elemName)
            else:
                result = self.database.deleteElem(elemName, dbName)
                if(result == responseCode.ELEM_IS_LOCKED):
                    msg = self.makeMessage("Element Is Locked", responseCode.ELEM_IS_LOCKED, elemName)
                elif(result == responseCode.ELEM_DELETE_SUCCESS):
                    msg = self.makeMessage("Element Delete Success", responseCode.ELEM_DELETE_SUCCESS, elemName)
                else:
                    msg = self.makeMessage("Database Error", responseCode.DB_ERROR, dbName)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, elemName)
        return msg

    # set TTL for an element
    def setTTL(self, dbName, elemName, ttl):
        if(self.isValidType(dbName) and self.isValidType(elemName)):
            if(self.database.isElemExist(dbName, elemName) is False):
                msg = self.makeMessage("Element Does Not Exist", responseCode.ELEM_NOT_EXIST, elemName)
            else:
                result = self.database.setElemTTL(dbName, elemName, ttl)
                if(result == responseCode.ELEM_IS_LOCKED):
                    msg = self.makeMessage("Element Is Locked", responseCode.ELEM_IS_LOCKED, elemName)
                elif(result == responseCode.ELEM_TTL_SET_SUCCESS):
                    msg = self.makeMessage("Element TTL Set Success", responseCode.ELEM_TTL_SET_SUCCESS, elemName)
                else:
                    msg = self.makeMessage("Database Error", responseCode.DB_ERROR, dbName)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, elemName)
        return msg

    # clear TTL for an element
    def clearTTL(self, dbName, elemName):
        if(self.isValidType(dbName) and self.isValidType(elemName)):
            if(self.database.isElemExist(dbName, elemName) is False):
                msg = self.makeMessage("Element Does Not Exist", responseCode.ELEM_NOT_EXIST, elemName)
            else:
                result = self.database.clearElemTTL(dbName, elemName)
                if(result == responseCode.ELEM_IS_LOCKED):
                    msg = self.makeMessage("Element Is Locked", responseCode.ELEM_IS_LOCKED, elemName)
                elif(result == responseCode.ELEM_TTL_CLEAR_SUCCESS):
                    msg = self.makeMessage("Element TTL Clear Success", responseCode.ELEM_TTL_CLEAR_SUCCESS, elemName)
                elif(result == responseCode.ELEM_NOT_SET_TTL):
                    msg = self.makeMessage("Element Is Not Set TTL", responseCode.ELEM_NOT_SET_TTL, elemName)
                else:
                    msg = self.makeMessage("Database Error", responseCode.DB_ERROR, dbName)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, elemName)
        return msg

    def showTTL(self, dbName, keyName):
        if(self.isValidType(dbName) and self.isValidType(keyName)):
            if(self.database.isDbExist(dbName)):
                result = self.database.showTTL(dbName, keyName, "ELEM")
                if(result == responseCode.TTL_NO_RECORD):
                    msg = self.makeMessage("TTL No Record", responseCode.TTL_NO_RECORD, keyName)
                elif(result == responseCode.TTL_EXPIRED):
                    msg = self.makeMessage("Element TTL Expired", responseCode.ELEM_EXPIRED, keyName)
                else:
                    msg = self.makeMessage("TTL Show Success", responseCode.TTL_SHOW_SUCCESS, result)
            else:
                msg = self.makeMessage("Database Does Not Exist", responseCode.DB_NOT_EXIST, dbName)
        else:
            msg = self.makeMessage("Element Type Error", responseCode.ELEM_TYPE_ERROR, dbName)
        return msg
