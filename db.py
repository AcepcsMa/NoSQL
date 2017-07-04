__author__ = 'Ma Haoxiang'

# import
import re
import os
import json

class NoSqlDb:

    # return types
    ELEM_TYPE_ERROR = 0
    CREATE_ELEM_SUCCESS = 1
    ELEM_ALREADY_EXIST = 2
    ELEM_NOT_EXIST = 3
    UPDATE_ELEM_SUCCESS = 4
    GET_ELEM_SUCCESS = 5
    ELEM_INCR_SUCCESS = 6
    ELEM_DECR_SUCCESS = 7

    def __init__(self):
        self.dbNameSet = {"db0", "db1", "db2", "db3", "db4"} # initial db names
        self.elemName = dict()
        self.elemDict = dict()

        for dbName in self.dbNameSet:
            self.elemName[dbName] = set()
            self.elemDict[dbName] = dict()


    # check if the type of elem is valid (string or int)
    def isValidType(self, elem):
        if('str' in str(type(elem)) or 'int' in str(type(elem))):
            return True
        else:
            return False


    def isInt(self, elem):
        if("int" in str(type(elem))):
            return True
        else:
            return False


    # create an element in the db
    def createElem(self, elemName, value, dbName):
        if(self.isValidType(elemName)
           and self.isValidType(value)
           and self.isValidType(dbName)): # check the type of elem name and elem value
            if elemName not in self.elemName[dbName]:
                self.elemName[dbName].add(elemName)
                self.elemDict[dbName][elemName] = value
                return NoSqlDb.CREATE_ELEM_SUCCESS

            else:   # this elem already exists in the db
                return NoSqlDb.ELEM_ALREADY_EXIST

        else:   # the type of elem name or elem value is invalid
            return NoSqlDb.ELEM_TYPE_ERROR


    # update the value of an elem in the db
    def updateElem(self, elemName, value, dbName):
        if elemName not in self.elemName[dbName]:
            return NoSqlDb.ELEM_NOT_EXIST

        else:   # find the elem in the db
            if(self.isValidType(elemName)
               and self.isValidType(value)
               and self.isValidType(dbName)):
                self.elemDict[dbName][elemName] = value
                return NoSqlDb.UPDATE_ELEM_SUCCESS
            else:
                return NoSqlDb.ELEM_TYPE_ERROR


    # get the value of existed elem
    def getElem(self, elemName, dbName):
        if(self.isValidType(elemName)
           and self.isValidType(dbName)):
            if(elemName not in self.elemName[dbName]):
                return (NoSqlDb.ELEM_NOT_EXIST,None)
            else:
                return (NoSqlDb.GET_ELEM_SUCCESS,self.elemDict[dbName][elemName])

        else:
            return (NoSqlDb.ELEM_TYPE_ERROR,None)


    # search element using regular expression
    def searchElem(self, expression, dbName):
        searchResult = set()
        expression = re.sub("\*",".*",expression)   # convert expression to regular expression
        for elemName in self.elemName[dbName]:
            try:
                searchResult.add(re.findall("({})".format(expression),elemName)[0])
            except:
                continue

        return list(searchResult)


    # get all element names in the db
    def getAllElem(self, dbName):
        return list(self.elemName[dbName])


    # increase the value of an element
    def increaseElem(self, elemName, dbName):
        if(self.isValidType(elemName) and self.isValidType(dbName)):
            if(elemName not in self.elemName[dbName]):
                return NoSqlDb.ELEM_NOT_EXIST
            else:
                if(self.isInt(self.elemDict[dbName][elemName])): # check if the element can be increased
                    self.elemDict[dbName][elemName] += 1
                    return NoSqlDb.ELEM_INCR_SUCCESS
                else:
                    return NoSqlDb.ELEM_TYPE_ERROR


    # decrease the value of an element
    def decreaseElem(self, elemName, dbName):
        if (self.isValidType(elemName) and self.isValidType(dbName)):
            if (elemName not in self.elemName[dbName]):
                return NoSqlDb.ELEM_NOT_EXIST
            else:
                if (self.isInt(self.elemDict[dbName][elemName])): # check if the element can be decreased
                    self.elemDict[dbName][elemName] -= 1
                    return NoSqlDb.ELEM_DECR_SUCCESS
                else:
                    return NoSqlDb.ELEM_TYPE_ERROR


    # save the data into file
    def saveDb(self):
        try:    # check if the data directory exists
            os.makedirs("data")
            for dbName in self.dbNameSet:
                os.makedirs("data{}{}".format(os.sep,dbName))
        except Exception as e:
            print (e)

        for dbName in self.dbNameSet:
            # save elements of each db
            with open("data" + os.sep + dbName + os.sep + "elemName.txt", "w") as elemNameFile:
                elemNameFile.write(json.dumps(list(self.elemName[dbName])))
            with open("data" + os.sep + dbName + os.sep + "elemValue.txt", "w") as elemValueFile:
                elemValueFile.write((json.dumps(self.elemDict[dbName])))


    # recover data from file
    def loadDb(self):
        try:
            dbNameSet = os.listdir("data")  # find all dbName in the data directory
            for dbName in dbNameSet:
                self.dbNameSet.add(dbName)

                # recover element names
                with open("data"+os.sep+dbName+os.sep+"elemName.txt","r") as elemNameFile:
                    elemNames = json.loads(elemNameFile.read())
                    for elemName in elemNames:
                        self.elemName[dbName].add(elemName)
                # recover element values
                with open("data" + os.sep + dbName + os.sep + "elemValue.txt", "r") as elemValueFile:
                    self.elemDict[dbName] = json.loads(elemValueFile.read())

        except Exception as e:
            print (e)


if __name__ == "__main__":
    pass
