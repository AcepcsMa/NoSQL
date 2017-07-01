__author__ = 'Ma Haoxiang'
import re

class NoSqlDb:

    ELEM_TYPE_ERROR = 0
    CREATE_ELEM_SUCCESS = 1
    ELEM_ALREADY_EXIST = 2
    ELEM_NOT_EXIST = 3
    UPDATE_ELEM_SUCCESS = 4
    GET_ELEM_SUCCESS = 5

    def __init__(self):
        self.elemNameSet = set()
        self.elemDict = dict()


    # check if the type of elem is valid (string or int)
    def isValidType(self, elem):
        if('str' in str(type(elem)) or 'int' in str(type(elem))):
            return True
        else:
            return False

    # create an element in the db
    def createElem(self, elemName, value):
        if(self.isValidType(elemName) and self.isValidType(value)): # check the type of elem name and elem value
            if elemName not in self.elemNameSet:
                self.elemNameSet.add(elemName)
                self.elemDict[elemName] = value
                return NoSqlDb.CREATE_ELEM_SUCCESS

            else:   # this elem already exists in the db
                return NoSqlDb.ELEM_ALREADY_EXIST

        else:   # the type of elem name or elem value is invalid
            return NoSqlDb.ELEM_TYPE_ERROR

    # update the value of an elem in the db
    def updateElem(self, elemName, value):
        if elemName not in self.elemNameSet:
            return NoSqlDb.ELEM_NOT_EXIST

        else:   # find the elem in the db
            if(self.isValidType(elemName) and self.isValidType(value)):
                self.elemDict[elemName] = value
                return NoSqlDb.UPDATE_ELEM_SUCCESS
            else:
                return NoSqlDb.ELEM_TYPE_ERROR

    # get the value of existed elem
    def getElem(self, elemName):
        if(self.isValidType(elemName)):
            if(elemName not in self.elemNameSet):
                return (NoSqlDb.ELEM_NOT_EXIST,None)
            else:
                return (NoSqlDb.GET_ELEM_SUCCESS,self.elemDict[elemName])

        else:
            return (NoSqlDb.ELEM_TYPE_ERROR,None)

    # search element using regular expression
    def searchElem(self, expression):
        searchResult = set()
        expression = re.sub("\*",".*",expression)   # convert expression to regular expression
        for elemName in self.elemNameSet:
            try:
                searchResult.add(re.findall("({})".format(expression),elemName)[0])
            except:
                continue

        return list(searchResult)


if __name__ == '__main__':
    pass
