__author__ = "Ma Haoxiang"

# import
from Response import responseCode
from Utils import *

# a decorator for save trigger
def saveTrigger(func):
    def trigger(*args, **kwargs):
        result = func(*args, **kwargs)
        self = args[0]
        self.opCount += 1
        # when opCounts reach the trigger, save automatically
        if self.opCount == self.saveTrigger:
            self.opCount = 0
            self.saveDb()
            self.logger.info("Auto Save Triggers")
        return result
    return trigger

# a decorator checking key name validity
def keyNameValidity(func):
    def checkValidity(*args, **kwargs):
        lowercase = [chr(i) for i in range(97,123)]
        uppercase = [chr(i) for i in range(65,91)]
        underline = ["_"]
        keyName = kwargs["keyName"]
        if(keyName[0] not in lowercase
           and keyName[0] not in uppercase
           and keyName[0] not in underline):
            return responseCode.KEY_NAME_INVALID
        else:
            result = func(*args, **kwargs)
            return result
    return checkValidity

# a decorator which checks the type of args
def validTypeCheck(func):
    def check(*args, **kwargs):
        dbName = kwargs["dbName"]
        keyName = kwargs["keyName"]
        if((isinstance(dbName, int) is False and isinstance(dbName, str) is False)
           or (isinstance(keyName, str) is False and isinstance(keyName, int) is False)):
            return {
                "msg":"Element Type Error",
                "typeCode":responseCode.ELEM_TYPE_ERROR,
                "data":keyName
            }
        else:
            result = func(*args,**kwargs)
            return result
    return check

# Password check decorator
def passwordCheck(func):
    def check(*args, **kwargs):
        dbName = kwargs["dbName"]
        password = kwargs["password"]
        database = args[0]
        if database.verifyPassword(dbName, password) is False:
            return responseCode.DB_PASSWORD_ERROR
        else:
            return func(*args, **kwargs)
    return check
