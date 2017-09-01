__author__ = "Ma Haoxiang"

# import
from response import responseCode

# a decorator for save trigger
def saveTrigger(func):
    def trigger(*args, **kwargs):
        result = func(*args, **kwargs)
        self = args[0]
        self.opCount += 1
        # when opCounts reach the trigger, save automatically
        if(self.opCount == self.saveTrigger):
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
        keyName = args[2]
        if(keyName[0] not in lowercase and keyName[0] not in uppercase
             and keyName[0] not in underline):
            return responseCode.KEY_NAME_INVALID
        else:
            result = func(*args, **kwargs)
            return result
    return checkValidity

# a decorator which checks the type of args
def validTypeCheck(func):
    def check(*args, **kwargs):
        dbName = args[1]
        keyName = args[2]
        if(("str" not in str(type(dbName)) and "int" not in str(type(dbName)))
           or ("str" not in str(type(keyName)) and "int" not in str(type(keyName)))):
            return {
                "msg":"Element Type Error",
                "typeCode":responseCode.ELEM_TYPE_ERROR,
                "data":keyName
            }
        else:
            result = func(*args,**kwargs)
            return result
    return check