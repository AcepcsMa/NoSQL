__author__ = 'Ma Haoxiang'

class Utils(object):

    @staticmethod
    def isInt(elem):
        return isinstance(elem, int)

    @staticmethod
    def isDict(elem):
        return isinstance(elem, dict)

    @staticmethod
    def isSet(elem):
        return isinstance(elem, set)

    @staticmethod
    def isValidType(*elems):
        for elem in elems:
            if elem is not None and not isinstance(elem, str) and not isinstance(elem, int):
                return False
        return True

    @staticmethod
    def makeMessage(msg, typeCode, data):
        message = {
            "msg":msg,
            "typeCode":typeCode,
            "data":data
        }
        return message
