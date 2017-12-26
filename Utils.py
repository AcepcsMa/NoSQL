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
            if(isinstance(elem, str) is False and isinstance(elem, int) is False):
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



if __name__ == "__main__":
    import hashlib

    print(hashlib.md5("admin".encode()).hexdigest())