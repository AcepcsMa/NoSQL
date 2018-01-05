__author__ = 'Ma Haoxiang'

# import
import flask
import Database
from ConfigParser import ConfigParser
import Timer
import TTLTimer
from TTLTool import TTLTool
from Response import responseCode
from DbHandler import DbHandler
from ElemHandler import ElemHandler
from ListHandler import ListHandler
from HashHandler import HashHandler
from SetHandler import SetHandler
from ZSetHandler import ZSetHandler
import json

app = flask.Flask(__name__)

@app.route("/setDbPassword",methods=["POST"])
def setDbPassword():
    myHandler = DbHandler(database)
    try:
        adminKey = flask.request.json["adminKey"]
        dbName = flask.request.json["dbName"]
        password = flask.request.json["password"]
    except:
        adminKey = dbName = password = None
    result = myHandler.setDbPassword(adminKey, dbName, password)
    return flask.jsonify(result)

@app.route("/changeDbPassword",methods=["PUT"])
def changeadDbPassword():
    myHandler = DbHandler(database)
    try:
        adminKey = flask.request.json["adminKey"]
        dbName = flask.request.json["dbName"]
        originalPwd = flask.request.json["originalPwd"]
        newPwd = flask.request.json["newPwd"]

    except:
        adminKey = dbName = originalPwd = newPwd = None
    result = myHandler.changeDbPassword(adminKey, dbName, originalPwd, newPwd)
    return flask.jsonify(result)

@app.route("/removeDbPassword",methods=["DELETE"])
def removeDbPassword():
    myHandler = DbHandler(database)
    try:
        adminKey = flask.request.json["adminKey"]
        dbName = flask.request.json["dbName"]
    except:
        adminKey = dbName = None
    result = myHandler.removeDbPassword(adminKey, dbName)
    return flask.jsonify(result)

@app.route("/getType/<string:dbName>/<string:keyName>",methods=["GET"])
def getType(dbName, keyName):
    result = database.getType(dbName, keyName)
    return flask.jsonify(result)

@app.route("/setTTL",methods=["POST"])
def setTTL():
    try:
        dataType = flask.request.json["dataType"]
        dbName = flask.request.json["dbName"]
        keyName = flask.request.json["keyName"]
        ttl = flask.request.json["ttl"]
    except:
        dataType = dbName = keyName = ttl = None
    result = ttlTool.setTTL(dbName, keyName, ttl, dataType)
    return flask.jsonify(result)

@app.route("/clearTTL",methods=["POST"])
def clearTTL():
    try:
        dataType = flask.request.json["dataType"]
        dbName = flask.request.json["dbName"]
        keyName = flask.request.json["keyName"]
    except:
        dataType = dbName = keyName = None
    result = ttlTool.clearTTL(dbName, keyName, dataType)
    return flask.jsonify(result)

@app.route("/makeElem",methods=["POST"])
def makeElem():
    myHandler = ElemHandler(database)
    dataJson = json.loads(flask.request.get_data())
    try:
        dbName = dataJson["dbName"]
        keyName = dataJson["elemName"]
        value = dataJson["elemValue"]
    except:
        dbName = keyName = value = None
    try:
        password = dataJson["password"]
    except:
        password = None
    result = myHandler.createElem(dbName=dbName,
                                  keyName=keyName,
                                  value=value,
                                  password=password)

    resp = flask.make_response(flask.jsonify(result))
    resp.headers['Access-Control-Allow-Origin'] = "*"
    return resp

@app.route("/getElem/<string:dbName>/<string:elemName>", defaults={"password": None})
@app.route("/getElem/<string:dbName>/<string:elemName>/<string:password>",methods=["GET"])
def getElem(dbName, elemName, password):
    myHandler = ElemHandler(database)
    result = myHandler.getElem(dbName=dbName,
                               keyName=elemName,
                               password=password)

    resp = flask.make_response(flask.jsonify(result))
    resp.headers['Access-Control-Allow-Origin'] = "*"
    return resp

@app.route("/updateElem",methods=["PUT"])
def updateElem():
    myHandler = ElemHandler(database)
    try:
        dbName = flask.request.json["dbName"]
        keyName = flask.request.json["elemName"]
        value = flask.request.json["elemValue"]
    except:
        dbName = keyName = value = None
    try:
        password = flask.request.json["password"]
    except:
        password = None
    result = myHandler.updateElem(dbName=dbName,
                                  keyName=keyName,
                                  value=value,
                                  password=password)
    return flask.jsonify(result)

@app.route("/searchElem/<string:dbName>/<string:expression>",
           defaults={"password": None})
@app.route("/searchElem/<string:dbName>/<string:expression>/<string:password>",
           methods=["GET"])
def searchElem(dbName, expression, password):
    myHandler = ElemHandler(database)
    result = myHandler.searchElem(dbName=dbName,
                                  expression=expression,
                                  password=password)
    return flask.jsonify(result)

@app.route("/getAllElem/<dbName>", defaults={"password": None})
@app.route("/getAllElem/<string:dbName>/<string:password>", methods=["GET"])
def searchAllElem(dbName, password):
    myHandler = ElemHandler(database)
    result = myHandler.searchAllElem(dbName=dbName,
                                     password=password)
    return flask.jsonify(result)

@app.route("/increaseElem/<string:dbName>/<string:elemName>",
           defaults={"password": None},
           methods=["PUT"])
@app.route("/increaseElem/<string:dbName>/<string:elemName>/<string:password>",
           methods=["PUT"])
def increaseElem(dbName, elemName, password):
    myHandler = ElemHandler(database)
    result = myHandler.increaseElem(dbName=dbName,
                                    keyName=elemName,
                                    password=password)
    return flask.jsonify(result)

@app.route("/decreaseElem/<string:dbName>/<string:elemName>",
           defaults={"password": None},
           methods=["PUT"])
@app.route("/decreaseElem/<string:dbName>/<string:elemName>/<string:password>",
           methods=["PUT"])
def decreaseElem(dbName, elemName, password):
    myHandler = ElemHandler(database)
    result = myHandler.decreaseElem(dbName=dbName,
                                    keyName=elemName,
                                    password=password)
    return flask.jsonify(result)

@app.route("/deleteElem/<string:dbName>/<string:keyName>",
           defaults={"password":None},
           methods=["DELETE"])
@app.route("/deleteElem/<string:dbName>/<string:keyName>/<string:password>",
           methods=["DELETE"])
def deleteElem(dbName, keyName, password):
    myHandler = ElemHandler(database)
    result = myHandler.deleteElem(dbName=dbName,
                                  keyName=keyName,
                                  password=password)
    return flask.jsonify(result)

@app.route("/makeList",methods=["POST"])
def makeList():
    myHandler = ListHandler(database)
    try:
        dbName = flask.request.json["dbName"]
        listName = flask.request.json["listName"]
    except:
        dbName = listName = None
    try:
        password = flask.request.json["password"]
    except:
        password = None

    result = myHandler.createList(dbName=dbName,
                                  keyName=listName,
                                  password=password)
    return flask.jsonify(result)

@app.route("/getList/<string:dbName>/<string:listName>",
           defaults={"password": None},
           methods=["GET"])
@app.route("/getList/<string:dbName>/<string:listName>/<string:password>",
           methods=["GET"])
def getList(dbName, listName, password):
    myHandler = ListHandler(database)
    result = myHandler.getList(dbName=dbName,
                               keyName=listName,
                               password=password)
    return flask.jsonify(result)

@app.route("/leftGetList/<string:dbName>/<string:listName>/<int:count>",
           defaults={"password": None},
           methods=["GET"])
@app.route("/leftGetList/<string:dbName>/<string:listName>/<int:count>/<string:password>",
           methods=["GET"])
def leftGetList(dbName, listName, count, password):
    myHandler = ListHandler(database)
    result = myHandler.getListL(dbName=dbName,
                                keyName=listName,
                                count=count,
                                password=password)
    return flask.jsonify(result)

@app.route("/rightGetList/<string:dbName>/<string:listName>/<int:count>",
           defaults={"password": None},
           methods=["GET"])
@app.route("/rightGetList/<string:dbName>/<string:listName>/<int:count>/<string:password>",
           methods=["GET"])
def rightGetList(dbName, listName, count, password):
    myHandler = ListHandler(database)
    result = myHandler.getListR(dbName=dbName,
                                keyName=listName,
                                count=count,
                                password=password)
    return flask.jsonify(result)

@app.route("/getListByRange/<string:dbName>/<string:listName>/<int:start>/<int:end>",
           defaults={"password": None},
           methods=["GET"])
@app.route("/getListByRange/<string:dbName>/<string:listName>/<int:start>/<int:end>/<string:password>",
           methods=["GET"])
def getListByRange(dbName, listName, start, end, password):
    myHandler = ListHandler(database)
    result = myHandler.getListByRange(dbName=dbName, keyName=listName,
                                      start=start, end=end,
                                      password=password)
    return flask.jsonify(result)

@app.route("/getListRandom/<string:dbName>/<string:listName>/<int:numRand>",
           defaults={"password": None},
           methods=["GET"])
@app.route("/getListRandom/<string:dbName>/<string:listName>/<int:numRand>/<string:password>",
           methods=["GET"])
def getListRandom(dbName, listName, numRand, password):
    myHandler = ListHandler(database)
    result = myHandler.getListRandom(dbName=dbName,
                                     keyName=listName,
                                     numRand=numRand)
    return flask.jsonify(result)

@app.route("/insertList",methods=["PUT"])
def insertList():
    myHandler = ListHandler(database)
    try:
        dbName = flask.request.json["dbName"]
        listName = flask.request.json["listName"]
        listValue = flask.request.json["listValue"]
    except:
        dbName = listName = listValue = None
    try:
        password = flask.request.json["password"]
    except:
        password = None
    result = myHandler.insertList(dbName=dbName, keyName=listName,
                                  value=listValue, password=password)
    return flask.jsonify(result)

@app.route("/leftInsertList",methods=["PUT"])
def leftInsertList():
    myHandler = ListHandler(database)
    try:
        dbName = flask.request.json["dbName"]
        listName = flask.request.json["listName"]
        listValue = flask.request.json["listValue"]
    except:
        dbName = listName = listValue = None
    try:
        password = flask.request.json["password"]
    except:
        password = None
    result = myHandler.insertListL(dbName=dbName, keyName=listName,
                                   value=listValue, password=password)
    return flask.jsonify(result)

@app.route("/deleteList/<string:dbName>/<string:listName>",
           defaults={"password": None},
           methods=["DELETE"])
@app.route("/deleteList/<string:dbName>/<string:listName>/<string:password>",
           methods=["DELETE"])
def deleteList(dbName, listName, password):
    myHandler = ListHandler(database)
    result = myHandler.deleteList(dbName=dbName,
                                  keyName=listName,
                                  password=password)
    return flask.jsonify(result)

@app.route("/rmFromList",methods=["PUT"])
def rmFromList():
    myHandler = ListHandler(database)
    try:
        dbName = flask.request.json["dbName"]
        listName = flask.request.json["listName"]
        listValue = flask.request.json["listValue"]
    except:
        dbName = listName = listValue = None
    try:
        password = flask.request.json["password"]
    except:
        password = None
    result = myHandler.rmFromList(dbName=dbName, keyName=listName,
                                  value=listValue, password=password)
    return flask.jsonify(result)

@app.route("/clearList/<dbName>/<listName>",
           defaults={"password": None},
           methods=["PUT"])
@app.route("/clearList/<dbName>/<listName>/<string:password>",
           methods=["PUT"])
def clearList(dbName, listName, password):
    myHandler = ListHandler(database)
    result = myHandler.clearList(dbName=dbName,
                                 keyName=listName,
                                 password=password)
    return flask.jsonify(result)

@app.route("/mergeLists",methods=["PUT"])
def mergeLists():
    myHandler = ListHandler(database)
    try:
        dbName = flask.request.json["dbName"]
        listName1 = flask.request.json["list1"]
        listName2 = flask.request.json["list2"]
        resultListName = flask.request.json["resultList"]
        resultListName = None if len(resultListName) == 0 else resultListName
    except:
        dbName = listName1 = listName2 = resultListName = None
    try:
        password = flask.request.json["password"]
    except:
        password = None
    result = myHandler.mergeLists(dbName=dbName, keyName1=listName1,
                                  keyName2=listName2, resultKeyName=resultListName,
                                  password=password)
    return flask.jsonify(result)

@app.route("/searchList/<string:dbName>/<string:expression>",
           defaults={"password": None},
           methods=["GET"])
@app.route("/searchList/<string:dbName>/<string:expression>/<string:password>",
           methods=["GET"])
def searchList(dbName, expression, password):
    myHandler = ListHandler(database)
    result = myHandler.searchList(dbName=dbName,
                                  expression=expression,
                                  password=password)
    return flask.jsonify(result)

@app.route("/searchAllList/<string:dbName>",
           defaults={"password": None},
           methods=["GET"])
@app.route("/searchAllList/<string:dbName>/<string:password>",
           methods=["GET"])
def searchAllList(dbName, password):
    myHandler = ListHandler(database)
    result = myHandler.searchAllList(dbName=dbName,
                                     password=password)
    return flask.jsonify(result)

@app.route("/getListSize/<string:dbName>/<string:listName>",
           defaults={"password": None},
           methods=["GET"])
@app.route("/getListSize/<string:dbName>/<string:listName>/<string:password>",
           methods=["GET"])
def getListSize(dbName, listName, password):
    myHandler = ListHandler(database)
    result = myHandler.getSize(dbName=dbName,
                               keyName=listName,
                               password=password)
    return flask.jsonify(result)

@app.route("/makeHash",methods=["POST"])
def makeHash():
    myHandler = HashHandler(database)
    try:
        dbName = flask.request.json["dbName"]
        hashName = flask.request.json["hashName"]
    except:
        dbName = hashName = None
    try:
        password = flask.request.json["password"]
    except:
        password = None
    result = myHandler.createHash(dbName=dbName,
                                  keyName=hashName,
                                  password=password)
    return flask.jsonify(result)

@app.route("/getHash/<string:dbName>/<string:hashName>",
           defaults={"password": None},
           methods=["GET"])
@app.route("/getHash/<string:dbName>/<string:hashName>/<string:password>",
           methods=["GET"])
def getHash(dbName, hashName, password):
    myHandler = HashHandler(database)
    result = myHandler.getHash(dbName=dbName,
                               keyName=hashName,
                               password=password)
    return flask.jsonify(result)

@app.route("/getHashKeySet/<string:dbName>/<string:hashName>",
           defaults={"password": None},
           methods=["GET"])
@app.route("/getHashKeySet/<string:dbName>/<string:hashName>/<string:password>",
           methods=["GET"])
def getHashKeySet(dbName, hashName, password):
    myHandler = HashHandler(database)
    result = myHandler.getKeySet(dbName=dbName,
                                 keyName=hashName,
                                 password=password)
    return flask.jsonify(result)

@app.route("/getHashValues/<string:dbName>/<string:hashName>",
           defaults={"password": None},
           methods=["GET"])
@app.route("/getHashValues/<string:dbName>/<string:hashName>/<string:password>",
           methods=["GET"])
def getHashValues(dbName, hashName, password):
    myHandler = HashHandler(database)
    result = myHandler.getValues(dbName=dbName,
                                 keyName=hashName,
                                 password=password)
    return flask.jsonify(result)

@app.route("/getMultipleHashValues",methods=["POST"])
def getMultipleHashValues():
    myHandler = HashHandler(database)
    try:
        dbName = flask.request.json["dbName"]
        hashName = flask.request.json["hashName"]
        keys = flask.request.json["keyNames"]
    except:
        dbName = hashName = keys = None
    try:
        password = flask.request.json["password"]
    except:
        password = None
    result = myHandler.getMultipleValues(dbName=dbName, keyName=hashName,
                                         keys=keys, password=password)
    return flask.jsonify(result)

@app.route("/insertHash",methods=["PUT"])
def insertHash():
    myHandler = HashHandler(database)
    try:
        dbName = flask.request.json["dbName"]
        hashName = flask.request.json["hashName"]
        keyName = flask.request.json["keyName"]
        value = flask.request.json["value"]
    except:
        dbName = hashName = keyName = value = None
    try:
        password = flask.request.json["password"]
    except:
        password = None
    result = myHandler.insertHash(dbName=dbName, keyName=hashName,
                                  key=keyName, value=value,
                                  password=password)
    return flask.jsonify(result)

@app.route("/isHashKeyExist/<string:dbName>/<string:hashName>/<string:keyName>",
           defaults={"password": None},
           methods=["GET"])
@app.route("/isHashKeyExist/<string:dbName>/<string:hashName>/<string:keyName>/<string:password>",
           methods=["GET"])
def isHashKeyExist(dbName, hashName, keyName, password):
    myHandler = HashHandler(database)
    result = myHandler.isKeyExist(dbName=dbName, keyName=hashName,
                                  key=keyName, password=password)
    return flask.jsonify(result)

@app.route("/deleteHash/<dbName>/<hashName>",
           defaults={"password": None},
           methods=["DELETE"])
@app.route("/deleteHash/<dbName>/<hashName>/<string:password>",
           methods=["DELETE"])
def deleteHash(dbName, hashName, password):
    myHandler = HashHandler(database)
    result = myHandler.deleteHash(dbName=dbName,
                                  keyName=hashName,
                                  password=password)
    return flask.jsonify(result)

@app.route("/rmFromHash",methods=["PUT"])
def rmFromHash():
    myHandler = HashHandler(database)
    try:
        dbName = flask.request.json["dbName"]
        hashName = flask.request.json["hashName"]
        keyName = flask.request.json["keyName"]
    except:
        dbName = hashName = keyName = None
    try:
        password = flask.request.json["password"]
    except:
        password = None
    result = myHandler.rmFromHash(dbName=dbName, keyName=hashName,
                                  key=keyName, password=password)
    return flask.jsonify(result)

@app.route("/clearHash/<string:dbName>/<string:hashName>",
           defaults={"password": None},
           methods=["GET"])
@app.route("/clearHash/<string:dbName>/<string:hashName>/<string:password>",
           methods=["GET"])
def clearHash(dbName, hashName, password):
    myHandler = HashHandler(database)
    result = myHandler.clearHash(dbName=dbName,
                                 keyName=hashName,
                                 password=password)
    return flask.jsonify(result)

@app.route("/replaceHash",methods=["PUT"])
def replaceHash():
    myHandler = HashHandler(database)
    try:
        dbName = flask.request.json["dbName"]
        hashName = flask.request.json["hashName"]
        hashValue = flask.request.json["hashValue"]
    except:
        dbName = hashName = hashValue = None
    try:
        password = flask.request.json["password"]
    except:
        password = None
    result = myHandler.replaceHash(dbName=dbName, keyName=hashName,
                                   value=hashValue, password=password)
    return flask.jsonify(result)

@app.route("/mergeHashs",methods=["PUT"])
def mergeHashs():
    myHandler = HashHandler(database)
    try:
        mergeMode = flask.request.json["mode"]
        dbName = flask.request.json["dbName"]
        hashName1 = flask.request.json["hash1"]
        hashName2 = flask.request.json["hash2"]
        resultHashName = flask.request.json["resultHash"]
        resultHashName = None if len(resultHashName) == 0 else resultHashName
    except:
        mergeMode = dbName = hashName1 = hashName2 = resultHashName = None
    try:
        password = flask.request.json["password"]
    except:
        password = None
    result = myHandler.mergeHashs(dbName=dbName, keyName1=hashName1,
                                  keyName2=hashName2, resultKeyName=resultHashName,
                                  mergeMode=mergeMode, password=password)
    return flask.jsonify(result)

@app.route("/searchHash/<dbName>/<string:expression>",
           defaults={"password": None},
           methods=["GET"])
@app.route("/searchHash/<dbName>/<string:expression>/<string:password>", methods=["GET"])
def searchHash(dbName, expression, password):
    myHandler = HashHandler(database)
    result = myHandler.searchHash(dbName=dbName, expression=expression, password=password)
    return flask.jsonify(result)

@app.route("/searchAllHash/<dbName>",
           defaults={"password": None},
           methods=["GET"])
@app.route("/searchAllHash/<string:dbName>/<string:password>", methods=["GET"])
def searchAllHash(dbName, password):
    myHandler = HashHandler(database)
    result = myHandler.searchAllHash(dbName=dbName, password=password)
    return flask.jsonify(result)

@app.route("/getHashSize/<string:dbName>/<string:hashName>",
           defaults={"password": None},
           methods=["GET"])
@app.route("/getHashSize/<string:dbName>/<string:hashName>/<string:password>",
           methods=["GET"])
def getHashSize(dbName, hashName, password):
    myHandler = HashHandler(database)
    result = myHandler.getSize(dbName=dbName,
                               keyName=hashName,
                               password=password)
    return flask.jsonify(result)

@app.route("/increaseHash/<string:dbName>/<string:hashName>/<string:keyName>",
           defaults={"password": None},
           methods=["PUT"])
@app.route("/increaseHash/<string:dbName>/<string:hashName>/<string:keyName>/<string:password>",
           methods=["PUT"])
def increaseHash(dbName, hashName, keyName, password):
    myHandler = HashHandler(database)
    result = myHandler.increaseHash(dbName=dbName, keyName=hashName,
                                    key=keyName, password=password)
    return flask.jsonify(result)

@app.route("/decreaseHash/<string:dbName>/<string:hashName>/<string:keyName>",
           defaults={"password": None},
           methods=["PUT"])
@app.route("/decreaseHash/<string:dbName>/<string:hashName>/<string:keyName>/<string:password>",
           methods=["PUT"])
def decreaseHash(dbName, hashName, keyName, password):
    myHandler = HashHandler(database)
    result = myHandler.decreaseHash(dbName=dbName, keyName=hashName,
                                    key=keyName, password=password)
    return flask.jsonify(result)

@app.route("/makeSet",methods=["POST"])
def makeSet():
    myHandler = SetHandler(database)
    try:
        dbName = flask.request.json["dbName"]
        setName = flask.request.json["setName"]
    except:
        dbName = setName = None
    try:
        password = flask.request.json["password"]
    except:
        password = None
    result = myHandler.createSet(dbName=dbName,
                                 keyName=setName,
                                 password=password)
    return flask.jsonify(result)

@app.route("/getSet/<string:dbName>/<string:setName>",
           defaults={"password": None},
           methods=["GET"])
@app.route("/getSet/<string:dbName>/<string:setName>/<string:password>",
           methods=["GET"])
def getSet(dbName, setName, password):
    myHandler = SetHandler(database)
    result = myHandler.getSet(dbName=dbName,
                              keyName=setName,
                              password=password)
    return flask.jsonify(result)

@app.route("/getSetRandom/<string:dbName>/<string:setName>/<int:numRand>",
           defaults={"password": None},
           methods=["GET"])
@app.route("/getSetRandom/<string:dbName>/<string:setName>/<int:numRand>/<string:password>",
           methods=["GET"])
def getSetRandom(dbName, setName, numRand, password):
    myHandler = SetHandler(database)
    result = myHandler.getSetRandom(dbName=dbName, keyName=setName,
                                    numRand=numRand, password=password)
    return flask.jsonify(result)

@app.route("/insertSet",methods=["PUT"])
def insertSet():
    myHandler = SetHandler(database)
    try:
        dbName = flask.request.json["dbName"]
        setName = flask.request.json["setName"]
        setValue = flask.request.json["setValue"]
    except:
        dbName = setName = setValue = None
    try:
        password = flask.request.json["password"]
    except:
        password = None
    result = myHandler.insertSet(dbName=dbName, keyName=setName,
                                 setValue=setValue, password=password)
    return flask.jsonify(result)

@app.route("/rmFromSet",methods=["PUT"])
def rmFromSet():
    myHandler = SetHandler(database)
    try:
        dbName = flask.request.json["dbName"]
        setName = flask.request.json["setName"]
        setValue = flask.request.json["setValue"]
    except:
        dbName = setName = setValue = None
    try:
        password = flask.request.json["password"]
    except:
        password = None
    result = myHandler.rmFromSet(dbName=dbName, keyName=setName,
                                 setValue=setValue, password=password)
    return flask.jsonify(result)

@app.route("/clearSet/<string:dbName>/<string:setName>",
           defaults={"password": None},
           methods=["PUT"])
@app.route("/clearSet/<string:dbName>/<string:setName>/<string:password>",
           methods=["PUT"])
def clearSet(dbName, setName, password):
    myHandler = SetHandler(database)
    result = myHandler.clearSet(dbName=dbName,
                                keyName=setName,
                                password=password)
    return flask.jsonify(result)

@app.route("/deleteSet/<string:dbName>/<string:setName>",
           defaults={"password": None},
           methods=["DELETE"])
@app.route("/deleteSet/<string:dbName>/<string:setName>/<string:password>",
           methods=["DELETE"])
def deleteSet(dbName, setName, password):
    myHandler = SetHandler(database)
    result = myHandler.deleteSet(dbName=dbName,
                                 keyName=setName,
                                 password=password)
    return flask.jsonify(result)

@app.route("/searchSet/<string:dbName>/<string:expression>",
           defaults={"password": None},
           methods=["GET"])
@app.route("/searchSet/<string:dbName>/<string:expression>/<string:password>",
           methods=["GET"])
def searchSet(dbName, expression, password):
    myHandler = SetHandler(database)
    result = myHandler.searchSet(dbName=dbName,
                                 expression=expression,
                                 password=password)
    return flask.jsonify(result)

@app.route("/searchAllSet/<string:dbName>",
           defaults={"password": None},
           methods=["GET"])
@app.route("/searchAllSet/<string:dbName>/<string:password>",
           methods=["GET"])
def searchAllSet(dbName, password):
    myHandler = SetHandler(database)
    result = myHandler.searchAllSet(dbName=dbName,
                                    password=password)
    return flask.jsonify(result)

@app.route("/unionSet",methods=["PUT"])
def unionSet():
    myHandler = SetHandler(database)
    try:
        dbName = flask.request.json["dbName"]
        setName1 = flask.request.json["setName1"]
        setName2 = flask.request.json["setName2"]
    except:
        dbName = setName1 = setName2 = None
    try:
        password = flask.request.json["password"]
    except:
        password = None
    result = myHandler.unionSet(dbName=dbName, setName1=setName1,
                                setName2=setName2, password=password)
    return flask.jsonify(result)

@app.route("/intersectSet",methods=["PUT"])
def intersectSet():
    myHandler = SetHandler(database)
    try:
        dbName = flask.request.json["dbName"]
        setName1 = flask.request.json["setName1"]
        setName2 = flask.request.json["setName2"]
    except:
        dbName = setName1 = setName2 = None
    try:
        password = flask.request.json["password"]
    except:
        password = None
    result = myHandler.intersectSet(dbName=dbName, setName1=setName1,
                                    setName2=setName2, password=password)
    return flask.jsonify(result)

@app.route("/diffSet",methods=["PUT"])
def diffSet():
    myHandler = SetHandler(database)
    try:
        dbName = flask.request.json["dbName"]
        setName1 = flask.request.json["setName1"]
        setName2 = flask.request.json["setName2"]
    except:
        dbName = setName1 = setName2 = None
    try:
        password = flask.request.json["password"]
    except:
        password = None
    result = myHandler.diffSet(dbName=dbName, setName1=setName1,
                               setName2=setName2, password=password)
    return flask.jsonify(result)

@app.route("/replaceSet",methods=["PUT"])
def replaceSet():
    myHandler = SetHandler(database)
    try:
        dbName = flask.request.json["dbName"]
        setName = flask.request.json["setName"]
        setValue = set(flask.request.json["setValue"])
    except:
        dbName = setName = setValue = None
    try:
        password = flask.request.json["password"]
    except:
        password = None
    result = myHandler.replaceSet(dbName=dbName, keyName=setName,
                                  value=setValue, password=password)
    return flask.jsonify(result)

@app.route("/getSetSize/<string:dbName>/<string:setName>",
           defaults={"password": None},
           methods=["GET"])
@app.route("/getSetSize/<string:dbName>/<string:setName>/<string:password>",
           methods=["GET"])
def getSetSize(dbName, setName, password):
    myHandler = SetHandler(database)
    result = myHandler.getSize(dbName=dbName,
                               keyName=setName,
                               password=password)
    return flask.jsonify(result)


@app.route("/showTTL/<string:dbName>/<string:dataType>/<string:keyName>",
           defaults={"password": None},
           methods=["PUT"])
@app.route("/showTTL/<string:dbName>/<string:dataType>/<string:keyName>/<string:password>",
           methods=["PUT"])
def showTTL(dbName, dataType, keyName, password):
    if(dataType == "ELEM"):
        myHandler = ElemHandler(database)
    elif(dataType == "LIST"):
        myHandler = ListHandler(database)
    elif(dataType == "HASH"):
        myHandler = HashHandler(database)
    elif(dataType == "SET"):
        myHandler = SetHandler(database)
    elif(dataType == "ZSET"):
        myHandler = ZSetHandler(database)
    else:
        myHandler = None
    try:
        result = myHandler.showTTL(dbName=dbName,
                                   keyName=keyName,
                                   password=password)
        return flask.jsonify(result)
    except:
        msg = {"msg":"Data Type Error",
               "typeCode":responseCode.DATA_TYPE_ERROR,
               "data":dataType}
        return flask.jsonify(msg)

@app.route("/makeZSet",methods=["POST"])
def makeZSet():
    myHandler = ZSetHandler(database)
    try:
        dbName = flask.request.json["dbName"]
        zsetName = flask.request.json["zsetName"]
    except:
        dbName = zsetName = None
    try:
        password = flask.request.json["password"]
    except:
        password = None
    result = myHandler.createZSet(dbName=dbName,
                                  keyName=zsetName,
                                  password=password)
    return flask.jsonify(result)

@app.route("/getZSet/<string:dbName>/<string:zsetName>",
           defaults={"password": None},
           methods=["GET"])
@app.route("/getZSet/<string:dbName>/<string:zsetName>/<string:password>",
           methods=["GET"])
def getZSet(dbName, zsetName, password):
    myHandler = ZSetHandler(database)
    result = myHandler.getZSet(dbName=dbName,
                               keyName=zsetName,
                               password=password)
    return flask.jsonify(result)

@app.route("/insertZSet",methods=["PUT"])
def insertZSet():
    myHandler = ZSetHandler(database)
    try:
        dbName = flask.request.json["dbName"]
        zsetName = flask.request.json["zsetName"]
        value = flask.request.json["value"]
        score = flask.request.json["score"]
    except:
        dbName = zsetName = value = score = None
    try:
        password = flask.request.json["password"]
    except:
        password = None
    result = myHandler.insertZSet(dbName=dbName, keyName=zsetName,
                                  value=value, score=score,
                                  password=password)
    return flask.jsonify(result)

@app.route("/rmFromZSet",methods=["PUT"])
def rmFromZSet():
    myHandler = ZSetHandler(database)
    try:
        dbName = flask.request.json["dbName"]
        zsetName = flask.request.json["zsetName"]
        value = flask.request.json["value"]
    except:
        dbName = zsetName = value = None
    try:
        password = flask.request.json["password"]
    except:
        password = None
    result = myHandler.rmFromZSet(dbName=dbName, keyName=zsetName,
                                  value=value, password=password)
    return flask.jsonify(result)

@app.route("/clearZSet/<string:dbName>/<string:zsetName>",
           defaults={"password": None},
           methods=["PUT"])
@app.route("/clearZSet/<string:dbName>/<string:zsetName>/<string:password>",
           methods=["PUT"])
def clearZSet(dbName, zsetName, password):
    myHandler = ZSetHandler(database)
    result = myHandler.clearZSet(dbName=dbName,
                                 keyName=zsetName,
                                 password=password)
    return flask.jsonify(result)

@app.route("/deleteZSet/<string:dbName>/<string:zsetName>",
           defaults={"password": None},
           methods=["DELETE"])
@app.route("/deleteZSet/<string:dbName>/<string:zsetName>/<string:password>",
           methods=["DELETE"])
def deleteZSet(dbName, zsetName, password):
    myHandler = ZSetHandler(database)
    result = myHandler.deleteZSet(dbName=dbName,
                                  keyName=zsetName,
                                  password=password)
    return flask.jsonify(result)

@app.route("/searchZSet/<string:dbName>/<string:expression>",
           defaults={"password": None},
           methods=["GET"])
@app.route("/searchZSet/<string:dbName>/<string:expression>/<string:password>",
           methods=["GET"])
def searchZSet(dbName, expression, password):
    myHandler = ZSetHandler(database)
    result = myHandler.searchZSet(dbName=dbName,
                                  expression=expression,
                                  password=password)
    return flask.jsonify(result)

@app.route("/searchAllZSet/<string:dbName>",
           defaults={"password": None},
           methods=["GET"])
@app.route("/searchAllZSet/<string:dbName>/<string:password>",
           methods=["GET"])
def searchAllZSet(dbName, password):
    myHandler = ZSetHandler(database)
    result = myHandler.searchAllZSet(dbName=dbName,
                                     password=password)
    return flask.jsonify(result)

@app.route("/findMinFromZSet/<string:dbName>/<string:zsetName>",
           defaults={"password": None},
           methods=["GET"])
@app.route("/findMinFromZSet/<string:dbName>/<string:zsetName>/<string:password>",
           methods=["GET"])
def findMinFromZSet(dbName, zsetName, password):
    myHandler = ZSetHandler(database)
    result = myHandler.findMin(dbName=dbName,
                               keyName=zsetName,
                               password=password)
    return flask.jsonify(result)

@app.route("/findMaxFromZSet/<string:dbName>/<string:zsetName>",
           defaults={"password": None},
           methods=["GET"])
@app.route("/findMaxFromZSet/<string:dbName>/<string:zsetName>/<string:password>",
           methods=["GET"])
def findMaxFromZSet(dbName, zsetName, password):
    myHandler = ZSetHandler(database)
    result = myHandler.findMax(dbName=dbName,
                               keyName=zsetName,
                               password=password)
    return flask.jsonify(result)

@app.route("/getScore/<string:dbName>/<string:zsetName>/<string:valueName>",
           defaults={"password": None},
           methods=["GET"])
@app.route("/getScore/<string:dbName>/<string:zsetName>/<string:valueName>/<string:password>",
           methods=["GET"])
def getScore(dbName, zsetName,valueName, password):
    myHandler = ZSetHandler(database)
    result = myHandler.getScore(dbName=dbName, keyName=zsetName,
                                value=valueName, password=password)
    return flask.jsonify(result)

@app.route("/getValuesByRange/<string:dbName>/<string:zsetName>/<int:start>/<int:end>",
           defaults={"password": None},
           methods=["GET"])
@app.route("/getValuesByRange/<string:dbName>/<string:zsetName>/<int:start>/<int:end>/<string:password>",
           methods=["GET"])
def getValuesByRange(dbName, zsetName, start, end, password):
    myHandler = ZSetHandler(database)
    result = myHandler.getValuesByRange(dbName=dbName, keyName=zsetName,
                                        start=start, end=end,
                                        password=password)
    return flask.jsonify(result)

@app.route("/getZSetSize/<string:dbName>/<string:zsetName>",methods=["GET"])
def getZSetSize(dbName, zsetName):
    myHandler = ZSetHandler(database)
    result = myHandler.getSize(dbName, zsetName)
    return flask.jsonify(result)

@app.route("/getRank/<string:dbName>/<string:zsetName>/<string:value>",methods=["GET"])
def getRank(dbName, zsetName, value):
    myHandler = ZSetHandler(database)
    result = myHandler.getRank(dbName, zsetName, value)
    return flask.jsonify(result)

@app.route("/rmFromZSetByScore",methods=["PUT"])
def rmFromZSetByScore():
    myHandler = ZSetHandler(database)
    try:
        dbName = flask.request.json["dbName"]
        zsetName = flask.request.json["zsetName"]
        start = flask.request.json["start"]
        end = flask.request.json["end"]
    except:
        dbName = zsetName = start = end = None
    result = myHandler.rmByScore(dbName, zsetName, start, end)
    return flask.jsonify(result)

@app.route("/addDatabase",methods=["POST"])
def addDatabase():
    myHandler = DbHandler(database)
    try:
        dbName = flask.request.json["dbName"]
    except:
        dbName = None
    result = myHandler.addDatabase(dbName)
    return flask.jsonify(result)

@app.route("/getAllDatabase",methods=["GET"])
def getAllDatabase():
    myHandler = DbHandler(database)
    result = myHandler.getAllDatabase()
    return flask.jsonify(result)

@app.route("/delDatabase/<string:dbName>",methods=["DELETE"])
def delDatabase(dbName):
    myHandler = DbHandler(database)
    result = myHandler.delDatabase(dbName)
    return flask.jsonify(result)

@app.route("/save",methods=["PUT"])
def saveDb():
    myHandler = DbHandler(database)
    result = myHandler.saveDb()
    return flask.jsonify(result)

@app.route("/changeSaveInterval/<int:interval>",methods=["PUT"])
def changeSaveInterval(interval):
    result = saveTimer.setInterval(interval)
    return flask.jsonify(result)

if __name__ == '__main__':


    # init the config parser and read the server config
    confParser = ConfigParser()
    serverConfig = confParser.getServerConfig("server.conf")

    # init the database
    databaseList = []
    database = Database.NoSqlDb(serverConfig)
    databaseList.append(database)

    # init the save timer
    saveTimer = Timer.Timer(databaseList, serverConfig["SAVE_INTERVAL"])
    saveTimer.start()

    # init the ttl timer
    TTLTimer = TTLTimer.ttlTimer(databaseList, serverConfig["TTL_CHECK_INTERVAL"])
    TTLTimer.setDaemon(True)

    # init ttl tool
    ttlTool = TTLTool(databaseList)

    # run the server
    app.run(host=serverConfig["HOST"], port=serverConfig["PORT"], debug=serverConfig["DEBUG"])
