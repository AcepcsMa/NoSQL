__author__ = 'Ma Haoxiang'

# import
import flask
import db
from ConfigParser import configParser
import Timer
import TTLTimer
from TTLTool import TTLTool
from Response import responseCode
from DbHandler import DbHandler
from ElemHandler import elemHandler
from ListHandler import listHandler
from HashHandler import hashHandler
from SetHandler import setHandler
from ZSetHandler import zsetHandler

app = flask.Flask(__name__)

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
    result = ttlTool.clearListTTL(dbName, keyName, dataType)
    return flask.jsonify(result)

@app.route("/makeElem",methods=["POST"])
def makeElem():
    myHandler = elemHandler(database)
    try:
        dbName = flask.request.json["dbName"]
        elemName = flask.request.json["elemName"]
        elemValue = flask.request.json["elemValue"]
    except:
        dbName = elemName = elemValue = None
    result = myHandler.createElem(dbName, elemName, elemValue)
    return flask.jsonify(result)

@app.route("/getElem/<string:dbName>/<string:elemName>",methods=["GET"])
def getElem(dbName, elemName):
    myHandler = elemHandler(database)
    result = myHandler.getElem(dbName, elemName)
    return flask.jsonify(result)

@app.route("/updateElem",methods=["PUT"])
def updateElem():
    myHandler = elemHandler(database)
    try:
        dbName = flask.request.json["dbName"]
        elemName = flask.request.json["elemName"]
        elemValue = flask.request.json["elemValue"]
    except:
        dbName = elemName = elemValue = None
    result = myHandler.updateElem(dbName, elemName, elemValue)
    return flask.jsonify(result)

@app.route("/searchElem/<string:dbName>/<string:expression>",methods=["GET"])
def searchElem(dbName,expression):
    myHandler = elemHandler(database)
    result = myHandler.searchElem(dbName, expression)
    return flask.jsonify(result)

@app.route("/getAllElem/<dbName>",methods=["GET"])
def searchAllElem(dbName):
    myHandler = elemHandler(database)
    result = myHandler.searchAllElem(dbName)
    return flask.jsonify(result)

@app.route("/increaseElem/<string:dbName>/<string:elemName>",methods=["PUT"])
def increaseElem(dbName, elemName):
    myHandler = elemHandler(database)
    result = myHandler.increaseElem(dbName, elemName)
    return flask.jsonify(result)

@app.route("/decreaseElem/<string:dbName>/<string:elemName>",methods=["PUT"])
def decreaseElem(dbName, elemName):
    myHandler = elemHandler(database)
    result = myHandler.decreaseElem(dbName, elemName)
    return flask.jsonify(result)

@app.route("/deleteElem/<string:dbName>/<string:elemName>",methods=["DELETE"])
def deleteElem(dbName, elemName):
    myHandler = elemHandler(database)
    result = myHandler.deleteElem(dbName, elemName)
    return flask.jsonify(result)

@app.route("/makeList",methods=["POST"])
def makeList():
    myHandler = listHandler(database)
    try:
        dbName = flask.request.json["dbName"]
        listName = flask.request.json["listName"]
    except:
        dbName = listName = None
    result = myHandler.createList(dbName, listName)
    return flask.jsonify(result)

@app.route("/getList/<string:dbName>/<string:listName>",methods=["GET"])
def getList(dbName, listName):
    myHandler = listHandler(database)
    result = myHandler.getList(dbName, listName)
    return flask.jsonify(result)

@app.route("/leftGetList/<string:dbName>/<string:listName>/<int:count>",methods=["GET"])
def leftGetList(dbName, listName, count):
    myHandler = listHandler(database)
    result = myHandler.getListL(dbName, listName, count)
    return flask.jsonify(result)

@app.route("/rightGetList/<string:dbName>/<string:listName>/<int:count>",methods=["GET"])
def rightGetList(dbName, listName, count):
    myHandler = listHandler(database)
    result = myHandler.getListR(dbName, listName, count)
    return flask.jsonify(result)

@app.route("/getListByRange/<string:dbName>/<string:listName>/<int:start>/<int:end>",methods=["GET"])
def getListByRange(dbName, listName, start, end):
    myHandler = listHandler(database)
    result = myHandler.getListByRange(dbName, listName, start, end)
    return flask.jsonify(result)

@app.route("/getListRandom/<string:dbName>/<string:listName>/<int:numRand>",methods=["GET"])
def getListRandom(dbName, listName, numRand):
    myHandler = listHandler(database)
    result = myHandler.getListRandom(dbName, listName, numRand)
    return flask.jsonify(result)

@app.route("/insertList",methods=["PUT"])
def insertList():
    myHandler = listHandler(database)
    try:
        dbName = flask.request.json["dbName"]
        listName = flask.request.json["listName"]
        listValue = flask.request.json["listValue"]
    except:
        dbName = listName = listValue = None
    result = myHandler.insertList(dbName, listName, listValue)
    return flask.jsonify(result)

@app.route("/leftInsertList",methods=["PUT"])
def leftInsertList():
    myHandler = listHandler(database)
    try:
        dbName = flask.request.json["dbName"]
        listName = flask.request.json["listName"]
        listValue = flask.request.json["listValue"]
    except:
        dbName = listName = listValue = None
    result = myHandler.insertListL(dbName, listName, listValue)
    return flask.jsonify(result)

@app.route("/deleteList/<string:dbName>/<string:listName>",methods=["DELETE"])
def deleteList(dbName, listName):
    myHandler = listHandler(database)
    result = myHandler.deleteList(dbName, listName)
    return flask.jsonify(result)

@app.route("/rmFromList",methods=["PUT"])
def rmFromList():
    myHandler = listHandler(database)
    try:
        dbName = flask.request.json["dbName"]
        listName = flask.request.json["listName"]
        listValue = flask.request.json["listValue"]
    except:
        dbName = listName = listValue = None
    result = myHandler.rmFromList(dbName, listName, listValue)
    return flask.jsonify(result)

@app.route("/clearList/<dbName>/<listName>",methods=["PUT"])
def clearList(dbName, listName):
    myHandler = listHandler(database)
    result = myHandler.clearList(dbName, listName)
    return flask.jsonify(result)

@app.route("/mergeLists",methods=["PUT"])
def mergeLists():
    myHandler = listHandler(database)
    try:
        dbName = flask.request.json["dbName"]
        listName1 = flask.request.json["list1"]
        listName2 = flask.request.json["list2"]
        resultListName = flask.request.json["resultList"]
        resultListName = None if len(resultListName) == 0 else resultListName
    except:
        dbName = listName1 = listName2 = resultListName = None
    result = myHandler.mergeLists(dbName, listName1, listName2, resultListName)
    return flask.jsonify(result)

@app.route("/searchList/<string:dbName>/<string:expression>",methods=["GET"])
def searchList(dbName, expression):
    myHandler = listHandler(database)
    result = myHandler.searchList(dbName, expression)
    return flask.jsonify(result)

@app.route("/searchAllList/<string:dbName>",methods=["GET"])
def searchAllList(dbName):
    myHandler = listHandler(database)
    result = myHandler.searchAllList(dbName)
    return flask.jsonify(result)

@app.route("/getListSize/<string:dbName>/<string:listName>",methods=["GET"])
def getListSize(dbName, listName):
    myHandler = listHandler(database)
    result = myHandler.getSize(dbName, listName)
    return flask.jsonify(result)

@app.route("/makeHash",methods=["POST"])
def makeHash():
    myHandler = hashHandler(database)
    try:
        dbName = flask.request.json["dbName"]
        hashName = flask.request.json["hashName"]
    except:
        dbName = hashName = None
    result = myHandler.createHash(dbName, hashName)
    return flask.jsonify(result)

@app.route("/getHash/<string:dbName>/<string:hashName>",methods=["GET"])
def getHash(dbName, hashName):
    myHandler = hashHandler(database)
    result = myHandler.getHash(dbName, hashName)
    return flask.jsonify(result)

@app.route("/getHashKeySet/<string:dbName>/<string:hashName>",methods=["GET"])
def getHashKeySet(dbName, hashName):
    myHandler = hashHandler(database)
    result = myHandler.getKeySet(dbName, hashName)
    return flask.jsonify(result)

@app.route("/getHashValues/<string:dbName>/<string:hashName>",methods=["GET"])
def getHashValues(dbName, hashName):
    myHandler = hashHandler(database)
    result = myHandler.getValues(dbName, hashName)
    return flask.jsonify(result)

@app.route("/getMultipleHashValues",methods=["POST"])
def getMultipleHashValues():
    myHandler = hashHandler(database)
    try:
        dbName = flask.request.json["dbName"]
        hashName = flask.request.json["hashName"]
        keyNames = flask.request.json["keyNames"]
    except:
        dbName = hashName = keyNames = None
    result = myHandler.getMultipleValues(dbName, hashName, keyNames)
    return flask.jsonify(result)

@app.route("/insertHash",methods=["PUT"])
def insertHash():
    myHandler = hashHandler(database)
    try:
        dbName = flask.request.json["dbName"]
        hashName = flask.request.json["hashName"]
        keyName = flask.request.json["keyName"]
        value = flask.request.json["value"]
    except:
        dbName = hashName = keyName = value = None
    result = myHandler.insertHash(dbName, hashName, keyName, value)
    return flask.jsonify(result)

@app.route("/isHashKeyExist/<string:dbName>/<string:hashName>/<string:keyName>",methods=["GET"])
def isHashKeyExist(dbName, hashName, keyName):
    myHandler = hashHandler(database)
    result = myHandler.isKeyExist(dbName, hashName, keyName)
    return flask.jsonify(result)

@app.route("/deleteHash/<dbName>/<hashName>",methods=["DELETE"])
def deleteHash(dbName, hashName):
    myHandler = hashHandler(database)
    result = myHandler.deleteHash(dbName, hashName)
    return flask.jsonify(result)

@app.route("/rmFromHash",methods=["PUT"])
def rmFromHash():
    myHandler = hashHandler(database)
    try:
        dbName = flask.request.json["dbName"]
        hashName = flask.request.json["hashName"]
        keyName = flask.request.json["keyName"]
    except:
        dbName = hashName = keyName = None
    result = myHandler.rmFromHash(dbName, hashName, keyName)
    return flask.jsonify(result)

@app.route("/clearHash/<string:dbName>/<string:hashName>",methods=["GET"])
def clearHash(dbName, hashName):
    myHandler = hashHandler(database)
    result = myHandler.clearHash(dbName, hashName)
    return flask.jsonify(result)

@app.route("/replaceHash",methods=["PUT"])
def replaceHash():
    myHandler = hashHandler(database)
    try:
        dbName = flask.request.json["dbName"]
        hashName = flask.request.json["hashName"]
        hashValue = flask.request.json["hashValue"]
    except:
        dbName = hashName = hashValue = None
    result = myHandler.replaceHash(dbName, hashName, hashValue)
    return flask.jsonify(result)

@app.route("/mergeHashs",methods=["PUT"])
def mergeHashs():
    myHandler = hashHandler(database)
    try:
        mergeMode = flask.request.json["mode"]
        dbName = flask.request.json["dbName"]
        hashName1 = flask.request.json["hash1"]
        hashName2 = flask.request.json["hash2"]
        resultHashName = flask.request.json["resultHash"]
        resultHashName = None if len(resultHashName) == 0 else resultHashName
    except:
        mergeMode = dbName = hashName1 = hashName2 = resultHashName = None
    result = myHandler.mergeHashs(dbName, hashName1, hashName2, resultHashName, mergeMode)
    return flask.jsonify(result)

@app.route("/searchHash/<dbName>/<string:expression>",methods=["GET"])
def searchHash(dbName, expression):
    myHandler = hashHandler(database)
    result = myHandler.searchHash(dbName, expression)
    return flask.jsonify(result)

@app.route("/searchAllHash/<dbName>",methods=["GET"])
def searchAllHash(dbName):
    myHandler = hashHandler(database)
    result = myHandler.searchAllHash(dbName)
    return flask.jsonify(result)

@app.route("/getHashSize/<string:dbName>/<string:hashName>",methods=["GET"])
def getHashSize(dbName, hashName):
    myHandler = hashHandler(database)
    result = myHandler.getSize(dbName, hashName)
    return flask.jsonify(result)

@app.route("/increaseHash/<string:dbName>/<string:hashName>/<string:keyName>",methods=["PUT"])
def increaseHash(dbName, hashName, keyName):
    myHandler = hashHandler(database)
    result = myHandler.increaseHash(dbName, hashName, keyName)
    return flask.jsonify(result)

@app.route("/decreaseHash/<string:dbName>/<string:hashName>/<string:keyName>",methods=["PUT"])
def decreaseHash(dbName, hashName, keyName):
    myHandler = hashHandler(database)
    result = myHandler.decreaseHash(dbName, hashName, keyName)
    return flask.jsonify(result)

@app.route("/makeSet",methods=["POST"])
def makeSet():
    myHandler = setHandler(database)
    try:
        dbName = flask.request.json["dbName"]
        setName = flask.request.json["setName"]
    except:
        dbName = setName = None
    result = myHandler.createSet(dbName, setName)
    return flask.jsonify(result)

@app.route("/getSet/<string:dbName>/<string:setName>",methods=["GET"])
def getSet(dbName, setName):
    myHandler = setHandler(database)
    result = myHandler.getSet(dbName, setName)
    return flask.jsonify(result)

@app.route("/getSetRandom/<string:dbName>/<string:setName>/<int:numRand>",methods=["GET"])
def getSetRandom(dbName, setName, numRand):
    myHandler = setHandler(database)
    result = myHandler.getSetRandom(dbName, setName, numRand)
    return flask.jsonify(result)

@app.route("/insertSet",methods=["PUT"])
def insertSet():
    myHandler = setHandler(database)
    try:
        dbName = flask.request.json["dbName"]
        setName = flask.request.json["setName"]
        setValue = flask.request.json["setValue"]
    except:
        dbName = setName = setValue = None
    result = myHandler.insertSet(dbName, setName, setValue)
    return flask.jsonify(result)

@app.route("/rmFromSet",methods=["PUT"])
def rmFromSet():
    myHandler = setHandler(database)
    try:
        dbName = flask.request.json["dbName"]
        setName = flask.request.json["setName"]
        setValue = flask.request.json["setValue"]
    except:
        dbName = setName = setValue = None
    result = myHandler.rmFromSet(dbName, setName, setValue)
    return flask.jsonify(result)

@app.route("/clearSet/<string:dbName>/<string:setName>",methods=["PUT"])
def clearSet(dbName, setName):
    myHandler = setHandler(database)
    result = myHandler.clearSet(dbName, setName)
    return flask.jsonify(result)

@app.route("/deleteSet/<string:dbName>/<string:setName>",methods=["DELETE"])
def deleteSet(dbName, setName):
    myHandler = setHandler(database)
    result = myHandler.deleteSet(dbName, setName)
    return flask.jsonify(result)

@app.route("/searchSet/<string:dbName>/<string:expression>",methods=["GET"])
def searchSet(dbName, expression):
    myHandler = setHandler(database)
    result = myHandler.searchSet(dbName, expression)
    return flask.jsonify(result)

@app.route("/searchAllSet/<string:dbName>",methods=["GET"])
def searchAllSet(dbName):
    myHandler = setHandler(database)
    result = myHandler.searchAllSet(dbName)
    return flask.jsonify(result)

@app.route("/unionSet",methods=["PUT"])
def unionSet():
    myHandler = setHandler(database)
    try:
        dbName = flask.request.json["dbName"]
        setName1 = flask.request.json["setName1"]
        setName2 = flask.request.json["setName2"]
    except:
        dbName = setName1 = setName2 = None
    result = myHandler.unionSet(dbName, setName1, setName2)
    return flask.jsonify(result)

@app.route("/intersectSet",methods=["PUT"])
def intersectSet():
    myHandler = setHandler(database)
    try:
        dbName = flask.request.json["dbName"]
        setName1 = flask.request.json["setName1"]
        setName2 = flask.request.json["setName2"]
    except:
        dbName = setName1 = setName2 = None
    result = myHandler.intersectSet(dbName, setName1, setName2)
    return flask.jsonify(result)

@app.route("/diffSet",methods=["PUT"])
def diffSet():
    myHandler = setHandler(database)
    try:
        dbName = flask.request.json["dbName"]
        setName1 = flask.request.json["setName1"]
        setName2 = flask.request.json["setName2"]
    except:
        dbName = setName1 = setName2 = None
    result = myHandler.diffSet(dbName, setName1, setName2)
    return flask.jsonify(result)

@app.route("/replaceSet",methods=["PUT"])
def replaceSet():
    myHandler = setHandler(database)
    try:
        dbName = flask.request.json["dbName"]
        setName = flask.request.json["setName"]
        setValue = set(flask.request.json["setValue"])
    except:
        dbName = setName = setValue = None
    result = myHandler.replaceSet(dbName, setName, setValue)
    return flask.jsonify(result)

@app.route("/getSetSize/<string:dbName>/<string:setName>",methods=["GET"])
def getSetSize(dbName, setName):
    myHandler = setHandler(database)
    result = myHandler.getSize(dbName, setName)
    return flask.jsonify(result)

@app.route("/showTTL/<string:dbName>/<string:dataType>/<string:keyName>",methods=["PUT"])
def showTTL(dbName, dataType, keyName):
    if(dataType == "ELEM"):
        myHandler = elemHandler(database)
    elif(dataType == "LIST"):
        myHandler = listHandler(database)
    elif(dataType == "HASH"):
        myHandler = hashHandler(database)
    elif(dataType == "SET"):
        myHandler = setHandler(database)
    elif(dataType == "ZSET"):
        myHandler = zsetHandler(database)
    else:
        myHandler = None
    try:
        result = myHandler.showTTL(dbName, keyName)
        return flask.jsonify(result)
    except:
        msg = {"msg":"Data Type Error",
               "typeCode":responseCode.DATA_TYPE_ERROR,
               "data":dataType}
        return flask.jsonify(msg)

@app.route("/makeZSet",methods=["POST"])
def makeZSet():
    myHandler = zsetHandler(database)
    try:
        dbName = flask.request.json["dbName"]
        zsetName = flask.request.json["zsetName"]
    except:
        dbName = zsetName = None
    result = myHandler.createZSet(dbName, zsetName)
    return flask.jsonify(result)

@app.route("/getZSet/<string:dbName>/<string:zsetName>",methods=["GET"])
def getZSet(dbName, zsetName):
    myHandler = zsetHandler(database)
    result = myHandler.getZSet(dbName, zsetName)
    return flask.jsonify(result)

@app.route("/insertZSet",methods=["PUT"])
def insertZSet():
    myHandler = zsetHandler(database)
    try:
        dbName = flask.request.json["dbName"]
        zsetName = flask.request.json["zsetName"]
        value = flask.request.json["value"]
        score = flask.request.json["score"]
    except:
        dbName = zsetName = value = score = None
    result = myHandler.insertZSet(dbName, zsetName, value, score)
    return flask.jsonify(result)

@app.route("/rmFromZSet",methods=["PUT"])
def rmFromZSet():
    myHandler = zsetHandler(database)
    try:
        dbName = flask.request.json["dbName"]
        zsetName = flask.request.json["zsetName"]
        value = flask.request.json["value"]
    except:
        dbName = zsetName = value = None
    result = myHandler.rmFromZSet(dbName, zsetName, value)
    return flask.jsonify(result)

@app.route("/clearZSet/<string:dbName>/<string:zsetName>",methods=["PUT"])
def clearZSet(dbName, zsetName):
    myHandler = zsetHandler(database)
    result = myHandler.clearZSet(dbName, zsetName)
    return flask.jsonify(result)

@app.route("/deleteZSet/<string:dbName>/<string:zsetName>",methods=["DELETE"])
def deleteZSet(dbName, zsetName):
    myHandler = zsetHandler(database)
    result = myHandler.deleteZSet(dbName, zsetName)
    return flask.jsonify(result)

@app.route("/searchZSet/<string:dbName>/<string:expression>",methods=["GET"])
def searchZSet(dbName, expression):
    myHandler = zsetHandler(database)
    result = myHandler.searchZSet(dbName, expression)
    return flask.jsonify(result)

@app.route("/searchAllZSet/<string:dbName>",methods=["GET"])
def searchAllZSet(dbName):
    myHandler = zsetHandler(database)
    result = myHandler.searchAllZSet(dbName)
    return flask.jsonify(result)

@app.route("/findMinFromZSet/<string:dbName>/<string:zsetName>",methods=["GET"])
def findMinFromZSet(dbName, zsetName):
    myHandler = zsetHandler(database)
    result = myHandler.findMin(dbName, zsetName)
    return flask.jsonify(result)

@app.route("/findMaxFromZSet/<string:dbName>/<string:zsetName>",methods=["GET"])
def findMaxFromZSet(dbName, zsetName):
    myHandler = zsetHandler(database)
    result = myHandler.findMax(dbName, zsetName)
    return flask.jsonify(result)

@app.route("/getScore/<string:dbName>/<string:zsetName>/<string:valueName>",methods=["GET"])
def getScore(dbName, zsetName,valueName):
    myHandler = zsetHandler(database)
    result = myHandler.getScore(dbName, zsetName, valueName)
    return flask.jsonify(result)

@app.route("/getValuesByRange/<string:dbName>/<string:zsetName>/<int:start>/<int:end>",methods=["GET"])
def getValuesByRange(dbName, zsetName, start, end):
    myHandler = zsetHandler(database)
    result = myHandler.getValuesByRange(dbName, zsetName, start, end)
    return flask.jsonify(result)

@app.route("/getZSetSize/<string:dbName>/<string:zsetName>",methods=["GET"])
def getZSetSize(dbName, zsetName):
    myHandler = zsetHandler(database)
    result = myHandler.getSize(dbName, zsetName)
    return flask.jsonify(result)

@app.route("/getRank/<string:dbName>/<string:zsetName>/<string:value>",methods=["GET"])
def getRank(dbName, zsetName, value):
    myHandler = zsetHandler(database)
    result = myHandler.getRank(dbName, zsetName, value)
    return flask.jsonify(result)

@app.route("/rmFromZSetByScore",methods=["PUT"])
def rmFromZSetByScore():
    myHandler = zsetHandler(database)
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
    confParser = configParser()
    serverConfig = confParser.getServerConfig("server.conf")

    # init the database
    databaseList = []
    database = db.NoSqlDb(serverConfig)
    databaseList.append(database)

    # init the save timer
    saveTimer = Timer.timer(databaseList, serverConfig["SAVE_INTERVAL"])
    saveTimer.start()

    # init the ttl timer
    TTLTimer = TTLTimer.ttlTimer(databaseList, serverConfig["TTL_CHECK_INTERVAL"])
    TTLTimer.setDaemon(True)

    # init ttl tool
    ttlTool = TTLTool(databaseList)

    # run the server
    app.run(host=serverConfig["HOST"], port=serverConfig["PORT"], debug=serverConfig["DEBUG"])