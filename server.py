__author__ = 'Ma Haoxiang'

# import
import flask
import db
from configParser import configParser
import timer
import ttlTimer
from response import responseCode
from dbHandler import dbHandler
from elemHandler import elemHandler
from listHandler import listHandler
from hashHandler import hashHandler
from setHandler import setHandler
from zsetHandler import zsetHandler

app = flask.Flask(__name__)

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

@app.route("/updateElem",methods=["POST"])
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

@app.route("/increaseElem/<string:dbName>/<string:elemName>",methods=["GET"])
def increaseElem(dbName, elemName):
    myHandler = elemHandler(database)
    result = myHandler.increaseElem(dbName, elemName)
    return flask.jsonify(result)

@app.route("/decreaseElem/<string:dbName>/<string:elemName>",methods=["GET"])
def decreaseElem(dbName, elemName):
    myHandler = elemHandler(database)
    result = myHandler.decreaseElem(dbName, elemName)
    return flask.jsonify(result)

@app.route("/deleteElem/<string:dbName>/<string:elemName>",methods=["GET"])
def deleteElem(dbName, elemName):
    myHandler = elemHandler(database)
    result = myHandler.deleteElem(dbName, elemName)
    return flask.jsonify(result)

@app.route("/setElemTTL/<string:dbName>/<string:elemName>/<int:ttl>",methods=["GET"])
def setElemTTL(dbName, elemName, ttl):
    myHandler = elemHandler(database)
    result = myHandler.setTTL(dbName, elemName, ttl)
    return flask.jsonify(result)

@app.route("/clearElemTTL/<string:dbName>/<string:elemName>",methods=["GET"])
def clearElemTTL(dbName, elemName):
    myHandler = elemHandler(database)
    result = myHandler.clearTTL(dbName, elemName)
    return flask.jsonify(result)

@app.route("/makeList/<string:dbName>/<string:listName>",methods=["GET"])
def makeList(dbName, listName):
    myHandler = listHandler(database)
    result = myHandler.createList(dbName, listName)
    return flask.jsonify(result)

@app.route("/getList/<string:dbName>/<string:listName>",methods=["GET"])
def getList(dbName, listName):
    myHandler = listHandler(database)
    result = myHandler.getList(dbName, listName)
    return flask.jsonify(result)

@app.route("/insertList",methods=["POST"])
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

@app.route("/deleteList/<string:dbName>/<string:listName>",methods=["GET"])
def deleteList(dbName, listName):
    myHandler = listHandler(database)
    result = myHandler.deleteList(dbName, listName)
    return flask.jsonify(result)

@app.route("/rmFromList",methods=["POST"])
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

@app.route("/clearList/<dbName>/<listName>",methods=["GET"])
def clearList(dbName, listName):
    myHandler = listHandler(database)
    result = myHandler.clearList(dbName, listName)
    return flask.jsonify(result)

@app.route("/mergeLists",methods=["POST"])
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

@app.route("/setListTTL/<string:dbName>/<string:listName>/<int:ttl>",methods=["GET"])
def setListTTL(dbName, listName, ttl):
    myHandler = listHandler(database)
    result = myHandler.setTTL(dbName, listName, ttl)
    return flask.jsonify(result)

@app.route("/clearListTTL/<string:dbName>/<string:listName>",methods=["GET"])
def clearListTTL(dbName, listName):
    myHandler = listHandler(database)
    result = myHandler.clearTTL(dbName, listName)
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

@app.route("/insertHash",methods=["POST"])
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

@app.route("/deleteHash/<dbName>/<hashName>",methods=["GET"])
def deleteHash(dbName, hashName):
    myHandler = hashHandler(database)
    result = myHandler.deleteHash(dbName, hashName)
    return flask.jsonify(result)

@app.route("/rmFromHash",methods=["POST"])
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

@app.route("/replaceHash",methods=["POST"])
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

@app.route("/mergeHashs",methods=["POST"])
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

@app.route("/setHashTTL/<string:dbName>/<string:hashName>/<int:ttl>",methods=["GET"])
def setHashTTL(dbName, hashName, ttl):
    myHandler = hashHandler(database)
    result = myHandler.setTTL(dbName, hashName, ttl)
    return flask.jsonify(result)

@app.route("/clearHashTTL/<string:dbName>/<string:hashName>",methods=["GET"])
def clearHashTTL(dbName, hashName):
    myHandler = hashHandler(database)
    result = myHandler.clearTTL(dbName, hashName)
    return flask.jsonify(result)

@app.route("/makeSet/<string:dbName>/<string:setName>",methods=["GET"])
def makeSet(dbName, setName):
    myHandler = setHandler(database)
    result = myHandler.createSet(dbName, setName)
    return flask.jsonify(result)

@app.route("/getSet/<string:dbName>/<string:setName>",methods=["GET"])
def getSet(dbName, setName):
    myHandler = setHandler(database)
    result = myHandler.getSet(dbName, setName)
    return flask.jsonify(result)

@app.route("/insertSet",methods=["POST"])
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

@app.route("/rmFromSet",methods=["POST"])
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

@app.route("/clearSet/<string:dbName>/<string:setName>",methods=["GET"])
def clearSet(dbName, setName):
    myHandler = setHandler(database)
    result = myHandler.clearSet(dbName, setName)
    return flask.jsonify(result)

@app.route("/deleteSet/<string:dbName>/<string:setName>",methods=["GET"])
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

@app.route("/unionSet",methods=["POST"])
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

@app.route("/intersectSet",methods=["POST"])
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

@app.route("/diffSet",methods=["POST"])
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

@app.route("/replaceSet",methods=["POST"])
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

@app.route("/setSetTTL/<string:dbName>/<string:setName>/<int:ttl>",methods=["GET"])
def setSetTTL(dbName, setName, ttl):
    myHandler = setHandler(database)
    result = myHandler.setTTL(dbName, setName, ttl)
    return flask.jsonify(result)

@app.route("/clearSetTTL/<string:dbName>/<string:setName>",methods=["GET"])
def clearSetTTL(dbName, setName):
    myHandler = setHandler(database)
    result = myHandler.clearTTL(dbName, setName)
    return flask.jsonify(result)

@app.route("/showTTL/<string:dbName>/<string:dataType>/<string:keyName>",methods=["GET"])
def showTTL(dbName, dataType, keyName):
    if(dataType == "ELEM"):
        myHandler = elemHandler(database)
    elif(dataType == "LIST"):
        myHandler = listHandler(database)
    elif(dataType == "HASH"):
        myHandler = hashHandler(database)
    elif(dataType == "SET"):
        myHandler = setHandler(database)
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

@app.route("/makeZSet/<string:dbName>/<string:zsetName>",methods=["GET"])
def makeZSet(dbName, zsetName):
    myHandler = zsetHandler(database)
    result = myHandler.createZSet(dbName, zsetName)
    return flask.jsonify(result)

@app.route("/getZSet/<string:dbName>/<string:zsetName>",methods=["GET"])
def getZSet(dbName, zsetName):
    myHandler = zsetHandler(database)
    result = myHandler.getZSet(dbName, zsetName)
    return flask.jsonify(result)

@app.route("/insertZSet",methods=["POST"])
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

@app.route("/rmFromZSet",methods=["POST"])
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

@app.route("/clearZSet/<string:dbName>/<string:zsetName>",methods=["GET"])
def clearZSet(dbName, zsetName):
    myHandler = zsetHandler(database)
    result = myHandler.clearZSet(dbName, zsetName)
    return flask.jsonify(result)

@app.route("/deleteZSet/<string:dbName>/<string:zsetName>",methods=["GET"])
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

@app.route("/getValues",methods=["POST"])
def getValues():
    myHandler = zsetHandler(database)
    try:
        dbName = flask.request.json["dbName"]
        zsetName = flask.request.json["zsetName"]
        start = flask.request.json["start"]
        end = flask.request.json["end"]
    except:
        dbName = zsetName = start = end = None
    result = myHandler.getValues(dbName, zsetName, start, end)
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

@app.route("/rmFromZSetByScore",methods=["POST"])
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

@app.route("/addDatabase/<string:dbName>",methods=["GET"])
def addDatabase(dbName):
    myHandler = dbHandler(database)
    result = myHandler.addDatabase(dbName)
    return flask.jsonify(result)

@app.route("/getAllDatabase",methods=["GET"])
def getAllDatabase():
    myHandler = dbHandler(database)
    result = myHandler.getAllDatabase()
    return flask.jsonify(result)

@app.route("/delDatabase/<string:dbName>",methods=["GET"])
def delDatabase(dbName):
    myHandler = dbHandler(database)
    result = myHandler.delDatabase(dbName)
    return flask.jsonify(result)

@app.route("/save",methods=["GET"])
def saveDb():
    myHandler = dbHandler(database)
    result = myHandler.saveDb()
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
    saveTimer = timer.timer(databaseList, serverConfig["SAVE_INTERVAL"])
    saveTimer.start()

    # init the ttl timer
    TTLTimer = ttlTimer.ttlTimer(databaseList, serverConfig["TTL_CHECK_INTERVAL"])

    # run the server
    app.run(host=serverConfig["HOST"], port=serverConfig["PORT"], debug=serverConfig["DEBUG"])
