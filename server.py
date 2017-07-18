__author__ = 'Ma Haoxiang'

# import
import json
import flask
import db
from configParser import configParser
import timer
from dbHandler import dbHandler
from elemHandler import elemHandler
from listHandler import listHandler
from hashHandler import hashHandler

app = flask.Flask(__name__)

@app.route("/makeElem",methods=["POST"])
def makeElem():
    myHandler = elemHandler(database)
    try:
        dbName = flask.request.json["dbName"]
        elemName = flask.request.json["elemName"]
        elemValue = flask.request.json["elemValue"]
    except:
        dbName = None
        elemName = None
        elemValue = None
    result = myHandler.createElem(elemName, elemValue, dbName)
    return flask.jsonify(result)

@app.route("/getElem/<expression>",methods=["GET"])
def getElem(expression):
    myHandler = elemHandler(database)
    dbName = expression.split("->")[0]
    elemName = expression.split("->")[1]
    result = myHandler.getElem(elemName, dbName)
    return flask.jsonify(result)

@app.route("/searchElem/<dbName>/<string:expression>",methods=["GET"])
def searchElem(dbName,expression):
    myHandler = elemHandler(database)
    result = myHandler.searchElem(expression, dbName)
    return flask.jsonify(result)

@app.route("/getAllElem/<dbName>",methods=["GET"])
def searchAllElem(dbName):
    myHandler = elemHandler(database)
    result = myHandler.searchAllElem(dbName)
    return flask.jsonify(result)

@app.route("/increaseElem/<element>",methods=["GET"])
def increaseElem(element):
    myHandler = elemHandler(database)
    dbName = element.split("->")[0]
    elemName = element.split("->")[1]
    result = myHandler.increaseElem(elemName, dbName)
    return flask.jsonify(result)

@app.route("/decreaseElem/<element>",methods=["GET"])
def decreaseElem(element):
    myHandler = elemHandler(database)
    dbName = element.split("->")[0]
    elemName = element.split("->")[1]
    result = myHandler.decreaseElem(elemName, dbName)
    return flask.jsonify(result)

@app.route("/deleteElem/<string:element>",methods=["GET"])
def deleteElem(element):
    myHandler = elemHandler(database)
    dbName = element.split("->")[0]
    elemName = element.split("->")[1]
    result = myHandler.deleteElem(elemName, dbName)
    return flask.jsonify(result)

@app.route("/makeList/<string:expression>",methods=["GET"])
def makeList(expression):
    myHandler = listHandler(database)
    dbName = expression.split("->")[0]
    listName = expression.split("->")[1]
    result = myHandler.createList(listName, dbName)
    return flask.jsonify(result)

@app.route("/getList/<string:expression>",methods=["GET"])
def getList(expression):
    myHandler = listHandler(database)
    dbName = expression.split("->")[0]
    listName = expression.split("->")[1]
    result = myHandler.getList(listName, dbName)
    return flask.jsonify(result)

@app.route("/insertList/<string:expression>",methods=["GET"])
def insertList(expression):
    myHandler = listHandler(database)
    dbName = expression.split("->")[0]
    listName = expression.split("->")[1]
    value = expression.split("->")[2]
    if (value[0] == "\"" and value[-1] == "\""):  # distinguish string value and int value
        value = str(value[1:-1])
    else:
        value = int(value)
    result = myHandler.insertList(listName, value, dbName)
    return flask.jsonify(result)

@app.route("/deleteList/<string:expression>",methods=["GET"])
def deleteList(expression):
    myHandler = listHandler(database)
    dbName = expression.split("->")[0]
    listName = expression.split("->")[1]
    result = myHandler.deleteList(listName, dbName)
    return flask.jsonify(result)

@app.route("/rmFromList/<string:expression>",methods=["GET"])
def rmFromList(expression):
    myHandler = listHandler(database)
    dbName = expression.split("->")[0]
    listName = expression.split("->")[1]
    value = expression.split("->")[2]
    if (value[0] == "\"" and value[-1] == "\""):  # distinguish string value and int value
        value = str(value[1:-1])
    else:
        value = int(value)
    result = myHandler.rmFromList(dbName, listName, value)
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

@app.route("/makeHash/<string:expression>",methods=["POST"])
def makeHash(expression):
    myHandler = hashHandler(database)
    dbName = expression.split("->")[0]
    hashName = expression.split("->")[1]
    hashValue = json.loads(flask.request.form["hashValue"])
    result = myHandler.createHash(dbName, hashName, hashValue)
    return flask.jsonify(result)

@app.route("/getHash/<string:expression>",methods=["GET"])
def getHash(expression):
    myHandler = hashHandler(database)
    dbName = expression.split("->")[0]
    hashName = expression.split("->")[1]
    result = myHandler.getHash(dbName, hashName)
    return flask.jsonify(result)

@app.route("/insertHash/<string:expression>",methods=["POST"])
def insertHash(expression):
    myHandler = hashHandler(database)
    dbName = expression.split("->")[0]
    hashName = expression.split("->")[1]
    print (flask.request.json)
    try:
        keyName = flask.request.json["keyName"]
        value = flask.request.json["value"]
    except:
        keyName = None
        value = None
    result = myHandler.insertHash(dbName, hashName, keyName, value)
    return flask.jsonify(result)

@app.route("/isHashKeyExist/<dbName>/<hashName>/<keyName>",methods=["GET"])
def isHashKeyExist(dbName, hashName, keyName):
    myHandler = hashHandler(database)
    result = myHandler.isKeyExist(dbName, hashName, keyName)
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
    database = db.NoSqlDb(serverConfig)

    #init the save timer
    saveTimer = timer.timer(database,serverConfig["SAVE_INTERVAL"])
    saveTimer.start()

    # run the server
    app.run(host=serverConfig["HOST"],port=serverConfig["PORT"],debug=serverConfig["DEBUG"])