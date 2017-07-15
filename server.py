__author__ = 'Ma Haoxiang'

# import
import flask
import handler
import db
from configParser import configParser
import timer

app = flask.Flask(__name__)

@app.route("/makeElem/<expression>",methods=["GET"])
def makeElem(expression):
    if("->" not in expression):
        msg = {
            "msg":"Wrong Expression",
            "typeCode":None,
            "data":None
        }
        return flask.jsonify(msg)
    else:
        myHandler = handler.dbHandler(database)
        dbName = expression.split("->")[0]
        elemName = expression.split("->")[1]
        elemValue = expression.split("->")[2]
        if(elemValue[0] == "\"" and elemValue[-1] == "\""): # distinguish string value and int value
            elemValue = str(elemValue[1:-1])
        else:
            elemValue = int(elemValue)
        result = myHandler.createElem(elemName, elemValue, dbName)
        return flask.jsonify(result)

@app.route("/getElem/<element>",methods=["GET"])
def getElem(element):
    myHandler = handler.dbHandler(database)
    dbName = element.split("->")[0]
    elemName = element.split("->")[1]
    result = myHandler.getElem(elemName, dbName)
    return flask.jsonify(result)

@app.route("/searchElem/<dbName>/<string:expression>",methods=["GET"])
def searchElem(dbName,expression):
    myHandler = handler.dbHandler(database)
    result = myHandler.searchElem(expression, dbName)
    return flask.jsonify(result)

@app.route("/getAllElem/<dbName>",methods=["GET"])
def searchAllElem(dbName):
    myHandler = handler.dbHandler(database)
    result = myHandler.searchAllElem(dbName)
    return flask.jsonify(result)

@app.route("/increaseElem/<element>",methods=["GET"])
def increaseElem(element):
    myHandler = handler.dbHandler(database)
    dbName = element.split("->")[0]
    elemName = element.split("->")[1]
    result = myHandler.increaseElem(elemName, dbName)
    return flask.jsonify(result)

@app.route("/decreaseElem/<element>",methods=["GET"])
def decreaseElem(element):
    myHandler = handler.dbHandler(database)
    dbName = element.split("->")[0]
    elemName = element.split("->")[1]
    result = myHandler.decreaseElem(elemName, dbName)
    return flask.jsonify(result)

@app.route("/save",methods=["GET"])
def saveDb():
    myHandler = handler.dbHandler(database)
    result = myHandler.saveDb()
    return flask.jsonify(result)

@app.route("/deleteElem/<string:element>",methods=["GET"])
def deleteElem(element):
    dbName = element.split("->")[0]
    elemName = element.split("->")[1]
    myHandler = handler.dbHandler(database)
    result = myHandler.deleteElem(elemName, dbName)
    return flask.jsonify(result)

@app.route("/makeList/<string:expression>",methods=["GET"])
def makeList(expression):
    myHandler = handler.dbHandler(database)
    dbName = expression.split("->")[0]
    listName = expression.split("->")[1]
    result = myHandler.createList(listName, dbName)
    return flask.jsonify(result)

@app.route("/getList/<string:expression>",methods=["GET"])
def getList(expression):
    myHandler = handler.dbHandler(database)
    dbName = expression.split("->")[0]
    listName = expression.split("->")[1]
    result = myHandler.getList(listName, dbName)
    return flask.jsonify(result)

@app.route("/insertList/<string:expression>",methods=["GET"])
def insertList(expression):
    myHandler = handler.dbHandler(database)
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
    myHandler = handler.dbHandler(database)
    dbName = expression.split("->")[0]
    listName = expression.split("->")[1]
    result = myHandler.deleteList(listName, dbName)
    return flask.jsonify(result)

@app.route("/rmFromList/<string:expression>",methods=["GET"])
def rmFromList(expression):
    myHandler = handler.dbHandler(database)
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
    myHandler = handler.dbHandler(database)
    result = myHandler.searchList(dbName, expression)
    return flask.jsonify(result)

@app.route("/searchAllList/<string:dbName>",methods=["GET"])
def searchAllList(dbName):
    myHandler = handler.dbHandler(database)
    result = myHandler.searchAllList(dbName)
    return flask.jsonify(result)

@app.route("/addDatabase/<string:dbName>",methods=["GET"])
def addDatabase(dbName):
    myHandler = handler.dbHandler(database)
    result = myHandler.addDatabase(dbName)
    return flask.jsonify(result)

@app.route("/getAllDatabase",methods=["GET"])
def getAllDatabase():
    myHandler = handler.dbHandler(database)
    result = myHandler.getAllDatabase()
    return flask.jsonify(result)

@app.route("/delDatabase/<string:dbName>",methods=["GET"])
def delDatabase(dbName):
    myHandler = handler.dbHandler(database)
    result = myHandler.delDatabase(dbName)
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