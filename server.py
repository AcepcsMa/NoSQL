__author__ = 'Ma Haoxiang'

# import
import flask
import handler
import db
from configParser import configParser

app = flask.Flask(__name__)

@app.route("/make/<expression>",methods=["GET"])
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


@app.route("/get/<element>",methods=["GET"])
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


@app.route("/addDatabase/<string:dbName>",methods=["GET"])
def addDatabase(dbName):
    myHandler = handler.dbHandler(database)
    result = myHandler.addDatabase(dbName)
    return flask.jsonify(result)



if __name__ == '__main__':
    # init the database
    database = db.NoSqlDb()

    # init the config parser and read the server config
    confParser = configParser()
    serverConfig = confParser.getServerConfig("server.conf")

    # run the server
    app.run(host=serverConfig["HOST"],port=serverConfig["PORT"],debug=serverConfig["DEBUG"])