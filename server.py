__author__ = 'Ma Haoxiang'

# import
import flask
import db
from configParser import configParser

app = flask.Flask(__name__)

@app.route("/make/<expression>",methods=["GET"])
def makeElem(expression):
    if("->" not in expression):
        return "Expression Error!"
    else:
        dbName = expression.split("->")[0]
        elemName = expression.split("->")[1]
        elemValue = expression.split("->")[2]
        if(elemValue[0] == "\"" and elemValue[-1] == "\""): # distinguish string value and int value
            elemValue = str(elemValue[1:-1])
        else:
            elemValue = int(elemValue)
        result = myDb.createElem(elemName, elemValue, dbName)
        return flask.jsonify(result)


@app.route("/get/<element>",methods=["GET"])
def getElem(element):
    dbName = element.split("->")[0]
    elemName = element.split("->")[1]
    result = myDb.getElem(elemName, dbName)

    return flask.jsonify(result)


@app.route("/searchElem/<dbName>/<string:expression>",methods=["GET"])
def searchElem(dbName,expression):
    result = myDb.searchElem(expression, dbName)
    return flask.jsonify(result)


@app.route("/getAllElem/<dbName>",methods=["GET"])
def searchAllElem(dbName):
    result = myDb.searchAllElem(dbName)
    return flask.jsonify(result)


@app.route("/increaseElem/<element>",methods=["GET"])
def increaseElem(element):
    dbName = element.split("->")[0]
    elemName = element.split("->")[1]
    result = myDb.increaseElem(elemName, dbName)

    return flask.jsonify(result)


@app.route("/decreaseElem/<element>",methods=["GET"])
def decreaseElem(element):
    dbName = element.split("->")[0]
    elemName = element.split("->")[1]
    result = myDb.decreaseElem(elemName, dbName)

    return flask.jsonify(result)


@app.route("/save",methods=["GET"])
def saveDb():
    result = myDb.saveDb()
    return flask.jsonify(result)


if __name__ == '__main__':

    # init the database
    myDb = db.NoSqlDb()

    # load data from file
    myDb.loadDb()

    # init the config parser and read the server config
    confParser = configParser()
    serverConfig = confParser.getServerConfig("server.conf")

    # run the server
    app.run(host=serverConfig["HOST"],port=serverConfig["PORT"],debug=serverConfig["DEBUG"])