import flask
import db

app = flask.Flask(__name__)

@app.route("/make/<expression>",methods=["GET"])
def makeElem(expression):
    if("->" not in expression):
        return "Expression Error!"
    else:
        elemName = expression.split("->")[0]
        elemValue = expression.split("->")[1]
        result = myDb.createElem(elemName,elemValue)
        if(result == myDb.CREATE_ELEM_SUCCESS):
            return "Make Element Success"
        elif(result == myDb.ELEM_ALREADY_EXIST):
            return "Element Already Exists"
        elif(result == myDb.ELEM_TYPE_ERROR):
            return "Element Type Error"


@app.route("/get/<elemName>",methods=["GET"])
def getElem(elemName):
    result = myDb.getElem(elemName)
    if(result[0] == myDb.ELEM_NOT_EXIST):
        return None
    elif(result[0] == myDb.GET_ELEM_SUCCESS):
        return result[1]
    elif(result[0] == myDb.ELEM_TYPE_ERROR):
        return None


@app.route("/searchElem/<string:expression>",methods=["GET"])
def searchElem(expression):
    result = myDb.searchElem(expression)
    return flask.jsonify(result)


if __name__ == '__main__':
    myDb = db.NoSqlDb()
    app.run(host="localhost",port=8888,debug=True)