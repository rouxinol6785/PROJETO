#Base de Dados- LEI 

from flask import Flask , request, jsonify
import time, logging, psycopg2
import uuid

app = Flask(__name__)

StatusCodes= {
    'success': 200,
    'bad_request': 400,
    'internal_error': 500
}


#DATA BASE ACCESS

def db_connection():
    db = psycopg2.connect(
        user = "postgres",
        password = "postgres",
        host = "localhost",
        port = "5432",
        database = "dbproj"
    )
    return db


@app.route("/")
def test():
    return "Bem-vindo!"


@app.route("/dbproj/user/", methods=["POST"])
def registUser():
    connect= db_connection()
    cur = connect.cursor()

    payload = request.get_json()

    if not payload:
        connect.close()
        response = {'status': StatusCodes['bad_request'], 'results': 'Valor não está na payload'}
        return jsonify(response)

    userID = str(uuid.uuid4())   #generate random uuid

    cur.execute("BEGIN")
    cur.execute("LOCK TABLE utilizador IN EXCLUSIVE MODE")   #  Avoid the overhead of multiple row locks on a table
    

    values = (userID, payload["username"], payload["email"], payload["password"] )

    try:    
        cur.execute("INSERT INTO utilizador (userID,username, email, password) VALUES (%s %s %s %s )", values)
        cur.execute("COMMIT")
        response= {'status': StatusCodes['success'], 'results': f'Utilizador introduzido, userID: {payload["userID"]}'}       

    except (Exception, psycopg2.DatabaseError) as error:
        cur.execute("ROLLBACK")
        if "username" in error.pgerror or "authtoken" in error.pgerror:
            erro = "Erro: Utilizador já existe!"
        elif "email" in error.pgerror:
            erro = "Erro: Utilizador ja existe!"
        return jsonify(erro)
    finally:
        if connect is not None:
            connect.close()

    return jsonify(response)



@app.route("/dbproj/user/", methods=["PUT"])
def login():
    connect = db_connection()
    cur= connect.cursor()

    log = request.get_json()
    

    if not log:
        connect.close()
        response = {'status': StatusCodes['bad_request'], 'errors': "Insira dados"}
        return jsonify(response)

    cur.execute("BEGIN")
    cur.execute("LOCK TABLE utilizador IN EXCLUSIVE MODE")  #  Avoid the overhead of multiple row locks on a table

    #verificar se o utilizador existe
    value = (log["username"])

    cur.execute("SELECT username,password FROM utilizador WHERE username = %s", value)
    user = cur.fetchall()
    if not user:
        cur.execute("ROLLBACK")
        erro = "Utilizador não existe!"
        if connect is not None:
            connect.close()
        return jsonify(erro)

    authToken = str(uuid.uuid4())

    if log["password"] != user[0][1]:           # verificar se a password coincide com o utilizador
        cur.execute("ROLLBACK")
        erro = "Utilizador e password não coincidem."
        if connect is not None:
            connect.close()
        return jsonify(erro)

    validar = "UPDATE utilizador SET authtoken = %s WHERE username = %s"
    val= (authToken, log["username"])

    try:
        cur.execute(validar,val)
        cur.execute("COMMIT")
        response = {'status': StatusCodes['success'], 'token': authToken}

    except (Exception, psycopg2.DatabaseError) as error:
        cur.execute("ROLLBACK")
        response = {'status': StatusCodes['bad_request'], 'errors': "Erro de autenticação."}

    finally:
        if connect is not None:
            connect.close()
    
    return jsonify(response)




@app.route("/dbproj/product", methods=['POST'])
def criaProduto():
    connect = db_connection()
    cur = connect.cursor()

    payload = request.get_json()

    if not payload:
        response = {'status': StatusCodes['bad_request'], 'results': 'Valor não está na payload'}
        return jsonify(response)


    prodId = str(uuid.uuid4())  #generate random uuid

    cur.execute("BEGIN")
    cur.execute("LOCK TABLE product IN EXCLUSIVE MODE")      #  Avoid the overhead of multiple row locks on a table

    
    
    ins = "INSERT INTO product (prod_id, price, stock, prod_name, description, espicificacoes) VALUES (%s, %f, %d, %s, %s, %s)"
    values = (prodId,payload["price"],payload["stock"],payload["prod_name"],payload["description"],payload["especificacoes"])

    try:
        cur.execute(ins, values)
        cur.execute("COMMIT")
        response= {'status': StatusCodes['success'], 'results': f'Novo produto inserido: {payload["prodID"]}'}

    except (Exception, psycopg2.DatabaseError) as error:
        cur.execute("ROLLBACK")
        if "prod_name" in error.pgerror or "authtoken" in error.pgerror:
            updte = "UPDATE prod_name SET stock = stock + %d WHERE prodId = %s"
            val = "SELECT stock,prodID FROM product"

            cur.execute(updte,val)
            cur.execute("COMMIT")
            response= {'status': StatusCodes['success'], 'results': f'Produto já existe! Stock atualizado {payload["prodID"]}'}
    
    finally:
        if connect is not None:
            connect.close()

    return jsonify(response)



@app.route("/dbproj/product/<prodId>", methods=["PUT"])
def atualizaProd(prodId):
    connect = db_connection()
    cur = connect.cursor()

    payload = request.get_json()
    if not payload:
        response = {'status': StatusCodes['api_error'], 'results': 'Valor de prodId não está na payload'}
        return jsonify(response)

    cur.execute("BEGIN")
    cur.execute("LOCK TABLE product IN EXCLUSIVE MODE")

    ins = "INSERT INTO product (prod_id, price, stock, prod_name, description, espicificacoes) VALUES (%s, %f, %d, %s, %s, %s)"
    values = (prodId,payload["price"],payload["stock"],payload["prod_name"],payload["description"],payload["especificacoes"])

    update = "UPDATE versao_ant SET description = %s, price = %d"
    val = "SELECT product.price,product.description FROM product,versao_ant WHERE product.prod_id = versao_ant.product_prod_id"

    try:
        cur.execute(ins, values)
        cur.execute(update,val)
        cur.execute("COMMIT")
        response= {'status': StatusCodes['success'], 'results': f'Nova versão do produto inserida\nAntiga versão salva {payload["prodID"]}'}

    except (Exception, psycopg2.DatabaseError) as error:
        cur.execute("ROLLBACK")

    finally:
        if connect is not None:
            connect.close()

    return jsonify(response)


@app.route("/dbproj/product/<prodId>", methods=["GET"])
def consultaProd(prodId):
    connect = db_connection()
    cur = connect.cursor()

    payload = request.get_json()
    if not payload:
        response = {'status': StatusCodes['api_error'], 'results': 'Valor de prodId não está na payload'}
        return jsonify(response)

    cur.execute("BEGIN")
    cur.execute("LOCK TABLE product IN EXCLUSIVE MODE")

    select = "SELECT *FROM product where prod_id = %s"
    val = (prodId)

    try:
        cur.execute(select, val)
        cur.execute("COMMIT")
        response= {'status': StatusCodes['success']}
    
    except (Exception, psycopg2.DatabaseError) as error:
        cur.execute("ROLLBACK")
        if "prod_id" not in error.pgerror or "authtoken" not in error.pgerror:
            erro = "Erro: Produto não existe!"
        return jsonify(erro)
        
    finally:
        if connect is not None:
            connect.close()

    return jsonify(response)



@app.route("/dbproj/campanha", methods=['POST'])
def criaCampanha():
    connect = db_connection()
    cur = connect.cursor()

    payload = request.get_json()
    if not payload:
        response = {'status': StatusCodes['api_error'], 'results': 'Valor de prodId não está na payload'}
        return jsonify(response)


    campId = str(uuid.uuid4())  #generate random uuid
    cur.execute("BEGIN")
    cur.execute("LOCK TABLE product IN EXCLUSIVE MODE")

    ins = "INSERT INTO campanha(num_coupon, perc_desc, camp_id, val_inicio, val_fim) VALUES (%d,%f,%s,%s,%s)"
    values = (payload["num_coupon"],payload["perc_desc"],campId,payload["val_inicio"],payload["val_fim"])

    try:
        cur.execute(ins,values)
        cur.execute("COMMIT")
        response= {'status': StatusCodes['success'], 'results': f'Nova campanha iniciada {payload["campId"]}'}

    except (Exception, psycopg2.DatabaseError) as error:
        cur.execute("ROLLBACK")
        #if SELECT * FROM campanha WHERE val_fim > TRUNC(SYSDATE)

    finally:
        if connect is not None:
            connect.close()

    return jsonify(response)





@app.route("/dbproj/rating/{produtctid}", methods=['POST'])
def rating(prodId):
    connect = db_connection()
    cur = connect.cursor()

    payload = request.get_json()
    if not payload:
        response = {'status': StatusCodes['internal_error'], 'results': 'erro no rating'}#ve la isto so
        return jsonify(response)

    cur.execute("BEGIN")
    cur.execute("LOCK TABLE rating IN EXCLUSIVE MODE")
    values = (prodId, payload["rate"], payload["coment"])
    try:
        cur.execute("INSERT INTO rating (product_prod_Id, rate, coment) VALUES(%s,%d,%s)", values)
        cur.execute("COMMIT")
        response = {'status': StatusCodes['success'], 'results': f'rating introduzido, prodId: {payload["prodId"]}'}
    except (Exception, psycopg2.DatabaseError):
        if 1 > rate > 5:
            response = {'status': StatusCodes['bad_request'], 'results': 'Valor do rating nao esta entre os permitidos'}
            cur.execute("ROLLBACK")
    finally:
        if connect is not None:
            connect.close()

    return jsonify(response)



@app.route("/dbproj/questions/{produtct id}", methods=['POST'])
def q_n_a(prodId):
    connect = db_connection()
    cur = connect.cursor()
    payload = request.get_json()
    questId = str(uuid.uuid4())
    if not payload:
        response = {'status': StatusCodes['internal_error'],'results': 'erro question'}  #ve isto tb
        return jsonify(response)
    cur.execute("BEGIN")
    cur.execute("LOCK TABLE notification_quest_answ_order_quantidade IN EXCLUSIVE MODE")
    values = (prodId, questId,payload['text'])
    try:
        cur.execute("INSERT INTO notification_quest_answ_order_quantidade (order_quantidade_product_prod_id, quest_answ_quest_id, quest_answ_text) VALUES(%s,%d,%s)", values)
        cur.execute("COMMIT")
        response = {'status': StatusCodes['success'], 'results': f'pergunta introduzida, quest_answ_quest_id: {payload[questId]}'}
    except (Exception, psycopg2.DatabaseError) as error:
       if questId in error.pgerror:
            response = {'status': StatusCodes['bad_request'], 'results': 'Valor do rating não está entre os permitidos'}
            cur.execute("ROLLBACK")
    finally:
        if connect is not None:
            connect.close()




if __name__ == "__main__":
    app.run(host="localhost", port="8080", debug=True, threaded=True)