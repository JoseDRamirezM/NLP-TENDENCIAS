import mysql.connector

# Decouple
from decouple import config

class Database():
    def __init__(self):
        '''
        Configuración de la base de datos MySQL
        '''
        print('[ETL] Inicializando base de datos...')

        # Config session
        self.config = {
            'user': config('MYSQL_USER'),
            'host': config('MYSQL_HOST'),
            'password': config('MYSQL_PASSWORD'),
            'database': config('MYSQL_DATABASE'),
            'connect_timeout': 60
        }
        self.mysql = None

    #Config Access
    def login_database(self) -> 'mysql.connector.cursor':
        '''
        Iniciamos una conexion a la base de datos.
        '''
        try:
            print('login_database')
            self.mysql = mysql.connector.connect(**self.config)
            return self.mysql.cursor()
        except mysql.connector.Error as error:
            print('Login database Error: ' + str(error))
    
    def logout_database(self):
        '''
        Cerramos la conexion a la base de datos.
        '''
        if self.mysql:
            self.mysql.close()

    # SELECTS
    def get_products(self) -> list:
        '''
        Obtenemos los identificadores de producto de la base de datos.
        '''
        try:
            ncursor = self.login_database()
            query = "SELECT k_products FROM Products"
            ncursor.execute(query)
            return ncursor.fetchall(), True
        except mysql.connector.Error as error:
            print('Error get_products: ' + str(error))
            return [[], False]
        finally:
            self.logout_database()
        

    # INSERTS
    def insert_comments(self, comment: dict):
        '''
        Insertamos los comentarios y el resultado del NLP en la base de datos.
        Args:
            - k_product: str
            - nlp_score: float
            - comment_text: string
        '''
        try:
            # Debug LOG
            print(f"[DEBUG]|DB - insert_comment: {comment['k_product'], comment['nlp_score'], comment['comment_text']}")
            if comment:
                ncursor = self.login_database()
                query = "INSERT INTO Comments (k_products, nlp_score, comment_text) VALUES (%s, %s, %s)"
                ncursor.execute(query, (comment['k_product'], comment['nlp_score'], comment['comment_text']))
                self.mysql.commit()
                print(f"Comentario para producto: {comment['k_product']} añadido satisfactoriamente.")
                return True
        except mysql.connector.Error as error:
            print('Error : insert_comments' + str(error))
            return False
        finally:
            self.logout_database()