import pymysql.cursors
from schema_graph import *

host = 'localhost'
db = 'test'
username = 'dev'
password = 'password'

connection = pymysql.connect(host=host,
                             user=username,
                             password=password,
                             db=db,
                             cursorclass=pymysql.cursors.DictCursor)

graph = Graph(connection, db)
schema = graph.make_schema()
try:
	schema.map()
finally:
	connection.close()
