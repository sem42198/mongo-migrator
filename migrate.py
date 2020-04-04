import pymysql.cursors
from schema import *

host = 'localhost'
db = 'test'
username = 'dev'
password = 'password'

connection = pymysql.connect(host=host,
                             user=username,
                             password=password,
                             db=db,
                             cursorclass=pymysql.cursors.DictCursor)

schema = Schema(connection)
tbl = Table('dogs', 'name')
tbl.add_one_to_many_child('toys', 'id', 'dog')
schema.add_table(tbl)
try:
	schema.map()
finally:
	connection.close()
