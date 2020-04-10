import pymysql.cursors
import pymongo
from schema_graph import *

def migrate():
	mysql_host = 'localhost'
	mysql_port = 3306
	mysql_username = 'dev'
	mysql_password = 'password'

	mongodb_host = 'localhost'
	mongodb_port = 27017

	db_name = 'world'

	mysql_connection = pymysql.connect(host=mysql_host,
	                             port=mysql_port,
	                             user=mysql_username,
	                             password=mysql_password,
	                             db=db_name,
	                             cursorclass=pymysql.cursors.DictCursor)

	mongo_client = pymongo.MongoClient(mongodb_host, mongodb_port)

	graph = Graph(mysql_connection, db_name)
	opts = graph.treeify_options()

	view_schemas(0, 5, opts)


	if input('Would you like to migrate to MongoDB? (y/n)').lower() == 'y':
		schema = opts[int(input('Which schema would you like to use?')) - 1]
		try:
			schema.map(mongo_client)
		finally:
			mysql_connection.close()


def view_schemas(start, end, opts):
	for i in range(start, min(end, len(opts))):
		opt = opts[i]
		print('%d)' % (i + 1))
		print(opt)

	while input('Would you like to preview a schema? (y/n)').lower() == 'y':
		schema = opts[int(input('Which shema would you like to preview?')) - 1]
		filename = input('Enter name of preview file:')
		schema.preview(filename)

	if end < len(opts) and input('Would you like to view more schema options? (y/n)').lower() == 'y':
		view_schemas(end, end + 5, opts)

migrate()