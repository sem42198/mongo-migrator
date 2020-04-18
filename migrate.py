import argparse
import pymysql.cursors
import pymongo
from schema_graph import *

def migrate(args):

	mysql_connection = pymysql.connect(host=args.mysql_host,
	                             port=args.mysql_port,
	                             user=args.mysql_username,
	                             password=args.mysql_password,
	                             db=args.database,
	                             cursorclass=pymysql.cursors.DictCursor)

	mongo_client = pymongo.MongoClient(args.mongodb_host, args.mongodb_port)

	graph = Graph(mysql_connection, args.database)
	opts = graph.get_opts()

	view_schemas(0, 5, opts)


	if input('Would you like to migrate to MongoDB? (y/n) ').lower() == 'y':
		schema = opts[int(input('Which schema would you like to use? ')) - 1]
		try:
			schema.map(mongo_client)
		finally:
			mysql_connection.close()


def view_schemas(start, end, opts):
	for i in range(start, min(end, len(opts))):
		opt = opts[i]
		print('%d)' % (i + 1))
		print(opt)

	if end < len(opts) and input('Would you like to view more schema options? (y/n) ').lower() == 'y':
		return view_schemas(end, end + 5, opts)

	while input('Would you like to preview a schema? (y/n) ').lower() == 'y':
		schema = opts[int(input('Which schema would you like to preview? ')) - 1]
		num_records = int(input('How many records would you like to preview? '))
		filename = input('Enter name of preview file: ')
		schema.preview(filename, num_records)


def main():
	parser = argparse.ArgumentParser()
	parser.add_argument('--mysql-host', default='localhost', help='MySQL database host')
	parser.add_argument('--mysql-port', default=3306, type=int, help='MySQL database port')
	parser.add_argument('--mysql-username', required=True, help='MySQL username')
	parser.add_argument('--mysql-password', required=True, help='MySQL password')
	parser.add_argument('--mongodb-host', default='localhost', help='MongoDB host')
	parser.add_argument('--mongodb-port', default=27017, type=int, help='MongoDB port')
	parser.add_argument('database', help='Name of the MySQL database (MongoDB databse name will match)')
	args = parser.parse_args()
	migrate(args)


if __name__ == '__main__':
    main()