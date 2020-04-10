from schema import *
import copy

duplication_weight = 1
data_loss_weight = 10
reference_weight = 5

TABLES_LIST_SQL = "SELECT TABLE_NAME FROM information_schema.tables WHERE TABLE_SCHEMA = %s;"

PK_SQL = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s \
AND COLUMN_KEY = 'PRI';"

TABLE_SIZE_SQL = "SELECT DATA_LENGTH FROM information_schema.tables WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s;"

NUM_ROWS_SQL = "SELECT COUNT(*) AS NUM_ROWS FROM %s;"

FKEYS_SQL = "SELECT COLUMN_NAME, REFERENCED_TABLE_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE \
REFERENCED_TABLE_SCHEMA = %s AND TABLE_NAME = %s;"

DISTINCT_FK_COUNT_SQL = "SELECT COUNT(DISTINCT(%s)) DISTINCT_VALS FROM %s;"

class Graph:

	def __init__(self, connection, db_name):
		self.db_name = db_name
		self.connection = connection
		self.nodes = {}
		with self.connection.cursor() as cursor:
			cursor.execute(TABLES_LIST_SQL, (self.db_name,))
			tables = cursor.fetchall()
			for result in tables:
				table = result['TABLE_NAME']
				cursor.execute(PK_SQL, (self.db_name, table))
				pk = cursor.fetchone()['COLUMN_NAME']
				cursor.execute(TABLE_SIZE_SQL, (self.db_name, table))
				tbl_size = cursor.fetchone()['DATA_LENGTH']
				cursor.execute(NUM_ROWS_SQL % table)
				num_rows = cursor.fetchone()['NUM_ROWS']
				rowsize = tbl_size / num_rows
				Node(self, table, pk, rowsize, num_rows)
			for result in tables:
				table = result['TABLE_NAME']
				node = self.nodes[table]
				cursor.execute(FKEYS_SQL, (self.db_name, table))
				for result in cursor.fetchall():
					cursor.execute(DISTINCT_FK_COUNT_SQL % (result['COLUMN_NAME'], table))
					distinct_vals = cursor.fetchone()['DISTINCT_VALS']
					refed_table = self.nodes[result['REFERENCED_TABLE_NAME']]
					node.add_fkey(result['COLUMN_NAME'], refed_table, distinct_vals)


	def treeify_options(self):
		opts = [self.copy()]
		return [opt.make_schema() for opt in opts]

	def copy(self):
		cp = Graph(self.connection, self.db_name)
		cp.nodes = copy.deepcopy(self.nodes)
		return cp

	def make_schema(self):
		schema = Schema(self)
		for node in self.root_nodes():
			schema.add_table(node.make_table())
		return schema

	def root_nodes(self):
		roots = set()
		for node in self.nodes.values():
			if len(node.parent_edges) == 0:
				roots.add(node)
		return roots

	def __str__(self):
		s = ''
		for node in self.nodes.values():
			s += str(node) + '\n'
		return s





class Node:

	def __init__(self, graph, table, pk, rowsize, num_rows):
		self.name = table
		self.pk = pk
		self.rowsize = rowsize
		self.num_rows = num_rows
		self.child_edges = set()
		self.parent_edges = set()
		graph.nodes[self.name] = self

	def add_fkey(self, fk_col, referenced_table, distinct_fk_count):
		Edge(referenced_table, self, fk_col, self.name, distinct_fk_count)

	def make_table(self):
		table = Table(self.name, self.pk)
		self._embed_children(table)
		return table

	def _embed_children(self, table):
		for edge in self.child_edges:
			node = edge.to_node
			child = table.add_one_to_many_child(node.name, node.pk, edge.fkey_col)
			node._embed_children(child)


	def __str__(self):
		string = self.name + ":"
		for edge in self.child_edges:
			string += " " + str(edge)
		return string

	# def estimate_duplication_cost(self, visited = set()):
	# 	if self in visited:
	# 		return 0
	# 	visited.add(self)
	# 	total = self.rowsize * self.num_rows
	# 	for edge in self.child_edges:
	# 		child = edge.to_node
	# 		total += child.duplication_cost()
	# 	return total * duplication_weight

	# def duplicate(self):





class Edge:

	def __init__(self, from_node, to_node, fkey_col, fkey_table, distinct_fk_count):
		self.from_node = from_node
		self.to_node = to_node
		self.fkey_col = fkey_col
		self.fkey_table = fkey_table
		self.distinct_fk_count = distinct_fk_count
		self.reversed = False
		self.reference = False
		self.from_node.child_edges.add(self)
		self.to_node.parent_edges.add(self)

	def __str__(self):
		return self.to_node.name

	# def estimate_reversal_cost(self):
	# 	duplication = max(self.to_node.num_rows - self.from_node.num_rows, 0)
	# 	loss = self.from_node.num_rows - self.distinct_fk_count
	# 	return duplication * duplication_weight + loss * data_loss_weight

	# def reverse(self):
	# 	self.reversed = True
	# 	self.from_node.child_edges.remove(self)
	# 	self.from_node.parent_edges.add(self)
	# 	self.to_node.parent_edges.remove(self)
	# 	self.to_node.child_edges.add(self)

	# def make_reference(self):
	# 	if len(self.to_node.parent_edges) > 0:
	# 		self.reference = True
	# 		return True
	# 	else:
	# 		return False
