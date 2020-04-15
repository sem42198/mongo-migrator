from schema import *
import copy

duplication_cost = 1
data_loss_cost = 50
reference_cost = 20
root_node_cost = 20

TABLES_LIST_SQL = "SELECT TABLE_NAME FROM information_schema.tables WHERE TABLE_SCHEMA = %s AND TABLE_TYPE != 'VIEW';"

PK_SQL = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s \
AND COLUMN_KEY = 'PRI';"

TABLE_SIZE_SQL = "SELECT DATA_LENGTH FROM information_schema.tables WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s;"

NUM_ROWS_SQL = "SELECT COUNT(*) AS NUM_ROWS FROM %s;"

FKEYS_SQL = "SELECT COLUMN_NAME, REFERENCED_TABLE_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE \
REFERENCED_TABLE_SCHEMA = %s AND TABLE_NAME = %s;"

DISTINCT_FK_COUNT_SQL = "SELECT COUNT(DISTINCT(%s)) DISTINCT_VALS FROM %s;"

NULL_FK_COUNT = "SELECT COUNT(*) AS NULL_COUNT FROM %s WHERE %s IS NULL;"

class Graph:

    def __init__(self, connection, db_name, current_id=0):
        self.db_name = db_name
        self.connection = connection
        self.nodes = {}
        self.edges = {}
        self.current_id = current_id
        if current_id == 0:
            self.init_tables()
        

    def init_tables(self):
        nodes = {}
        with self.connection.cursor() as cursor:
            cursor.execute(TABLES_LIST_SQL, (self.db_name,))
            tables = cursor.fetchall()
            for result in tables:
                table = result['TABLE_NAME']
                cursor.execute(PK_SQL, (self.db_name, table))
                pk = (cursor.fetchone() or {}).get('COLUMN_NAME')
                cursor.execute(TABLE_SIZE_SQL, (self.db_name, table))
                tbl_size = cursor.fetchone()['DATA_LENGTH']
                cursor.execute(NUM_ROWS_SQL % table)
                num_rows = cursor.fetchone()['NUM_ROWS']
                rowsize = (tbl_size or num_rows * 32) / num_rows
                nodes[table] = Node(self, table, pk, rowsize, num_rows)
            for result in tables:
                table = result['TABLE_NAME']
                node = nodes[table]
                cursor.execute(FKEYS_SQL, (self.db_name, table))
                for result in cursor.fetchall():
                    cursor.execute(DISTINCT_FK_COUNT_SQL % (result['COLUMN_NAME'], table))
                    distinct_vals = cursor.fetchone()['DISTINCT_VALS']
                    refed_table = nodes[result['REFERENCED_TABLE_NAME']]
                    cursor.execute(NULL_FK_COUNT % (table, result['COLUMN_NAME']))
                    null_fk_count = cursor.fetchone()['NULL_COUNT']
                    node.add_fkey(self, result['COLUMN_NAME'], refed_table, distinct_vals, null_fk_count)


    def generate(self, graphs, steps):
        multiparent_node = self.get_multi_parent_node()
        if multiparent_node != None:

            # Try duplicating the node
            copy = self.copy_graph()
            copy.nodes[multiparent_node.id].duplicate(copy)
            graphs.append([copy, steps + ['Duplicated Node: %s' % multiparent_node.name]])
            # copy.treeify_options(tree_opts, steps + ['Duplicated Node: %s' % multiparent_node.name])

            # Try reversing an edge
            for edge in multiparent_node.parent_edges:
                if not edge.reversed:
                    copy = self.copy_graph()
                    copy.edges[edge.id].reverse()
                    graphs.append([copy, steps + ['Reversed Edge: %s' % copy.edges[edge.id]]])
        else:
            cycle = self.get_cycle()
            for edge in cycle:
                if not edge.reversed:
                    copy = self.copy_graph()
                    copy.edges[edge.id].reverse()
                    graphs.append([copy, steps + ['Reversed Edge: %s' % copy.edges[edge.id]]])
            # print('Found cycle:')
            # print(cycle)



    def get_opts(self):
        tree_opts = []
        graphs = [[self, []]]
        num_edges = len(self.edges)
        while not len(graphs) == 0:
            curr, steps = graphs.pop()
            if curr.is_valid():
                tree_opts.append([curr, steps])
            elif len(steps) < num_edges * (2/3):
                curr.generate(graphs, steps)
        index = [[tree_opts[i][0].heuristic(), i] for i in range(len(tree_opts))]
        index.sort()
        # for ind in index:
        #     print(ind[0])
        #     steps = tree_opts[ind[1]][1]
        #     print(', '.join([node.name for node in tree_opts[ind[1]][0].root_nodes()]))
        #     print('%s\n====================' % '\n'.join(steps))
        # return []
        return [tree_opts[ind[1]][0].make_schema() for ind in index]

    def copy_graph(self):
        cp = Graph(self.connection, self.db_name, current_id=self.current_id)
        for node in self.nodes.values():
            node.copy_node(cp)
        for edge in self.edges.values():
            edge.copy_edge(cp)
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
            else:
                all_ref = True
                for edge in node.parent_edges:
                    if not edge.reference:
                        all_ref = False
                if all_ref:
                    roots.add(node)
        return roots

    def data_loss_cost(self):
        data_loss = {}
        for edge in self.edges.values():
            loss = 0
            if not edge.reference:
                if edge.reversed:
                    loss = (data_loss_cost * edge.to_node.rowsize * 
                        max(edge.to_node.num_rows - edge.distinct_fk_count, 0))
                else:
                    loss = data_loss_cost * edge.null_fk_count * edge.to_node.rowsize
            if not edge.to_node.name in data_loss or loss < data_loss[edge.to_node.name]:
                data_loss[edge.to_node.name] = loss
        return sum(data_loss.values())

    def root_node_cost(self):
        return root_node_cost * sum([node.rowsize * node.num_rows for node in self.nodes.values()])

    def heuristic(self):
        total = self.data_loss_cost() + self.root_node_cost()
        for edge in self.edges.values():
            total += edge.duplication_and_ref_cost()
        return total

    def get_cycle(self):
        tested = set()
        for node in self.nodes.values():
            if node in tested:
                continue
            cycle = node.cycle_search(tested, set())
            if cycle != None:
                return cycle
        return None

    def get_multi_parent_node(self):
        for node in self.nodes.values():
            if len(node.parent_edges) > 1:
                for edge in node.parent_edges:
                    if not edge.reference:
                        return node
        return None

    def is_valid(self):
        return self.get_cycle() == None and self.get_multi_parent_node() == None

    def get_next_id(self):
        self.current_id += 1
        return self.current_id


    def __str__(self):
        s = ''
        for node in self.nodes.values():
            s += "%s\n" % node
        return s

    __repr__ = __str__





class Node:

    def __init__(self, graph, table, pk, rowsize, num_rows, node_id=None):
        self.id = (node_id or graph.get_next_id())
        self.name = table
        self.pk = pk
        self.rowsize = rowsize
        self.num_rows = num_rows
        self.child_edges = set()
        self.parent_edges = set()
        graph.nodes[self.id] = self

    def add_fkey(self, graph, fk_col, referenced_table, distinct_fk_count, null_fk_count):
        Edge(graph, referenced_table, self, fk_col, self.name, distinct_fk_count, null_fk_count)

    def make_table(self):
        table = Table(self.name, self.pk)
        self._embed_children(table)
        return table

    def _embed_children(self, table):
        for edge in self.child_edges:
            node = edge.to_node
            if edge.reversed:
                child = table.add_many_to_one_child(node.name, node.pk, edge.fkey_col)
            else:
                child = table.add_one_to_many_child(node.name, node.pk, edge.fkey_col)
            node._embed_children(child)

    def data_size(self):
        return self.rowsize * self.num_rows

    def __str__(self):
        children = ', '.join([edge.to_node.name for edge in self.child_edges])
        return "%s -> {%s}" % (self.name, children)

    __repr__ = __str__

    def cycle_search(self, tested, visited, path=[]):
        if self in visited:
            return path
        else:
            visited.add(self)
            tested.add(self)
            for edge in self.child_edges:
                if not edge.reference:
                    cycle = edge.to_node.cycle_search(tested, visited=visited, path=path + [edge])
                    if cycle != None:
                        return cycle
            return None

    def duplicate(self, graph):
        graph.nodes.pop(self.id)
        for edge in self.child_edges:
            graph.edges.pop(edge.id)
            edge.to_node.parent_edges.remove(edge)
        for edge in self.parent_edges:
            copy = Node(graph, self.name, self.pk, self.rowsize, self.num_rows)
            edge.to_node = copy
            copy.parent_edges.add(edge)
            for edge in self.child_edges:
                edge_copy = Edge(graph, copy, edge.to_node, edge.fkey_col, edge.fkey_table, edge.distinct_fk_count, 
                    edge.null_fk_count)
                edge_copy.reversed = edge.reversed
                edge_copy.reference = edge.reference

    def copy_node(self, graph):
        Node(graph, self.name, self.pk, self.rowsize, self.num_rows, node_id=self.id)






class Edge:

    def __init__(self, graph, from_node, to_node, fkey_col, fkey_table, distinct_fk_count, null_fk_count, edge_id=None,
        reversed=False, reference=False):

        self.from_node = from_node
        self.to_node = to_node
        self.fkey_col = fkey_col
        self.fkey_table = fkey_table
        self.distinct_fk_count = distinct_fk_count
        self.null_fk_count = null_fk_count
        self.reversed = reversed
        self.reference = reference
        self.from_node.child_edges.add(self)
        self.to_node.parent_edges.add(self)
        self.id = (edge_id or graph.get_next_id())
        graph.edges[self.id] = self

    def __str__(self):
        return "(%s -> %s) via %s.%s" % (self.from_node.name, self.to_node.name, self.fkey_table, self.fkey_col)

    __repr__ = __str__

    def reverse(self):
        self.reversed = not self.reversed
        self.from_node.child_edges.remove(self)
        self.from_node.parent_edges.add(self)
        self.to_node.parent_edges.remove(self)
        self.to_node.child_edges.add(self)
        new_to_node = self.from_node
        self.from_node = self.to_node
        self.to_node = new_to_node

    def make_ref(self):
        self.reference = True

    def reverse_and_make_ref(self):
        self.reverse()
        self.make_ref()

    # lower is better
    def duplication_and_ref_cost(self):
        total = 0
        if self.reference:
            total += reference_cost * self.to_node.data_size()
        elif self.reversed:
            total += (duplication_cost * self.to_node.rowsize * 
                max(self.from_node.num_rows - self.null_fk_count - self.to_node.num_rows, 0))
        else:
            total += duplication_cost * self.to_node.data_size()
        return total

    def copy_edge(self, graph):
        from_node = graph.nodes[self.from_node.id]
        to_node = graph.nodes[self.to_node.id]
        Edge(graph, from_node, to_node, self.fkey_col, self.fkey_table, self.distinct_fk_count, self.null_fk_count,
            edge_id=self.id, reversed=self.reversed, reference=self.reference)


    