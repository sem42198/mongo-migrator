from mongodb_schema import *
from statistics import mean
import copy

data_storage_cost = 1
data_loss_cost = 10
ref_cost = 7

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

    def __init__(self, connection, db_name, current_id=0, steps=[]):
        self.db_name = db_name
        self.connection = connection
        self.nodes = {}
        self.edges = {}
        self.current_id = current_id
        self.steps = steps
        if current_id == 0:
            self.init_tables()

    def add_step(self, step):
        self.steps = self.steps + [step]
        

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


    def generate(self, graphs):
        multiparent_node = self.get_multi_parent_node()
        if multiparent_node != None:

            if not multiparent_node.dont_dup:
                copy = self.copy_graph()
                copy.nodes[multiparent_node.id].duplicate(copy)
                graphs.append(copy)

            problem_edges = multiparent_node.parent_edges

        else:
            problem_edges = self.get_cycle() or []

        # Try reversing an edge
        for edge in problem_edges:
            if not edge.reversed:
                copy = self.copy_graph()
                copy.edges[edge.id].reverse(copy)
                graphs.append(copy)

        # Try making an edge a ref edge
        for edge in problem_edges:
            if not edge.reference:
                copy = self.copy_graph()
                copy.edges[edge.id].make_ref(copy)
                graphs.append(copy)



    def get_opts(self):
        # make any table with a fk pointing to itself a ref since there is no other option
        for edge in self.edges.values():
            if edge.from_node == edge.to_node:
                edge.make_ref(self)

        for node in self.nodes.values():
            if len(node.parent_edges) > 1:
                # we make an exception for when it has a recursive edge but otherwise don't duplicate it
                nonref = False
                for edge in node.parent_edges:
                    if not edge.reference:
                        if nonref:
                            node.dont_dup = True
                        else:
                            nonref = True
            if not node.in_undirected_cycle():
                # only allow duplication if node is part of a cycle were the graph undirected
                node.dont_dup = True
        
        tree_opts = []
        graphs = [self]
        num_edges = len(self.edges)
        while not len(graphs) == 0:
            curr = graphs.pop()
            if curr.is_valid():
                tree_opts.append(curr)
            elif len(curr.steps) < num_edges * (2/3):
                curr.generate(graphs)
        for opt in tree_opts:
            for root in opt.root_nodes():
                root.adjust_child_size()
        for opt in tree_opts:
            opt.handle_lossy_edges(tree_opts)
        Graph.scale_opt_scores(tree_opts)
        index = [[tree_opts[i].score, i] for i in range(len(tree_opts))]
        index.sort()
        return [tree_opts[i].make_mongodb_schema() for _h, i in index]

    def handle_lossy_edges(self, tree_opts):
        for root in self.root_nodes():
            for edge in root.child_edges:
                if self.data_loss_cost(edge.to_node.name) != 0:
                    copy = self.copy_graph()
                    copy.edges[edge.id].make_ref(copy)
                    if copy.is_valid():
                        for root in copy.root_nodes():
                            root.adjust_child_size()
                        tree_opts.append(copy)
                        copy.handle_lossy_edges(tree_opts)

                    # now reverse the ref edge
                    copy = copy.copy_graph()
                    copy.edges[edge.id].reverse(copy)
                    if copy.is_valid():
                        for root in copy.root_nodes():
                            root.adjust_child_size()
                        tree_opts.append(copy)
                        copy.handle_lossy_edges(tree_opts)



    def copy_graph(self):
        cp = Graph(self.connection, self.db_name, current_id=self.current_id, steps=self.steps)
        for node in self.nodes.values():
            node.copy_node(cp)
        for edge in self.edges.values():
            edge.copy_edge(cp)
        return cp

    def make_mongodb_schema(self):
        schema = Schema(self)
        for node in self.root_nodes():
            schema.add_table(node.make_table())
        for node in self.nodes.values():
            node.add_refs(schema)
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

    def data_loss_cost(self, table=None):
        total_size = {}
        orig_size = {}
        for node in self.nodes.values():
            orig_size[node.name] = node.orig_num_rows * node.rowsize
            total_size[node.name] = total_size.get(node.name, 0) + node.distinct_rows * node.rowsize

        if table:
            return max(0, orig_size[table] - total_size[table])
        else:
            return sum([max(0, orig_size[table] - total_size[table]) for table in total_size.keys()])

    def data_storage_cost(self):
        return sum([node.data_size() for node in self.nodes.values()])

    def ref_cost(self):
        num_refs = 0
        for edge in self.edges.values():
            if edge.reference:
                num_refs += 1
        return num_refs

    def scale_opt_scores(schema_opts):
        data_loss = []
        data_storage = []
        refs = []
        for graph in schema_opts:
            data_loss.append(graph.data_loss_cost())
            data_storage.append(graph.data_storage_cost())
            refs.append(graph.ref_cost())
        average_data_loss = max(mean(data_loss), 1e-9)
        average_data_storage = max(mean(data_storage), 1e-9)
        average_refs = max(mean(refs), 1e-9)
        for i in range(len(schema_opts)):
            graph = schema_opts[i]
            graph.scaled_data_loss = data_loss_cost * data_loss[i] / average_data_loss
            graph.scaled_data_storage = data_storage_cost * data_storage[i] / average_data_storage
            graph.scaled_refs = ref_cost * refs[i] / average_refs
            graph.score = graph.scaled_data_loss + graph.scaled_data_storage + graph.scaled_refs

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

    def refs_valid(self):
        # having multiple refs to a node can create to same problem as duplicating it
        for node in self.nodes.values():
            if node.dont_dup and len(node.parent_edges) > 1:
                # don't actually count a recursive edge as a parent edge
                nonrec = False
                for edge in node.parent_edges:
                    if edge.to_node != edge.from_node:
                        if nonrec:
                            return False
                        else:
                            nonrec = True
        return True

    def is_valid(self):
        return self.get_cycle() == None and self.get_multi_parent_node() == None and self.refs_valid()

    def get_next_id(self):
        self.current_id += 1
        return self.current_id


    def __str__(self):
        return "\n".join(['------------------------------'] +
            [str(node) for node in self.nodes.values()] + ['=============================='])

    __repr__ = __str__





class Node:

    def __init__(self, graph, table, pk, rowsize, num_rows, node_id=None, dont_dup=False):
        self.id = (node_id or graph.get_next_id())
        self.name = table
        self.pk = pk
        self.rowsize = rowsize
        self.num_rows = num_rows
        self.orig_num_rows = num_rows
        self.distinct_rows = num_rows
        self.dont_dup = dont_dup
        self.child_edges = set()
        self.parent_edges = set()
        self.path = []
        graph.nodes[self.id] = self

    def add_fkey(self, graph, fk_col, referenced_table, distinct_fk_count, null_fk_count):
        Edge(graph, referenced_table, self, fk_col, self.name, distinct_fk_count, null_fk_count)

    def make_table(self):
        self.path = [self.name]
        table = Table(self.name, self.pk)
        self._embed_children(table)
        return table

    def _embed_children(self, table):
        for edge in self.child_edges:
            node = edge.to_node
            if edge.reference:
                continue
            elif edge.reversed:
                child, label = table.add_many_to_one_child(node.name, node.pk, edge.fkey_col)
            else:
                child, label = table.add_one_to_many_child(node.name, node.pk, edge.fkey_col)
            node.path = self.path + [label]
            node._embed_children(child)

    def add_refs(self, schema):
        for edge in self.child_edges:
            if edge.reference:
                if edge.reversed:
                    schema.add_many_to_one_ref(edge.to_node.name, edge.to_node.pk, edge.from_node.path, 
                        edge.from_node.pk, edge.fkey_col)
                else:
                    schema.add_one_to_many_ref(edge.to_node.name, edge.to_node.pk, edge.from_node.path, 
                        edge.from_node.pk, edge.fkey_col)

    def data_size(self):
        return self.rowsize * self.num_rows

    def adjust_child_size(self):
        for edge in self.child_edges:
            if edge.reference:
                continue
            elif edge.reversed:
                edge.to_node.num_rows = self.num_rows * (1 - (edge.null_fk_count / self.orig_num_rows))
                edge.to_node.distinct_rows = ((self.distinct_rows / self.orig_num_rows) * edge.to_node.orig_num_rows * 
                    (1 - (edge.null_fk_count / self.orig_num_rows)))
            else:
                edge.to_node.num_rows = ((self.num_rows / self.orig_num_rows) * 
                    (edge.to_node.orig_num_rows - edge.null_fk_count))
                edge.to_node.distinct_rows = ((self.distinct_rows / self.orig_num_rows) *
                    (edge.to_node.orig_num_rows - edge.null_fk_count))
            edge.to_node.adjust_child_size()


    def __str__(self):
        edges = []
        for edge in self.child_edges:
            name = edge.to_node.name
            if edge.reference:
                edges.append("(%s)" % name)
            elif edge.reversed:
                edges.append(name)
            else:
                edges.append("[%s]" % name)
        children = ', '.join(edges)
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

    def in_undirected_cycle(self):
        return self.undirected_search_for(self, [], set())

    def undirected_search_for(self, search_for, used_edges, visited_nodes):
        visited_nodes.add(self)

        for edge in self.child_edges:
            if edge.reference or edge in used_edges :
                continue
            else:
                if edge.to_node == search_for:
                    return True
                elif not edge.to_node in visited_nodes:
                    if edge.to_node.undirected_search_for(search_for, used_edges + [edge], visited_nodes):
                        return True
        for edge in self.parent_edges:
            if edge.reference or edge in used_edges :
                continue
            else:
                if edge.from_node == search_for:
                    return True
                elif not edge.from_node in visited_nodes:
                    if edge.from_node.undirected_search_for(search_for, used_edges + [edge], visited_nodes):
                        return True
        return False


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
        graph.add_step("Duplicated node: %s" % self.name)

    def copy_node(self, graph):
        Node(graph, self.name, self.pk, self.rowsize, self.num_rows, node_id=self.id, dont_dup=self.dont_dup)






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

    def reverse(self, graph):
        self.reversed = not self.reversed
        self.from_node.child_edges.remove(self)
        self.to_node.parent_edges.remove(self)
        self.from_node.parent_edges.add(self)
        self.to_node.child_edges.add(self)
        new_to_node = self.from_node
        self.from_node = self.to_node
        self.to_node = new_to_node
        graph.add_step("Reversed edge: %s" % self)

    def make_ref(self, graph):
        self.reference = True
        graph.add_step("Converted edge to ref: %s" % self)

    def copy_edge(self, graph):
        from_node = graph.nodes[self.from_node.id]
        to_node = graph.nodes[self.to_node.id]
        Edge(graph, from_node, to_node, self.fkey_col, self.fkey_table, self.distinct_fk_count, self.null_fk_count,
            edge_id=self.id, reversed=self.reversed, reference=self.reference)


    