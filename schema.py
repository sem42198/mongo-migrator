import simplejson as json
import codec_options

class Schema:
    def __init__(self, graph):
        self.connection = graph.connection
        self.tables = []
        self.graph = graph

    def add_table(self, table):
        self.tables.append(table)

    def map(self, mongoclient):
        for table in self.tables:
            table.map(self.connection, mongo_database=mongoclient[self.graph.db_name])

    def preview(self, file, num=10):
        results = {}
        for table in self.tables:
            results[table.table_name] = table.map(self.connection, preview=True, num=num)
        f = open(file, 'w')
        f.write(json.dumps(results))

    def __str__(self):
        return str(self.graph)


class Table:
    def __init__(self, table_name, key):
        self.table_name = table_name
        self.key = key
        self.children = {}

    def add_one_to_many_child(self, child_table, child_key, fk_column):
        child = OneToManyChild(child_table, child_key, fk_column)
        self.children[child_table] = child
        return child

    def map(self, connection, preview=False, num=None, mongo_database=None):
        with connection.cursor() as cursor:
            limit = ''
            if num != None:
                limit = 'LIMIT %d' % num
            sql = "SELECT * FROM `%s` %s;" % (self.table_name, limit)
            cursor.execute(sql)
            result = cursor.fetchone()
            results = []
            while result != None:
                for col in self.children.keys():
                    result[col] = self.children[col].map(connection, result[self.key])
                if preview:
                    results.append(result)
                else:
                    mongo_database.get_collection(
                        self.table_name, codec_options=codec_options.get()).insert_one(result)
                result = cursor.fetchone()
        if preview:
            return results


class OneToManyChild(Table):
    def __init__(self, table_name, key, fk_column):
        super().__init__(table_name, key)
        # this column has to be not null
        self.child_column = fk_column


    def map(self, connection, parent_id):
        with connection.cursor() as cursor:
            sql = "select * from `" + self.table_name + "` where `" + self.child_column + "`=%s;"
            cursor.execute(sql, (parent_id,))
            results = cursor.fetchall()
            for result in results:
                for col in self.children.keys():
                    result[col] = self.children[col].map(connection)
            return results