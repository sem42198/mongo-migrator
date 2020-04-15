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

    def preview(self, file, num):
        results = {}
        for table in self.tables:
            results[table.table_name] = table.map(self.connection, preview=True, num=num)
        f = open(file, 'w')
        f.write(json.dumps(results, indent=4, default=str))

    def __str__(self):
        return str(self.graph)


class Table:
    def __init__(self, table_name, key):
        self.table_name = table_name
        self.key = key
        self.children = {}

    def add_one_to_many_child(self, child_table, child_key, fk_column):
        child = OneToManyChild(child_table, child_key, fk_column)
        label = child_table
        if label in self.children:
            other = self.children.pop(label)
            self.children["%s.%s" % (other.table_name, other.fk_column)] = other
            label = "%s.%s" % (child_table, fk_column)
        self.children[label] = child
        return child

    def add_many_to_one_child(self, child_table, child_key, fk_column):
        child = ManyToOneChild(child_table, child_key, fk_column)
        self.children[fk_column.strip('_id')] = child
        return child

    def map(self, connection, preview=False, num=None, mongo_database=None):
        with connection.cursor() as cursor:
            limit = ''
            if num != None:
                limit = 'ORDER BY RAND() LIMIT %d' % num
            sql = "SELECT * FROM `%s` %s;" % (self.table_name, limit)
            cursor.execute(sql)
            result = cursor.fetchone()
            results = []
            while result != None:
                for label in self.children.keys():
                    result[label] = self.children[label].map(connection, result, self.key)
                if preview:
                    results.append(result)
                else:
                    mongo_database.get_collection(
                        self.table_name, codec_options=codec_options.get()).insert_one(result)
                result = cursor.fetchone()
        if preview:
            return results

class Child(Table):
    def __init__(self, table_name, key, fk_column):
        super().__init__(table_name, key)
        self.fk_column = fk_column

class OneToManyChild(Child):
    # def __init__(self, table_name, key, fk_column):
    #     super().init(table_name, key, fk_column)


    def map(self, connection, parent_record, parent_key):
        parent_id = parent_record[parent_key]
        with connection.cursor() as cursor:
            sql = "select * from `" + self.table_name + "` where `" + self.fk_column + "`=%s;"
            cursor.execute(sql, (parent_id,))
            results = cursor.fetchall()
            for result in results:
                result.pop(self.fk_column)
                for col in self.children.keys():
                    result[col] = self.children[col].map(connection, result, self.key)
            return results

class ManyToOneChild(Child):
    # def __init__(self, table_name, key, fk_column):
    #     super().__init__(table_name, key, fk_column)

    def map(self, connection, parent_record, parent_key):
        child_id = parent_record.pop(self.fk_column)
        with connection.cursor() as cursor:
            sql = ("select * from `%s` where `%s`=" % (self.table_name, self.key)) + "%s;"
            cursor.execute(sql, (child_id,))
            result = cursor.fetchone()
            for label in self.children.keys():
                result[label] = self.children[label].map(connection, result, self.key)
            return result