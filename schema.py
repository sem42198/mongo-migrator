class Schema:
    def __init__(self, connection):
        self.connection = connection
        self.tables = []

    def add_table(self, table):
        self.tables.append(table)

    def map(self):
        for table in self.tables:
            table.map(self.connection)


class Table:
    def __init__(self, table_name, key):
        self.table_name = table_name
        self.key = key
        self.children = {}

    def add_one_to_many_child(self, child_table, child_key, fk_column):
        child = OneToManyChild(child_table, child_key, fk_column)
        self.children[child_table] = child
        return child

    def map(self, connection):
        with connection.cursor() as cursor:
            sql = "select * from `" + self.table_name + "`;"
            cursor.execute(sql)
            result = cursor.fetchone()
            while result != None:
                for col in self.children.keys():
                    result[col] = self.children[col].map(connection, result[self.key])
                print(result)
                result = cursor.fetchone()


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