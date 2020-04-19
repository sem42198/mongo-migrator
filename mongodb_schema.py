import simplejson as json
import codec_options

# represents a mongodb schema
class Schema:
    def __init__(self, graph):
        self.connection = graph.connection
        self.collections = []
        self.refs = []
        self.graph = graph

    # adds a collection to the schema
    def add_collection(self, table):
        self.collections.append(table)

    # adds a many to one reference to the schema
    def add_many_to_one_ref(self, child_name, child_key, parent_path, parent_key, fk_column):
        self.refs.append(ManyToOneRef(child_name, child_key, parent_path, parent_key, fk_column))

    # adds a one to many reference to the schema
    def add_one_to_many_ref(self, child_name, child_key, parent_path, parent_key, fk_column):
        self.refs.append(OneToManyRef(child_name, child_key, parent_path, parent_key, fk_column))

    # maps data from MySQL database accordng to this schema and saves it to MongoDB
    def map(self, mongoclient):
        db = mongoclient[self.graph.db_name]
        for table in self.collections:
            table.map(self.connection, mongo_database=db)
        for ref in self.refs:
            ref.add_ref(db)

    # maps a few records according to this schema and writes them to a JSON file
    def preview(self, file, num):
        results = {}
        for table in self.collections:
            results[table.table_name] = table.map(self.connection, preview=True, num=num)
        f = open(file, 'w')
        f.write(json.dumps(results, indent=4, default=str))

    def __str__(self):
        return str(self.graph)

# represents a collection in MongoDB
class Collection:
    def __init__(self, table_name, key):
        self.table_name = table_name
        self.key = key
        self.children = {}

    # adds an embeded one to many child record to the collection
    def add_one_to_many_child(self, child_table, child_key, fk_column):
        child = OneToManyChild(child_table, child_key, fk_column)
        label = "%s_%s" % (fk_column, child_table)
        self.children[label] = child
        return child, label

    # adds an embeded many to one (or one to one) child record to the collection
    def add_many_to_one_child(self, child_table, child_key, fk_column):
        child = ManyToOneChild(child_table, child_key, fk_column)
        label = fk_column
        self.children[label] = child
        return child, label

    # maps data for this collection from MySQL db
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

# represents an embeded child record
class Child(Collection):
    def __init__(self, table_name, key, fk_column):
        super().__init__(table_name, key)
        self.fk_column = fk_column

# represents a one to many embeded child record
class OneToManyChild(Child):

    # maps data for this child record from MySQL db
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

# represents a many to one embeded child record
class ManyToOneChild(Child):

    # maps data for this child record from MySQL db
    def map(self, connection, parent_record, parent_key):
        child_id = parent_record.pop(self.fk_column)
        with connection.cursor() as cursor:
            sql = ("select * from `%s` where `%s`=" % (self.table_name, self.key)) + "%s;"
            cursor.execute(sql, (child_id,))
            result = cursor.fetchone()
            for label in self.children.keys():
                result[label] = self.children[label].map(connection, result, self.key)
            return result

# represents a reference in MongoDB
class Ref():
    def __init__(self, child_name, child_key, parent_path, parent_key, fk_column):
        self.child_name = child_name
        self.child_key = child_key
        self.parent_path = parent_path
        self.parent_key = parent_key
        self.fk_column = fk_column

    # adds appropriate reference to all applicable records
    def do_update(self, db):
        collection = db[self.parent_path[0]]
        for record in collection.find():
            self.find_parents(record, self.parent_path[1:], db)
            collection.replace_one({'_id': record['_id']}, record)

    # finds the records (may be nested) that should have a reference added
    def find_parents(self, record, path, db):
        if len(path) == 0:
            if type(record) is list:
                for rec in record:
                    self.update_value(rec, db)
            else:
                self.update_value(record, db)
        else:
            new_recs = record.get(path[0], [])
            new_path = path[1:]
            if type(new_recs) is list:
                for new_rec in new_recs:
                    self.find_parents(new_rec, new_path, db)
            else:
                self.find_parents(new_recs, new_path, db)

# represents a one to many reference in MongoDB
class OneToManyRef(Ref):

    # adds the reference to records in MongoDB
    def add_ref(self, db):
        # first map referenced record ids to the key values of the records that should reference them
        self.children = {}
        for child in db[self.child_name].find():
            fk = child[self.fk_column]
            if fk:
                self.children[fk] = self.children.get(fk, [])
                self.children[fk].append(child['_id'])

        self.do_update(db)

    # adds reference to a single (oftentimes nested) record
    def update_value(self, record, db):
        label = "%s_%s_ref" % (self.fk_column ,self.child_name)
        key = record[self.parent_key]
        record[label] = self.children.get(key, [])

# represents a many to one reference in MongoDB
class ManyToOneRef(Ref):

    # adds the reference to records in MongoDB
    def add_ref(self, db):
        self.do_update(db)

    # adds reference to a single (oftentimes nested) record
    def update_value(self, record, db):
        fk = record[self.fk_column]
        if not fk:
            return
        child = db[self.child_name].find_one({self.child_key: fk})
        _id = child['_id']
        label = "%s_ref" % self.fk_column
        record[label] = _id