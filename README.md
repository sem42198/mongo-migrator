# Mongo Migrator

MongoMigrator is a tool for automatically migrating a MySQL database to MongoDB. It is capable of generating and ranking several schema options for you to choose from.

## Installation and Setup

- Clone or download this repository.
- You will need to have Python3 installed
- To install the requirements for Mongo Migrator run `pip3 install -r requirements.txt`
- To run the app you will need to run `python3 migrate.py` with the appropriate options specified. These options can be viewed by running `python3 migrate.py --help`.

## Schema Options

Schema options are displayed as a list of each record followed by its children.
- [*child name*] indicates a one to many child
- *child name* indicates a many to one child
- (*child name*) indicates a reference rather than an embedded child

Schema options are each given a score and are presented to you with the best scoring ones first. The scores are based on the following factors.
- Data loss
- Number of references
- Total data stored (duplication)

To get a more detailed understaning of a schema, you can preview what it will look like as a JSON file.

## Files and Structure

- migrate.py serves as the entry point to the application.
- schema_graph.py connects to the MySQL database and generates and ranks different schemas to map the database to MongoDB.
- mongodb_schema.py is the representation of a MongoDB schema and performs the actual migration.