# Mongo Migrator

MongoMigrator is a tool for automatically migrating a MySQL database to MongoDB. It is capable of generating and ranking several schema options for you to choose from.

## Installation and Setup

- Clone or download this repository.
- You will need to have Python3 and pip installed. This application has been tested with Python 3.6.9 but will work with any Python 3 version as far as we know.
- To install the requirements for Mongo Migrator run `pip3 install -r requirements.txt` from the Mongo Migrator's base directory.

## Database Setup

- You will need to [install MySQL](https://dev.mysql.com/doc/mysql-installation-excerpt/5.7/en/) and the database you wish to migrate. Some sample databases you can test the application with and their documentation are linked below.
  - [World](https://dev.mysql.com/doc/world-setup/en/)
  - [Employees](https://dev.mysql.com/doc/employee/en/)
  - [Classic Models](https://www.mysqltutorial.org/mysql-sample-database.aspx)
- You will also need to give a MySQL user permissions on the database you wish to migrate with `GRANT ALL ON DB_NAME.* to 'username'@'localhost'`.
- You will also need to setup a MongoDB instance. You can follow [these instructions](https://docs.mongodb.com/manual/administration/install-community/) to install MongoDB locally.
- Alternatively, you can use [MongoDB Atlas](https://www.mongodb.com/cloud/atlas) to create a free hosted instance. In this case, use the connection string provided under 'Connect to your application' as the `--mongodb-host` option.

## Running Mongo Migrator

- To run the app you will need to run `python3 mongo_migrator.py` with the appropriate options specified. These options are listed below. Additionally, you can run `python3 mongo_migrator.py --help` to see a summary of the available options.

|Option|Description|Required?|Default|
|---|---|---|---|
|--mysql-host|Name of MySQL database host|No|localhost|
|--mysql-port|Port number for MySQL database|No|3306|
|--mysql-username|Username of MySQL user with access to database you wish to migrate|Yes||
|--mysql-password|Password of MySQL user with access to database you wish to migrate|Yes||
|--mongodb-host|Host for MongoDB instance. Can also be a full MongoDB URI|No|localhost|
|--mongodb-port|Port number for MongoDB|No|27017|
|database|Name of MySQL database you wish to migrate|Yes||

- You can then respond to promts in order to view and previw schemas as well as migrate to MongoDB.
- When prompted to select which schema you wish to preview/migrate enter the number of the schema you wish to use. The schema number is an integer value listed before each schema option.
- The name of the resulting MongoDB database will be match the MySQL database.

## Schema Options

Schema options are displayed as a list of each record followed by its children.
- [*child name*] indicates a one to many child
- *child name* indicates a many to one child
- (*child name*) indicates a reference rather than an embedded child

Some record types may be listed more than once. This indicates that that record type has been "duplicated" and will appear in more than one place in the MongoDB database.

Schema options are each given a score and are presented to you with the best scoring ones first. The scores are based on the following factors.
- Data loss
- Number of references
- Total data stored (duplication)

To get a more detailed understaning of a schema, you can preview what it will look like as a JSON file.

## Files and Structure

- mongo_migrator.py serves as the entry point to the application.
- schema_graph.py connects to the MySQL database and generates and ranks different schemas to map the database to MongoDB.
- mongodb_schema.py is the representation of a MongoDB schema and performs the actual migration.
