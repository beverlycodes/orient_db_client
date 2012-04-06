OrientDB Client
===============

This gem is a very rough initial attempt at a Network Binary Protocol driver for OrientDB.

While in this early stage of development, the public interface is subject to change with no warning.

This is my first attempt at building a gem intended for public use.  Expect some difficulty getting this gem up and running in your projects.


Supported OrientDB Releases
---------------------------

I've tested this against OrientDB-1.0rc9 which is the current official release as of writing this README.


Supported Ruby Versions
---------------------------

This gem has only been tested on 1.9.3.  It *should* run on any 1.9 install, and will definitely NOT run on 1.8 due to the use of Encoding.  Compatibility with 1.8 is planned, but not scheduled for any particular milestone yet.


Basic Usage
-----------

There are two classifications of interaction with the OrientDB server:  Server and Database.  

A server session is used for creating and deleting databases, as well as confirming their existence.  (Setting and reading the server configuration is also part of the protocol, but not yet supported by this gem.)

A database session is used to performed all other work on a database, including but not limited to record CRUD operations, querying, and managing clusters (which are the physical files and logical subdivisions of an OrientDB database, not to be confused with clustering in the networking sense.)

## Connecting to an OrientDB Server

Before obtaining server or database sessions, a connection must be made to an OrientDB server instance.  There isn't much to this part.

    require 'orient_db_client'

    connection = OrientDBClient.connect('localhost')

If you need to specify the port, pass it in the options Hash:

    connection = OrientDBClient.connect('localhost', :port => 2424)

When you're done with the connection (and all of the sessions you have opened within it):

    connection.close

## Server Operations

A server session is only needed when an application wants to create, delete, or confirm the existence of a database.

To obtain a server session, call #open_server on the connection.  This requires user credentials from the `&lt;users&gt;` section of the OrientDB server's `orientdb-server-config.xml` file.

    server = connection.open_server({
        :user => 'root',
        :password => '<password>'
    })

**NOTE:** *I strongly suggest figuring out OrientDB's permission system and creating a non-root user in Orient's config.xml. :)*

Create a database using local storage (support for in-memory databases forthcoming):

    server.create_local_database("my_database")

Confirm that a database exists:

    server.database_exists? "my_database"

Delete a database:

    server.delete_database(my_database)


## Database Operations

Most work in OrientDB will be done with database sessions.  To open one:

    database = connection.open_database(database_name, {
        :user => 'admin',
        :password => 'admin'
    })

**NOTE:** *By default, OrientDB databases have three users pre-created, `reader`, `writer`, and `admin`.  Their passwords are the same as their names.  Don't forget to change these before running your OrientDB server in production. :)*


### CRUD

By default, a database contains the following clusters:  internal, index, default, orids, orole, and ouser.  (I'm not entirely sure what that default cluster is for, so I don't write to it.  It may be an all-purpose starter cluster though.)

Before records can be stored in the database, a cluster must be created to contain them.  Create one like this:

    cluster_id = database.create_physical_cluster("mycluster")

This adds a physical (file) cluster to the database.  (Support for logical clusters forthcoming.)

This gem was written to be somewhat liberal with what it will accept as a valid record Hash.  The gem will make implicit decisions about how to serialize each of the values.

The following is an acceptable record:

    record = { :key1 => 'value1', 'key2' => 2, 'key3' => 3.45 }

The symbolic `:key1` will be converted into a string.  Key2's value will be stored as an integer.  Key3's value will be stored as a double.

To gain more explicit control over serialization, a record must conform to the following structure:

    record = { :document => { :key1 => 'value1', 'key2' => 2, 'key3' => 3.45 },
               :structure => { 'key1' => :string, 'key2' => :integer, 'key3' => :double },
               :class => "MyClass" }

**NOTE:** The `:class` is optional.  It is only useful in databases that use classes to implement a schema.

Creating a record is straightforward:

    rid = database.create_record(cluster_id, record)

The return value is an OrientDbClient::Rid.  You can get the native "#i:p" form by calling #to_s on the returned Rid.

An existing record can be read using #load_Record:

    loaded_record = database.load_record(rid)

Records returned by #load_record will contain the above `:document` and `:structure` keys, along with `:record_version`, `:cluster_id`, and `:cluster_position`.  `:class` will be provided if the record has a class.

To update the record:

    loaded_record[:document]['key1'] = 'updated'
    version = database.update_record(loaded_record, rid, :incremental)

The updated record is stored to the database, and its new version number is returned.  The `:incremental` option tells OrientDB to increment the record's version number when persisting the record.  Alternatively, `:none` can be passed and OrientDB will not perform any type of versioning.  Lastly, an explicit numeric version can be passed and it will be used as the record's new version number.

**NOTE:** OrientDB does not currently retain old records.  It isn't so much "version control" as version marking.


To delete a record:

    database.delete_record(rid, version)

The version must match the version number of the record as it is currently stored in the database.


### Querying (Experimental)

OrientDB implements a SQL-like language that can be used to query records, add/alter/remove clusters, add/alter/remove classes, and more.  See the [SQL section](http://code.google.com/p/orient/wiki/SQL) of OrientDB's Wiki for more information.


Use the #query method to send a query.  **Be careful with this method.**  Parameterized queries and sanitization have not been implemented in the gem.  Insufficient sanitization of SQL strings sent to this method could open up the database to SQL Injection attacks.  While OrientDB's SQL language is still evolving, it will be difficult to know what kind of vulnerabilities exist.

    database.query "SELECT FROM cluster:mycluster"

This should return an array of deserialized records.

**NOTE: Expect this to be buggy right now.**  *OrientDB's protocol mentions several possible return values from a query, but I've only been able to get it to return record collections in practice.  As such, I've not coded for the possibility of getting back a single record, getting back raw data, or getting back a flat response.  I'm not even sure what the latter two look like when they come out of the OrientDB server.*

Testing
-------

    rake test:unit

There are integration tests as well, but the testing strategy is extremely flawed.  Start an instance of OrientDB before running.  Most of the tests are run against the "temp" database, which is reset every time OrientDB is started.  The integration test results should not be considered valid unless run against a fresh restart of the OrientDB server.

    rake test:integration
