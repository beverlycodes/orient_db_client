OrientDB Client
===============

This gem is a very rough initial attempt at a Network Binary Protocol driver for OrientDB.

While in this early stage of development, the public interface is subject to change with no warning.

This is my first attempt at building a gem intended for public use.  Expect some difficulty getting this gem up and running in your projects.


Supported OrientDB Releases
---------------------------

I've tested this against OrientDB-1.0rc9 which is the current official release as of writing this README.


Basic Usage
-----------


## Server Operations

    require 'orient_db_client`

    connection = OrientDbClient.connect('0.0.0.0', { :port => 2424 })

    server = connection.open_server({
        :user => 'root',
        :password => '<password>'
    })

    server.create_local_database("my_database")

    server.delete_database(database_name)


## Database Operations

    require 'orient_db_client`

    connection = OrientDbClient.connect('0.0.0.0', { :port => 2424 })

    database = connection.open_database(database_name, {
        :user => 'admin',
        :password => 'admin'
    })

    cluster_id = database.create_physical_cluster("test")

    record = { :document => { 'key1' => 'value1' } }

    rid = database.create_record(cluster_id, record)

    loaded_record = database.load_record(rid)
    loaded_record[:document]['key1'] = 'updated'

    version = database.update_record(loaded_record, rid, :incremental)

    edited_record = database.load_record(rid)

    database.delete_record(rid, edited_record[:record_version])


Testing
-------

    rake test:unit

There are integration tests as well, but the testing strategy is extremely flawed.  Start an instance of OrientDB before running.  Most of the tests are run against the "temp" database, which is reset every time OrientDB is started.  The integration test results should not be considered valid unless run against a fresh restart of the OrientDB server.

    rake test:integration
