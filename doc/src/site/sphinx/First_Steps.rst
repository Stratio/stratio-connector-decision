First Steps
***********

Streaming Crossdata connector allows the integration between Crossdata
and Stratio Streaming. Crossdata provides an easy and common language as
well as the integration with several other databases. More information
about Crossdata can be found at
`Crossdata <https://github.com/Stratio/crossdata>`__

Table of Contents
=================

-  `Before you start <#before-you-start>`__

   -  `Prerequisites <#prerequisites>`__
   -  `Configuration <#configuration>`__

-  `Creating the catalog and table <#creating-the-catalog-and-table>`__

   -  `Step 1: Create the catalog <#step-1-create-the-catalog>`__
   -  `Step 2: Create the table <#step-2-create-the-table>`__

-  `Querying Data <#querying-data>`__

   -  `Step 3: Select Window <#step-6-select-window>`__

      -  `Select all row window <#select-row-window>`__
      -  `Select all time window <#select-time-window>`__
      -  `Select with alias <#select-with-alias>`__
      -  `Select with several where
         clauses <#select-with-several-where-clauses>`__

-  `Inserting Data <#inserting-data>`__

   -  `Step 4: Insert into table
      students <#step-4-insert-into-table-students>`__

-  `Altering schemas <#altering-schemas>`__

   -  `Step 5: Alter table <#step-5-alter-table>`__

      -  `Add column <#add-column>`__

-  `Remove schemas <#delete-data-and-remove-schemas>`__

   -  `Step 6: Drop table <#step-6-drop-table>`__
   -  `Step 7: Drop catalog <#step-7-drop-catalog>`__

-  `Where to go from here <#where-to-go-from-here>`__

Before you start
================

Prerequisites
-------------

-  Basic knowledge of SQL like language.
-  First of all `Stratio Crossdata
   <version> <https://github.com/Stratio/crossdata>`__ is needed and must be
   installed. The server and the shell must be running.
-  An installation of `Stratio Streaming
   <version> <http://docs.stratio.com/modules/streaming-cep-engine/development/#stratio-streaming>`__.
-  Build an StreamingConnector executable and run it following this
   `guide <https://github.com/Stratio/stratio-connector-streaming#build-an-executable-connector-streaming>`__.

Configuration
-------------

Configure the datastore cluster.

::

    > ATTACH CLUSTER streamingCluster ON DATASTORE Streaming WITH OPTIONS {'KafkaServer': '[<ip>]', 'KafkaPort': '[9092]', 'zooKeeperServer':'[<ip>]','zooKeeperPort':'[2181]'};

The output must be similar to:

::

      Result: QID: 82926b1e-2f72-463f-8164-98969c352d40
      Cluster attached successfully

Now we must run the connector.

And attach the connector to the cluster created before.

::

      >  ATTACH CONNECTOR StreamingConnector TO streamingCluster  WITH OPTIONS {};

The output must be:

::

    CONNECTOR attached successfully

To ensure that the connector is online we can execute the Crossdata
Shell command:

::

      > describe connectors;

And the output must show a message similar to:

::

    Connector: connector.streamingconnector ONLINE  []  [datastore.streaming]   akka.tcp://CrossdataServerCluster@127.0.0.1:46646/user/ConnectorActor/

Creating the catalog and table
==============================

Step 1: Create the catalog
--------------------------

Now we will create the catalog and the table which we will use later in
the next steps.

To create the catalog we must execute.

::

        > CREATE CATALOG highschool;

The output must be:

::

    CATALOG created successfully;

Step 2: Create the table
------------------------

We switch to the database we have just created.

::

      > USE highschool;

To create the table we must execute the next command. The stream is
actually created at this point.

::

      > CREATE TABLE students ON CLUSTER streamingCluster (id int PRIMARY KEY, name text, age int, 
    enrolled boolean);

And the output must show:

::

    TABLE created successfully

Querying Data
=============

Step 3: Select Window
---------------------

Now we can execute one of the following queries before inserting data.
The queries are asynchronous, so it is possible to know the matched
result with the query id.

Select row window
~~~~~~~~~~~~~~~~~

::

      > SELECT * FROM students WITH WINDOW 2 ROWS;
     

Select time window
~~~~~~~~~~~~~~~~~~

::

      > SELECT * FROM students WITH WINDOW 10 sec;
      

Select with alias
~~~~~~~~~~~~~~~~~

::

       >  SELECT name as the_name, enrolled  as is_enrolled FROM students WITH WINDOW 20 sec;

Select with several where clauses
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

::

      >  SELECT * FROM students WITH WINDOW 20 sec WHERE age > 19 AND enrolled = true ;

Inserting Data
==============

Step 4: Insert into table students
----------------------------------

::

      >  INSERT INTO students(id, name,age,enrolled) VALUES (1, 'Jhon', 16,true);
      >  INSERT INTO students(id, name,age,enrolled) VALUES (2, 'Eva',20,true);
      >  INSERT INTO students(id, name,age,enrolled) VALUES (3, 'Lucie',18,true);
      >  INSERT INTO students(id, name,age,enrolled) VALUES (4, 'Cole',16,true);
      >  INSERT INTO students(id, name,age,enrolled) VALUES (5, 'Finn',17,false);
      >  INSERT INTO students(id, name,age,enrolled) VALUES (6, 'Violet',21,false);
      >  INSERT INTO students(id, name,age,enrolled) VALUES (7, 'Beatrice',18,true);
      >  INSERT INTO students(id, name,age,enrolled) VALUES (8, 'Henry',16,false);
      >  INSERT INTO students(id, name,age,enrolled) VALUES (9, 'Tom',17,true);
      >  INSERT INTO students(id, name,age,enrolled) VALUES (10, 'Betty',19,true);

For each row the output must be:

::

    STORED successfully

Altering Schemas
================

Step 5: Alter table
-------------------

Add column
~~~~~~~~~~

Now we will alter the table structure.

::

      > ALTER TABLE students ADD surname TEXT;
      OK

After the alter operation we can execute a new query:

::

      > SELECT * FROM students WITH WINDOW 1 ROWS WHERE surname = 'Smith';

Then, insert the surname field in the table.

::

        > INSERT INTO students(id, name,age,enrolled,surname) VALUES (10, 'Betty',19,true, 'Smith');

And the result must contain the row correctly.

::

      -----------------------------------------
      | age | name  | id | surname | enrolled | 
      -----------------------------------------
      | 19  | Betty | 10 | Smith   | true     | 
      -----------------------------------------

Remove Schemas
==============

Step 6: Drop table
------------------

To drop the table we must execute:

::

      >  DROP TABLE students;
      TABLE dropped successfully

Step 7: Drop catalog
--------------------

::

      >  DROP CATALOG IF EXISTS highschool;
      CATALOG dropped successfully

Where to go from here
=====================

To learn more about Stratio Crossdata, we recommend to visit the
`Crossdata
Reference <https://github.com/Stratio/crossdata/blob/master/_doc/meta-reference.md>`__.

