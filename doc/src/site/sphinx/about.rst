About
*****

Stratio Connector Streaming allows `Stratio Crossdata <http://docs.stratio.com/modules/crossdata/0.4/index.html>`__ to interact with Streaming.

Requirements
------------

Install `Stratio Streaming <http://docs.openstratio.org/getting-started.html#stratio-streaming>`__ and
run it. 

`Crossdata <http://docs.stratio.com/modules/crossdata/0.4/index.html>`__ is needed to
interact with this connector.

Compiling Stratio Connector Streaming
-------------------------------------

To automatically build execute the following command:

::

       > mvn clean install

Running the Stratio Connector Streaming
---------------------------------------

To run Connector Streaming execute in the parent directory:

::

       > ./connector-streaming/target/stratio-connector-streaming/bin/stratio-connector-streaming

Build a redistributable package
-------------------------------
It is possible too, to create a RPM or DEB package as:

::

       > mvn package -Ppackage
       
Once the package it’s created, execute this commands to install:

RPM Package:

::   
    
       > rpm -i target/stratio-connector-streaming-<version>.rpm

DEB Package:

::   
    
       > dpkg -i target/stratio-connector-streaming-<version>.deb

Now to start/stop the connector:

::   
    
       > service stratio-connector-streaming start
       
       > service stratio-connector-streaming stop


How to use Connector Streaming
------------------------------

A complete tutorial is available `here <First_Steps.rst>`__. The
basic commands are described below.

1. Start `Stratio Crossdata Server and then Stratio Crossdata Shell <http://docs.stratio.com/modules/crossdata/0.4/index.html>`__. 
2. Start Streaming Connector as it is explained before
3. In crossdata-shell:

Attach cluster on that datastore. The datastore name must be the same as the defined in the Datastore manifest.
::

       xdsh:user>  ATTACH CLUSTER <cluster_name> ON DATASTORE <datastore_name> WITH OPTIONS {'KafkaServer': '[<kafkaHost_1,kafkaHost_2...kafkaHost_n>]', 'KafkaPort': '[<kafkaPort_1, kafkaPort_2...kafkaPort_n>]', 'zooKeeperServer':'[<zooKeeperHost_1,zooKeeperHost_2...zooKeeperHost_n>]','zooKeeperPort':'[<zooKeeperPort_1,zooKeeperPort_2...zooKeeperPort_n>]'};

Attach the connector to the previously defined cluster. The connector name must match the one defined in the Connector Manifest, and the cluster name must match with the previously defined in the ATTACH CLUSTER command.
   |
:: 

       xdsh:user>  ATTACH CONNECTOR <connector name> TO <cluster name> WITH OPTIONS {};

At this point, we can start to send queries, that Crossdata execute with the connector specified..

   ::

           xdsh:user> CREATE CATALOG catalogTest;

           xdsh:user> USE catalogTest;

           xdsh:user> CREATE TABLE tableTest ON CLUSTER <cluster_name> (id int PRIMARY KEY, name text);

           xdsh:user> SELECT * FROM tableTest WITH WINDOW 2 sec;

           xdsh:user> INSERT INTO tableTest(id, name) VALUES (1, 'stratio');


Behaviours
----------

To ensure the creation of an ephemeral table the connector insert a
random value. This value can be recovered together with the real values.

License
=======

Licensed to STRATIO (C) under one or more contributor license
agreements. See the NOTICE file distributed with this work for
additional information regarding copyright ownership. The STRATIO (C)
licenses this file to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

