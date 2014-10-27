# About #


Stratio Connector Streaming is a crossdata connector interface implementation for stratio-streaming.

## Requirements ##

Install [Stratio Streaming] (http://docs.openstratio.org/getting-started.html#stratio-streaming) and run it. 
[Crossdata] (https://github.com/Stratio/crossdata) is needed to interact with this connector.

## Compiling Stratio Connector Streaming ##

To automatically build execute the following command:

```
   > mvn clean compile install
```

## Running the Stratio Connector Streaming ##

```
   > cd connector-streaming-core
   > mvn exec:java -Dexec.mainClass="com.stratio.connector.streaming.core.StreamingConnector"
```


## Build an executable Connector Streaming ##

To generate the executable execute the following command:

```
   > mvn crossdata-connector:install
```

To run Connector Streaming execute:

```
   > target/connector-streaming-core-0.4.0/bin/connector-streaming-core-0.4.0 start
```

To stop the connector execute:

```
   > target/connector-streaming-core-0.4.0/bin/connector-streaming-core-0.4.0 stop
```

## How to use Connector Streaming ##

 1. Start [crossdata-server and then crossdata-shell](https://github.com/Stratio/crossdata).  
 2. https://github.com/Stratio/crossdata
 3. Start Streaming Connector as it is explained before
 4. In crossdata-shell:
    
    Add a datastore with this command:
      
      ```
         xdsh:user>  ADD DATASTORE <Absolute path to Streaming Datastore manifest>;
      ```

    Attach cluster on that datastore. The datastore name must be the same as the defined in the Datastore manifest.
    
      ```
         xdsh:user>  ATTACH CLUSTER <cluster_name> ON DATASTORE <datastore_name> WITH OPTIONS {'KafkaServer': '[<kafkaServer>]', 'KafkaPort': '[<kafkaPort>]', 'zooKeeperServer':'[<zooKeeperServer>]','zooKeeperPort':'[<zooKeeperPort>]'};
      ```

    Add the connector manifest.

       ```
         xdsh:user>  ADD CONNECTOR <Path to Streaming Connector Manifest>
       ```
    
    Attach the connector to the previously defined cluster. The connector name must match the one defined in the 
    Connector Manifest.
    
        ```
            xdsh:user>  ATTACH CONNECTOR <connector name> TO <cluster name> WITH OPTIONS {};
        ```
    
    At this point, we can start to send queries.
    
        ...
            xdsh:user> CREATE CATALOG catalogTest;
        
            xdsh:user> USE catalogTest;
        
            xdsh:user> CREATE TABLE tableTest ON CLUSTER streaming_prod (id int PRIMARY KEY, name text);
    
            xdsh:user> INSERT INTO tableTest(id, name) VALUES (1, 'stratio');
    
            xdsh:user> SELECT * FROM tableTest;
        ...


## Behaviours ##
To ensure the creation of an ephemeral table the connector insert a random value. This value can be recovered together with the real values.

# License #

Licensed to STRATIO (C) under one or more contributor license agreements.
See the NOTICE file distributed with this work for additional information
regarding copyright ownership.  The STRATIO (C) licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.





