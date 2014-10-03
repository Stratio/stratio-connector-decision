/*
 * Stratio Meta
 *
 *   Copyright (c) 2014, Stratio, All rights reserved.
 *
 *   This library is free software; you can redistribute it and/or modify it under the terms of the
 *   GNU Lesser General Public License as published by the Free Software Foundation; either version
 *   3.0 of the License, or (at your option) any later version.
 *
 *   This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 *   even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 *   Lesser General Public License for more details.
 *
 *   You should have received a copy of the GNU Lesser General Public License along with this library.
 */

package com.stratio.connector.streaming.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.commons.CommonsConnector;
import com.stratio.connector.streaming.core.connection.StreamingConnectionHandler;
import com.stratio.connector.streaming.core.engine.StreamingMetadataEngine;
import com.stratio.connector.streaming.core.engine.StreamingQueryEngine;
import com.stratio.connector.streaming.core.engine.StreamingStorageEngine;
import com.stratio.connector.streaming.core.procces.ConnectorProcessHandler;
import com.stratio.meta.common.connector.ConnectorClusterConfig;
import com.stratio.meta.common.connector.IConfiguration;
import com.stratio.meta.common.connector.IMetadataEngine;
import com.stratio.meta.common.connector.IQueryEngine;
import com.stratio.meta.common.connector.IStorageEngine;
import com.stratio.meta.common.connector.Operations;
import com.stratio.meta.common.exceptions.ConnectionException;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.logicalplan.Filter;
import com.stratio.meta.common.logicalplan.LogicalStep;
import com.stratio.meta.common.logicalplan.LogicalWorkflow;
import com.stratio.meta.common.logicalplan.Project;
import com.stratio.meta.common.logicalplan.Select;
import com.stratio.meta.common.statements.structures.relationships.Operator;
import com.stratio.meta.common.statements.structures.relationships.Relation;
import com.stratio.meta2.common.data.ClusterName;
import com.stratio.meta2.common.data.ColumnName;
import com.stratio.meta2.common.data.TableName;
import com.stratio.meta2.common.metadata.ColumnType;
import com.stratio.meta2.common.statements.structures.selectors.ColumnSelector;

/**
 * This class implements the connector for Streaming.
 */
public class StreamingConnector extends CommonsConnector {


    private transient ConnectorProcessHandler processHandler;

    /**
     * The Log.
     */
    final Logger logger = LoggerFactory.getLogger(this.getClass());



    /**
     * Create a connection to Streaming. The client will be a transportClient by default unless stratio nodeClient is
     * specified.
     *
     * @param configuration
     *            the connection configuration. It must be not null.
     */

    @Override
    public void init(IConfiguration configuration) {

        connectionHandler = new StreamingConnectionHandler(configuration);

        processHandler = new ConnectorProcessHandler();

    }

    @Override
    public String getConnectorName() {
        return "Streaming";
    }

    /**
     * Return the DataStore Name.
     *
     * @return DataStore Name
     */
    @Override
    public String[] getDatastoreName() {
        return new String[] { "Streaming" };
    }

    /**
     * Return the StorageEngine.
     *
     * @return the StorageEngine
     */
    @Override
    public IStorageEngine getStorageEngine() {

        return new StreamingStorageEngine(connectionHandler);

    }

    /**
     * Return the QueryEngine.
     *
     * @return the QueryEngine
     */
    @Override
    public IQueryEngine getQueryEngine() {

        return new StreamingQueryEngine(connectionHandler, processHandler);
    }

    /**
     * Return the MetadataEngine.
     *
     * @return the MetadataEngine
     */
    @Override
    public IMetadataEngine getMetadataEngine() {
        return new StreamingMetadataEngine(connectionHandler);
    }

    public static void main(String args[]) { // TODO

        try {
            String ZOOKEEPER_SERVER = "10.200.0.58"; // "10.200.0.58";//"192.168.0.2";
            String KAFKA_SERVER = "10.200.0.58";// "10.200.0.58";//"192.168.0.2";
            String KAFKA_PORT = "9092";
            String ZOOKEEPER_PORT = "2181";

            StreamingConnector sC = new StreamingConnector();
            sC.init(null);

            Map<String, String> optionsNode = new HashMap<>();

            optionsNode.put("KafkaServer", KAFKA_SERVER);
            optionsNode.put("KafkaPort", KAFKA_PORT);
            optionsNode.put("zooKeeperServer", ZOOKEEPER_SERVER);
            optionsNode.put("zooKeeperPort", ZOOKEEPER_PORT);

            ConnectorClusterConfig config = new ConnectorClusterConfig(new ClusterName("CLUSTERNAME"), optionsNode);

            sC.connect(null, config);
            List<ColumnName> columns = new ArrayList<>();
            columns.add(new ColumnName("testC", "testT", "name1"));
            columns.add(new ColumnName("testC", "testT", "name2"));
            Project project = new Project(Operations.PROJECT, new TableName("testC", "testT"), new ClusterName(
                            "CLUSTERNAME"), columns);
            Map<String, ColumnType> type = new LinkedHashMap<>();
            type.put("name1", ColumnType.VARCHAR);
            type.put("name2", ColumnType.VARCHAR);
            Map<ColumnName, String> columnsAlias = new LinkedHashMap<>();
            columnsAlias.put(new ColumnName("testC", "testT", "name1"), "name1alias");
            columnsAlias.put(new ColumnName("testC", "testT", "name2"), "name2alias");

            Relation condition = new Relation(new ColumnSelector(new ColumnName("testC", "testT", "name2")),
                            Operator.EQ, new ColumnSelector(new ColumnName("testC", "testT", "name3")));
            Filter filter = new Filter(Operations.FILTER_NON_INDEXED_EQ, condition);

            /*
             * Relation condition2 = new Relation(new ColumnSelector(new ColumnName("testC", "testT", "name2")),
             * Operator.EQ, new StringSelector("value2_R1")); Filter filter2 = new
             * Filter(Operations.FILTER_NON_INDEXED_EQ, condition2);
             */

            Select select = new Select(Operations.SELECT_WINDOW, columnsAlias, type);
            project.setNextStep(filter);

            // filter.setNextStep(filter2);
            // filter2.setNextStep(select);
            filter.setNextStep(select);
            /**
             *
             Select select = new Select(Operations.SELECT_WINDOW, columnsAlias, type); project.setNextStep(select);
             */
            List<LogicalStep> initialStep = new ArrayList<>();
            initialStep.add(project);
            LogicalWorkflow lWF = new LogicalWorkflow(initialStep);
            sC.getQueryEngine().execute(lWF);
        } catch (ConnectionException e) {
            e.printStackTrace();
        } catch (UnsupportedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

    }

}
