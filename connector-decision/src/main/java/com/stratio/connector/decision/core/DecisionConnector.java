/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.  The STRATIO (C) licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.stratio.connector.decision.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.commons.CommonsConnector;
import com.stratio.connector.decision.core.connection.DecisionConnectionHandler;
import com.stratio.connector.decision.core.engine.DecisionMetadataEngine;
import com.stratio.connector.decision.core.engine.DecisionQueryEngine;
import com.stratio.connector.decision.core.engine.DecisionStorageEngine;
import com.stratio.connector.decision.core.procces.ConnectorProcessHandler;
import com.stratio.crossdata.common.connector.IConfiguration;
import com.stratio.crossdata.common.connector.IMetadataEngine;
import com.stratio.crossdata.common.connector.IQueryEngine;
import com.stratio.crossdata.common.connector.IStorageEngine;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.InitializationException;
import com.stratio.crossdata.connectors.ConnectorApp;

/**
 * This class implements the connector for Decision.
 */
public class DecisionConnector extends CommonsConnector {

    /**
     * The Log.
     */
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    /**
     * The process handler.
     */
    private transient ConnectorProcessHandler processHandler;

    /**
     * Constructor.
     *
     * @throws InitializationException
     *             if an error happens in the init.
     */
    public DecisionConnector() throws InitializationException {
        super("/DecisionConnector.xml","/DecisionDataStore.xml");
    }

    /**
     * REstar the connector.
     * @throws ExecutionException if an exception happens.
     */
    @Override
    public void restart() throws ExecutionException {

    }

    /**
     * The main for the agent.
     *
     * @param args
     *            init arguments.
     * @throws InitializationException
     *             in an error happens in init.
     */
    public static void main(String[] args) throws InitializationException {
        DecisionConnector decisionConnector = new DecisionConnector();
        ConnectorApp connectorApp = new ConnectorApp();
        connectorApp.startup(decisionConnector);
        decisionConnector.attachShutDownHook();
    }

    /**
     * Create a connection to Decision. The client will be a transportClient by default unless stratio nodeClient is
     * specified.
     *
     * @param configuration
     *            the connection configuration. It must be not null.
     */

    @Override
    public void init(IConfiguration configuration) {

        connectionHandler = new DecisionConnectionHandler(configuration);

        processHandler = new ConnectorProcessHandler();

    }


    /**
     * Return the StorageEngine.
     *
     * @return the StorageEngine
     */
    @Override
    public IStorageEngine getStorageEngine() {

        return new DecisionStorageEngine(connectionHandler);

    }

    /**
     * Return the QueryEngine.
     *
     * @return the QueryEngine
     */
    @Override
    public IQueryEngine getQueryEngine() {

        return new DecisionQueryEngine(connectionHandler, processHandler);
    }

    /**
     * Return the MetadataEngine.
     *
     * @return the MetadataEngine
     */
    @Override
    public IMetadataEngine getMetadataEngine() {
        return new DecisionMetadataEngine(connectionHandler);
    }



    /**
     * Attach shut down hook.
     */
    public void attachShutDownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    shutdown();
                } catch (ExecutionException e) {
                    logger.error("Fail ShutDown");
                }
            }
        });
    }


}
