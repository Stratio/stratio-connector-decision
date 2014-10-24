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

package com.stratio.connector.streaming.core.procces;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.streaming.core.procces.exception.ConnectionProcessException;
import com.stratio.crossdata.common.exceptions.ExecutionException;

/**
 * This clas handle a process.
 * Created by jmgomez on 3/10/14.
 */
public class ConnectorProcessHandler {

    /**
     * The log.
     */
    private final transient Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * The process map.
     */
    private Map<String, ThreadProcess> processMap = new HashMap<>();

    /**
     * Start the process.
     *
     * @param queryId          the query id.
     * @param connectorProcess a connector procces.
     * @throws ConnectionProcessException if the connection fail.
     */
    public void strartProcess(String queryId, ConnectorProcess connectorProcess) throws ConnectionProcessException {
        if (processMap.containsKey(queryId)) {
            String msg = "The processMap with id " + queryId + " already exists ";
            logger.error(msg);
            throw new ConnectionProcessException(msg);
        }
        Thread thread = new Thread(connectorProcess, "[StreamingQuery-" + queryId + "]");
        processMap.put(queryId, new ThreadProcess(thread, connectorProcess));
        thread.start();
    }

    /**
     * Return the query process.
     *
     * @param queryId the query id.
     * @return the process.
     * @throws ConnectionProcessException if a error happens.
     */
    public ConnectorProcess getProcess(String queryId) throws ConnectionProcessException {
        if (!processMap.containsKey(queryId)) {
            String msg = "The processMap with id " + queryId + " not exists ";
            logger.error(msg);
            throw new ConnectionProcessException(msg);
        }
        return processMap.get(queryId).getProcess();
    }

    /**
     * Stop the process.
     *
     * @param queryId the queryId.
     * @throws ConnectionProcessException if any error happens in the process.
     * @throws ExecutionException         if a execurion error happens.
     */
    public void stopProcess(String queryId) throws ConnectionProcessException, ExecutionException {
        ThreadProcess threadProcess = processMap.get(queryId);

        threadProcess.getProcess().endQuery();
        threadProcess.getThread().interrupt();
        processMap.remove(queryId);

    }

}

/**
 * A class to envelope the tread and the connectorProcess.
 */
class ThreadProcess {

    /**
     * The thread.
     */
    private Thread thread;
    /**
     * The connector process.
     */
    private ConnectorProcess process;

    /**
     * Constructor.
     *
     * @param thread  the thread.
     * @param process the process.
     */
    ThreadProcess(Thread thread, ConnectorProcess process) {
        this.thread = thread;
        this.process = process;
    }

    /**
     * Return the thread.
     *
     * @return the thread.
     */
    public Thread getThread() {
        return thread;
    }

    /**
     * Return the process.
     *
     * @return the preocess.
     */
    public ConnectorProcess getProcess() {
        return process;
    }

}