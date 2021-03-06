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

package com.stratio.connector.decision.core.engine.query.queryexecutor.messageprocess;

import com.stratio.connector.decision.core.engine.query.ConnectorQueryData;
import com.stratio.connector.decision.core.engine.query.util.ResultsetCreator;
import com.stratio.connector.decision.core.exception.ExecutionValidationException;

/**
 * A process manager factory.
 */
public final class ProcessMessageFactory {

    private ProcessMessageFactory() {
    }

    /**
     * Return the correct process manager.
     *
     * @param queryData
     *            the query data.
     * @param resultSetCreator
     *            the result creator.
     * @return the correct configure processMessage.
     * @throws ExecutionValidationException
     *             if an error happens.
     */
    public static ProcessMessage getProccesMessage(ConnectorQueryData queryData, ResultsetCreator resultSetCreator)
                    throws ExecutionValidationException {
        ProcessMessage connectorQueryExecutor = null;

        switch (queryData.getWindow().getType()) {
        case TEMPORAL:
            connectorQueryExecutor = new TimeWindowProcessMessage(queryData, resultSetCreator);
            break;
        case NUM_ROWS:
            connectorQueryExecutor = new ElementNumberProcessMessage(queryData, resultSetCreator);
            break;
        default:
            throw new ExecutionValidationException("Window " + queryData.getWindow().getType() + "is not supported");

        }
        return connectorQueryExecutor;
    }
}
