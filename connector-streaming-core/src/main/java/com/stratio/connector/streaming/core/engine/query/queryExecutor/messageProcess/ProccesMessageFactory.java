/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 *  See the NOTICE file distributed with this work for additional information
 *  regarding copyright ownership. The STRATIO (C) licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package com.stratio.connector.streaming.core.engine.query.queryExecutor.messageProcess;

import com.stratio.connector.streaming.core.engine.query.ConnectorQueryData;
import com.stratio.connector.streaming.core.engine.query.queryExecutor.ConnectorQueryExecutor;
import com.stratio.connector.streaming.core.engine.query.queryExecutor.ElementNumberQueryExecutor;
import com.stratio.connector.streaming.core.engine.query.queryExecutor.TimeWindowQueryExecutor;
import com.stratio.connector.streaming.core.engine.query.util.ResultsetCreator;
import com.stratio.meta.common.connector.IResultHandler;
import com.stratio.meta.common.exceptions.UnsupportedException;

/**
 * Created by jmgomez on 16/10/14.
 */
public class ProccesMessageFactory {
    public static ProcessMessage getProccesMessage(ConnectorQueryData queryData, ResultsetCreator resultSetCreator)
            throws UnsupportedException {
        ProcessMessage connectorQueryExecutor = null;

        switch (queryData.getWindow().getType()) {
        case TEMPORAL:
            connectorQueryExecutor = new TimeWindowProcessMessage(queryData, resultSetCreator);
            break;
        case NUM_ROWS:
            connectorQueryExecutor = new ElementNumberProcessMessage(queryData, resultSetCreator);
            break;
        default:
            throw new UnsupportedException("Window " + queryData.getWindow().getType() + "is not supported");

        }
        return connectorQueryExecutor;
    }
}
