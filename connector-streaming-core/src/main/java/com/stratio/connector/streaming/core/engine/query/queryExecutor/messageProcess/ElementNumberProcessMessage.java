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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.stratio.connector.streaming.core.engine.query.ConnectorQueryData;
import com.stratio.connector.streaming.core.engine.query.util.ResultsetCreator;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.exceptions.UnsupportedException;

/**
 * Created by jmgomez on 7/10/14.
 */
public class ElementNumberProcessMessage implements ProcessMessage {

    /**
     * The window length
     */
    private int windowLength;

    private List<Row> list = Collections.synchronizedList(new ArrayList());

    private ResultsetCreator resultsetCreator;

    /**
     * @param queryData
     * @throws com.stratio.meta.common.exceptions.UnsupportedException
     */
    public ElementNumberProcessMessage(ConnectorQueryData queryData,
            ResultsetCreator resultsetCreator)
            throws UnsupportedException {

        this.resultsetCreator = resultsetCreator;
        windowLength = queryData.getWindow().getNumRows();

    }

    @Override
    public void processMessage(Row row) {

        boolean isWindowReady = false;
        ArrayList<Row> copyNotSyncrhonizedList = null;
        synchronized (list) { // TODO ver si se puede sincronizar menos
            list.add(row);
            isWindowReady = (list.size() == windowLength);
            if (isWindowReady) {
                copyNotSyncrhonizedList = new ArrayList<>(list);
                list.clear();
            }
        }

        if (isWindowReady) {
            resultsetCreator.createResultSet(copyNotSyncrhonizedList).send();
        }
    }

    @Override public void end() {

    }

}
