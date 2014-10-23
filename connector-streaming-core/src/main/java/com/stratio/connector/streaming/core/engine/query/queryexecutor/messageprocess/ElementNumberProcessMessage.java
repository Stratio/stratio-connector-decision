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

package com.stratio.connector.streaming.core.engine.query.queryexecutor.messageprocess;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.stratio.connector.streaming.core.engine.query.ConnectorQueryData;
import com.stratio.connector.streaming.core.engine.query.util.ResultsetCreator;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.exceptions.UnsupportedException;

/**
 * This class represent a message processor by element number.
 * Created by jmgomez on 7/10/14.
 */
public class ElementNumberProcessMessage implements ProcessMessage {

    /**
     * The window length
     */
    private int windowLength;

    /**
     * The row temporal store.
     */
    private List<Row> rowTemporalStore = Collections.synchronizedList(new ArrayList());

    /**
     * The resultSet creator.
     */
    private ResultsetCreator resultsetCreator;

    /**
     * constructor.
     * @param queryData the querydata.
     * @param resultsetCreator the resultSet creator.
     * @throws UnsupportedException if an error happens.
     */
    public ElementNumberProcessMessage(ConnectorQueryData queryData, ResultsetCreator resultsetCreator)
            throws UnsupportedException {

        this.resultsetCreator = resultsetCreator;
        windowLength = queryData.getWindow().getNumRows();

    }

    /**
     * This method process a row.
     * @param row a row.
     */
    @Override
    public void processMessage(Row row) {

        boolean isWindowReady = false;
        ArrayList<Row> copyNotSyncrhonizedList = null;
        synchronized (rowTemporalStore) {
            rowTemporalStore.add(row);
            isWindowReady = (rowTemporalStore.size() == windowLength);
            if (isWindowReady) {
                copyNotSyncrhonizedList = new ArrayList<>(rowTemporalStore);
                rowTemporalStore.clear();
            }
        }

        if (isWindowReady) {
            resultsetCreator.create(copyNotSyncrhonizedList).send();
        }
    }

    /**
     * End the process.
     */
    @Override
    public void end() {

    }

}
