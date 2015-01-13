/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership. The STRATIO (C) licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.stratio.connector.streaming.ftest.thread.actions;

import java.util.ArrayList;
import java.util.List;

import com.stratio.connector.streaming.core.StreamingConnector;
import com.stratio.crossdata.common.connector.IStorageEngine;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.TableMetadata;

public class StreamingInserter extends Thread {

    private final RowToInsert rowToInsert;
    StreamingConnector streamingConnector;

    private ClusterName clusterName;
    private TableMetadata stream;
    private boolean finishThread = false;
    private List<ColumnType> typesToInsert = null;
    private long elementPerSecond = 10;
    private long numOfElement = 0;
    private String text = "Text";
    private int integerChangeable = 10;
    private boolean addIntegerChangeable;

    public StreamingInserter(StreamingConnector sC, ClusterName clusterName, TableMetadata stream, RowToInsert
     rowToInsert       ) {
        super("[StreamingInserter]");
        this.streamingConnector = sC;
        this.clusterName = clusterName;
        this.stream = stream;
        this.addIntegerChangeable = false;
        this.rowToInsert = rowToInsert;
    }

    public StreamingInserter elementPerSecond(long elements) {
        this.elementPerSecond = elements;
        return this;
    }

    public StreamingInserter numOfElement(long elements) {
        this.numOfElement = elements;
        return this;
    }

    public StreamingInserter addTypeToInsert(ColumnType type) {
        typesToInsert = (typesToInsert == null) ? new ArrayList<ColumnType>() : typesToInsert;
        typesToInsert.add(type);
        return this;
    }

    public void setAddIntegerChangeable(boolean addIntegerChangeable) {
        this.addIntegerChangeable = addIntegerChangeable;
    }

    @Override
    public void run() {

        if (typesToInsert == null) {
            typesToInsert = new ArrayList<ColumnType>(3);
            typesToInsert.add(ColumnType.BOOLEAN);
            typesToInsert.add(ColumnType.INT);
            typesToInsert.add(ColumnType.VARCHAR);
        }

        try {

            IStorageEngine storageEngine = streamingConnector.getStorageEngine();
            for (int i = 0; !finishThread; i++) {
                if (numOfElement != 0 && numOfElement - 1 == i) {
                    finishThread = true;
                }

                Row row = rowToInsert.getRowToInsert(i, text,typesToInsert,addIntegerChangeable,integerChangeable);

                try {

                    storageEngine.insert(clusterName, stream, row, false);
                } catch (ConnectorException e) {
                    e.printStackTrace();
                }

                if ((i % elementPerSecond) == 0) {
                    Thread.sleep(1000);
                }

            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }


    public void changeStingColumn(String stringOutput) {
        this.text = stringOutput;
    }

    public void changeIntegerChangeableColumn(int integerChangeable) {
        this.integerChangeable = integerChangeable;
    }

    public void end() {
        finishThread = true;
    }

}
