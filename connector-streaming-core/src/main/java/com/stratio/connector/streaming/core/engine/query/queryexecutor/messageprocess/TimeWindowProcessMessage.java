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

package com.stratio.connector.streaming.core.engine.query.queryexecutor.messageprocess;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import com.stratio.connector.streaming.core.engine.query.ConnectorQueryData;
import com.stratio.connector.streaming.core.engine.query.queryexecutor.timer.SendResultTimer;
import com.stratio.connector.streaming.core.engine.query.util.ResultsetCreator;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.exceptions.UnsupportedException;

/**
 * Created by jmgomez on 7/10/14.
 */
public class TimeWindowProcessMessage implements ProcessMessage {

    private final ResultsetCreator resultsetCreator;
    private boolean isInterrupted = false;
    private Timer timer;
    private List<Row> list = Collections.synchronizedList(new ArrayList<Row>());

    /**
     * @param queryData
     * @throws com.stratio.meta.common.exceptions.UnsupportedException
     */
    public TimeWindowProcessMessage(ConnectorQueryData queryData, ResultsetCreator resultsetCreator)
            throws UnsupportedException {

        TimerTask timerTask = new SendResultTimer(this);

        this.resultsetCreator = resultsetCreator;
        timer = new Timer("[Timer_" + queryData.getQueryId() + "]", true);
        timer.scheduleAtFixedRate(timerTask, queryData.getWindow().getDurationInMilliseconds(), queryData.getWindow()
                .getDurationInMilliseconds());

    }

    @Override
    public void processMessage(Row row) {

        synchronized (list) {
            list.add(row);
        }

    }

    @Override
    public void end() {
        isInterrupted = true;
        timer.cancel();
    }

    public void sendMessages() {
        if (!isInterrupted) {

            List<Row> copyNotSyncrhonizedList;
            synchronized (list) {
                copyNotSyncrhonizedList = new ArrayList<>(list);
                list.clear();
            }

            resultsetCreator.createResultSet(copyNotSyncrhonizedList).send();
        }

    }
}
