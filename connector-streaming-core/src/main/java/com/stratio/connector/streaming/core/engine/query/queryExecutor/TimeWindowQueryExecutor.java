package com.stratio.connector.streaming.core.engine.query.queryExecutor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.streaming.core.engine.query.ConnectorQueryData;
import com.stratio.connector.streaming.core.engine.query.queryExecutor.timer.SendResultTimer;
import com.stratio.meta.common.connector.IResultHandler;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.streaming.api.IStratioStreamingAPI;
import com.stratio.streaming.commons.exceptions.StratioAPISecurityException;
import com.stratio.streaming.commons.exceptions.StratioEngineOperationException;
import com.stratio.streaming.commons.exceptions.StratioEngineStatusException;

/**
 * Created by jmgomez on 7/10/14.
 */
public class TimeWindowQueryExecutor extends ConnectorQueryExecutor {

    private Timer timer;

    private List<Row> list = Collections.synchronizedList(new ArrayList<Row>());
    boolean isInterrupted = false;

    /**
     * @param queryData
     * @throws UnsupportedException
     */
    public TimeWindowQueryExecutor(ConnectorQueryData queryData, IResultHandler resultHandler)
                    throws UnsupportedException {
        super(queryData, resultHandler);

        TimerTask timerTask = new SendResultTimer(this);

        timer = new Timer("[Timer_"+queryData.getQueryId()+"]",true);
        timer.scheduleAtFixedRate(timerTask, 0, queryData.getWindow().getDurationInMilliseconds());

    }

    @Override
    public void endQuery(String streamName, Connection<IStratioStreamingAPI> connection)
                    throws StratioEngineStatusException, StratioAPISecurityException, StratioEngineOperationException {

        super.endQuery(streamName, connection);
        isInterrupted = true;
        timer.cancel();

    }

    @Override
    protected void processMessage(Row row) {


        synchronized (list) {
            list.add(row);
        }
    }

    public void sendMessages() {
        if (!isInterrupted) {

            List<Row> copyNotSyncrhonizedList;
            synchronized (list) {
                copyNotSyncrhonizedList = new ArrayList<>(list);
                list.clear();
            }

            sendResultSet(copyNotSyncrhonizedList);
        }

    }
}
