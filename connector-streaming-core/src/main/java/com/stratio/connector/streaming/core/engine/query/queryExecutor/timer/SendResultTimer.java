package com.stratio.connector.streaming.core.engine.query.queryExecutor.timer;

import java.util.Date;
import java.util.TimerTask;

import com.stratio.connector.streaming.core.engine.query.queryExecutor.TimeWindowQueryExecutor;

/**
 * Created by jmgomez on 9/10/14.
 */
public class SendResultTimer extends TimerTask {
    TimeWindowQueryExecutor timeWindowQueryExecutor;
    public SendResultTimer(TimeWindowQueryExecutor timeWindowQueryExecutor) {
        this.timeWindowQueryExecutor = timeWindowQueryExecutor;
    }

    @Override
    public void run() {
        timeWindowQueryExecutor.sendMessages();
    }
}
