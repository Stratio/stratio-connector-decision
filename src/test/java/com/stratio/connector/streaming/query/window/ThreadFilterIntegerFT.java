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

package com.stratio.connector.streaming.query.window;

import static org.junit.Assert.assertEquals;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.streaming.bean.StreamingBean;
import com.stratio.connector.streaming.ftest.GenericStreamingTest;
import com.stratio.connector.streaming.ftest.thread.actions.RowToInsertDefault;
import com.stratio.connector.streaming.query.window.theadHelper.StreamingThreadInserter;
import com.stratio.connector.streaming.query.window.theadHelper.StreamingThreadRead;
import com.stratio.crossdata.common.connector.IResultHandler;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.logicalplan.LogicalWorkflow;
import com.stratio.crossdata.common.metadata.TableMetadata;
import com.stratio.crossdata.common.result.QueryResult;
import com.stratio.crossdata.common.statements.structures.window.WindowType;

public class ThreadFilterIntegerFT extends GenericStreamingTest {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());


    public static final int CORRECT_ELMENT_TO_FIND = 90; // Must be a time window
    private static final int WAIT_TIME = 20;

    TableMetadata tableMetadata;
    int numberDefaultText = 0;
    int numberAlternativeText = 0;

    private int correctValueCount = 0;
    private int incorrectValueCount = 0;
    public static final int CORRECT_VALUE = 6;


    @Before
    public void setUp() throws ConnectorException {
        super.setUp();
        numberDefaultText = 0;
        numberAlternativeText = 0;
        correctValueCount = 0;
        incorrectValueCount = 0;
        StreamingBean.recalculateTableName();

        tableMetadata = StreamingBean.getTableMetadata(CATALOG,getClusterName());
        try {
            sConnector.getMetadataEngine().createTable(getClusterName(), tableMetadata);

        } catch (ExecutionException t) {
            logger.info("The table was created yet");
        }

    }

    @After
    public void tearDown(){
        try {
            sConnector.getMetadataEngine().dropTable(getClusterName(),new TableName(CATALOG,StreamingBean.TABLE_NAME));
        } catch (ConnectorException e) {
            logger.info("Error drop table");
            throw new RuntimeException(e);
        }
    }

    @Test
    public void lowerFilterTest() throws InterruptedException, UnsupportedException {


        int valueNoMatchInQuery = CORRECT_VALUE +1;

        LogicalWorkflow logicalWorkflow = StreamingBean.getSelectAllWF(CATALOG, getClusterName())
                .addNLowerFilter(StreamingBean.AGE,
                        CORRECT_VALUE +1, false).addWindow(WindowType.TEMPORAL, 5).getLogicalWorkflow();
        StreamingThreadRead stremingRead = new StreamingThreadRead(sConnector,logicalWorkflow,
                        new ResultNumberHandler(CORRECT_VALUE, valueNoMatchInQuery));

        stremingRead.start();
        waitSeconds(WAIT_TIME);

        StreamingThreadInserter stramingInserter1 = new StreamingThreadInserter(sConnector, getClusterName(),
                 new RowToInsertDefault(),CORRECT_VALUE, CATALOG);
        stramingInserter1.numOfElement(CORRECT_ELMENT_TO_FIND);
        stramingInserter1.start();
        StreamingThreadInserter stramingInserter = stramingInserter1;

        StreamingThreadInserter otherStreamingInserter = new StreamingThreadInserter(sConnector, getClusterName(),
                 new RowToInsertDefault(),valueNoMatchInQuery, CATALOG);
        otherStreamingInserter.start();

        waitSeconds(WAIT_TIME);
        stremingRead.end();


        waitSeconds(WAIT_TIME);


        otherStreamingInserter.end();
        stramingInserter.end();


        stremingRead.join();
        stramingInserter.join();
        otherStreamingInserter.join();

        assertEquals("We must not find beans with age = "+valueNoMatchInQuery, 0, incorrectValueCount);
        assertEquals("We must found "+CORRECT_ELMENT_TO_FIND+" beans with age ="+CORRECT_VALUE, CORRECT_ELMENT_TO_FIND,
                correctValueCount);

    }



    @Test
    public void greatFilterTest() throws InterruptedException, UnsupportedException {


        int valueNoMatchInQuery = CORRECT_VALUE -1;
        LogicalWorkflow logicalWorkflow = StreamingBean.getSelectAllWF(CATALOG, getClusterName())
                .addGreaterFilter(StreamingBean.AGE,CORRECT_VALUE -1, false).addWindow(WindowType.TEMPORAL, 5)
                .getLogicalWorkflow();

        StreamingThreadRead stremingRead = new StreamingThreadRead(sConnector,logicalWorkflow,
                new ResultNumberHandler(CORRECT_VALUE, valueNoMatchInQuery));


        stremingRead.start();

        waitSeconds(WAIT_TIME);


        StreamingThreadInserter streamingInserter = new StreamingThreadInserter(sConnector, getClusterName(),
                 new RowToInsertDefault(),CORRECT_VALUE, CATALOG);
        streamingInserter.numOfElement(CORRECT_ELMENT_TO_FIND);
        streamingInserter.start();

        StreamingThreadInserter otherStreamingInserter = new StreamingThreadInserter(sConnector, getClusterName(),
                 new RowToInsertDefault(),valueNoMatchInQuery, CATALOG);


        otherStreamingInserter.start();

        waitSeconds(WAIT_TIME);

        stremingRead.end();
        otherStreamingInserter.end();
        streamingInserter.end();

        stremingRead.join();
        otherStreamingInserter.join();
        streamingInserter.join();


        assertEquals("We must not find beans with age = "+valueNoMatchInQuery, 0, incorrectValueCount);
        assertEquals("We must found "+CORRECT_ELMENT_TO_FIND+" beans with age ="+CORRECT_VALUE, CORRECT_ELMENT_TO_FIND,
                correctValueCount);


    }

    @Test
    public void greatEqualsFilterTest() throws InterruptedException, UnsupportedException {


        int valueNoMatchInQuery = CORRECT_VALUE - 1;
        LogicalWorkflow logicalWorkflow = StreamingBean.getSelectAllWF(CATALOG, getClusterName())
                .addGreaterEqualFilter(StreamingBean.AGE, CORRECT_VALUE, false, false).addWindow(WindowType.TEMPORAL, 5)
                .getLogicalWorkflow();


        StreamingThreadRead stremingRead = new StreamingThreadRead(sConnector, logicalWorkflow,
                        new ResultNumberHandler(CORRECT_VALUE, valueNoMatchInQuery));

        stremingRead.start();
        waitSeconds(WAIT_TIME);


        StreamingThreadInserter streamingInserter = new StreamingThreadInserter(sConnector, getClusterName(),  new
                RowToInsertDefault(),CORRECT_VALUE,CATALOG);
        streamingInserter.numOfElement(CORRECT_ELMENT_TO_FIND);
        streamingInserter.start();


        StreamingThreadInserter otherStreamingInserter = new StreamingThreadInserter(sConnector, getClusterName(),
                new RowToInsertDefault(), valueNoMatchInQuery,CATALOG);


        otherStreamingInserter.start();

        waitSeconds(WAIT_TIME);

        stremingRead.end();

        waitSeconds(WAIT_TIME);
        otherStreamingInserter.end();
        streamingInserter.end();

        stremingRead.join();
        otherStreamingInserter.join();
        streamingInserter.join();

        assertEquals("We must not find beans with age = "+valueNoMatchInQuery, 0, incorrectValueCount);
        assertEquals("We must found "+CORRECT_ELMENT_TO_FIND+" beans with age ="+CORRECT_VALUE, CORRECT_ELMENT_TO_FIND,
                correctValueCount);


    }

    @Test
    public void lowerEqualsFilterTest() throws InterruptedException, UnsupportedException {

        int valueNoMatchInQuery = CORRECT_VALUE + 1;

        LogicalWorkflow logicalWorkflow = StreamingBean.getSelectAllWF(CATALOG, getClusterName())
                .addLowerEqualFilter(StreamingBean.AGE, CORRECT_VALUE, false).addWindow(WindowType.TEMPORAL, 5)
                .getLogicalWorkflow();

        StreamingThreadRead stremingRead = new StreamingThreadRead(sConnector, logicalWorkflow,
                        new ResultNumberHandler(CORRECT_VALUE, valueNoMatchInQuery));

        stremingRead.start();
        waitSeconds(WAIT_TIME);


        StreamingThreadInserter streamingInserter = new StreamingThreadInserter(sConnector, getClusterName(),
                new RowToInsertDefault(),CORRECT_VALUE, CATALOG);

        streamingInserter.numOfElement(CORRECT_ELMENT_TO_FIND);
        streamingInserter.start();

        StreamingThreadInserter otherStreamingInserter = new StreamingThreadInserter(sConnector, getClusterName(),
                new RowToInsertDefault(),valueNoMatchInQuery, CATALOG);

        otherStreamingInserter.start();

        waitSeconds(WAIT_TIME);

        stremingRead.end();
        waitSeconds(WAIT_TIME);



        otherStreamingInserter.end();
        streamingInserter.end();

        stremingRead.join();
        otherStreamingInserter.join();
        streamingInserter.join();

        assertEquals("We must not find beans with age = "+valueNoMatchInQuery, 0, incorrectValueCount);
        assertEquals("We must found "+CORRECT_ELMENT_TO_FIND+" beans with age ="+CORRECT_VALUE, CORRECT_ELMENT_TO_FIND,
                correctValueCount);

    }



    private class ResultNumberHandler implements IResultHandler {

        int correctValue;
        int incorrectValue;

        public ResultNumberHandler(int correctValue, int incorrectValue) {
            this.correctValue = correctValue;
            this.incorrectValue = incorrectValue;
        }

        @Override
        public void processException(String queryId, ExecutionException exception) {
            System.out.println(queryId + " " + exception.getMessage());
            exception.printStackTrace();
        }

        @Override
        public void processResult(QueryResult result) {


            for (Row row : result.getResultSet()) {
                int value = (int) row.getCell(StreamingBean.AGE).getValue();
                if (correctValue == value) {
                    correctValueCount++;
                } else if (incorrectValue == value) {
                    // If streaming read random init value
                    incorrectValueCount++;
                }

            }

        }

    }

}
