package com.stratio.connector.streaming.ftest.thread;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.stratio.connector.commons.ftest.schema.TableMetadataBuilder;
import com.stratio.connector.commons.ftest.workFlow.LogicalWorkFlowCreator;
import com.stratio.connector.streaming.core.StreamingConnector;
import com.stratio.connector.streaming.ftest.GenericStreamingTest;
import com.stratio.connector.streaming.ftest.thread.actions.StreamingInserter;
import com.stratio.connector.streaming.ftest.thread.actions.StreamingRead;
import com.stratio.meta.common.connector.ConnectorClusterConfig;
import com.stratio.meta.common.connector.IResultHandler;
import com.stratio.meta.common.data.Cell;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.exceptions.ConnectionException;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.InitializationException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.logicalplan.LogicalWorkflow;
import com.stratio.meta.common.logicalplan.Select;
import com.stratio.meta.common.metadata.structures.ColumnMetadata;
import com.stratio.meta.common.result.QueryResult;
import com.stratio.meta.common.statements.structures.window.WindowType;
import com.stratio.meta2.common.data.ClusterName;
import com.stratio.meta2.common.data.ColumnName;
import com.stratio.meta2.common.data.TableName;
import com.stratio.meta2.common.metadata.ColumnType;
import com.stratio.meta2.common.metadata.TableMetadata;

public class ThreadFilterFunctionalTest extends GenericStreamingTest{

    private static final String TEXT = "Text";
    public static final int CORRECT_ELMENT_TO_FIND = 90; //Must be a time window

    private static final String OTHER_TEXT = "OTHER...... ";
    private static final int WAIT_TIME = 20;



    Set<Integer> returnSet = new HashSet<>();


    TableMetadata tableMetadata;



    Boolean filterCorrect = true;
    int number = 0;

    @Before
    public void setUp() throws ConnectionException, UnsupportedException, ExecutionException, InitializationException {
         super.setUp();

        returnSet = new HashSet<>();

        TableMetadataBuilder tableMetadataBuilder = new TableMetadataBuilder(CATALOG, TABLE);
        tableMetadata = tableMetadataBuilder.addColumn(STRING_COLUMN,
                ColumnType.VARCHAR).addColumn(INTEGER_COLUMN,
                ColumnType.INT).addColumn(BOOLEAN_COLUMN,ColumnType.BOOLEAN).build();
        try {
            sConnector.getMetadataEngine().createTable(getClusterName(), tableMetadata);

        } catch (ExecutionException t) {

        }

    }


    @Test
    public void testEqualFilter() throws InterruptedException, UnsupportedException {




        StreamingRead stremingRead = new StreamingRead(sConnector, getClusterName(), tableMetadata,
                createEqualLogicalWorkFlow(),
                        new ResultHandler());

        stremingRead.start();
        System.out.println("TEST ********************** Querying......");
        waitSeconds(WAIT_TIME);

        System.out.println("TEST ********************** Inserting ......");
        StreamingInserter stramingInserter = new StreamingInserter(sConnector, getClusterName(), tableMetadata);
        stramingInserter.numOfElement(CORRECT_ELMENT_TO_FIND);
        stramingInserter.start();

        StreamingInserter oherStreamingInserter = new StreamingInserter(sConnector, getClusterName(), tableMetadata);
        oherStreamingInserter.changeOtuput(OTHER_TEXT);
        oherStreamingInserter.start();

        waitSeconds(WAIT_TIME);

        stremingRead.end();
        System.out.println("TEST ********************** END Querying Test......");
        waitSeconds(WAIT_TIME);

        System.out.println("TEST ********************** Change Test Querying......");
        waitSeconds(WAIT_TIME);

        System.out.println("TEST ********************** END Insert......");
        oherStreamingInserter.end();
        stramingInserter.end();
        waitSeconds(WAIT_TIME);


        assertTrue("The filter works",filterCorrect);
        assertEquals("All correct elements have been found", CORRECT_ELMENT_TO_FIND, number);


    }

    private LogicalWorkflow createEqualLogicalWorkFlow() throws UnsupportedException {
        LogicalWorkFlowCreator logicalWorkFlowCreator = new LogicalWorkFlowCreator(CATALOG, TABLE,
                getClusterName());

        LinkedList<LogicalWorkFlowCreator.ConnectorField> selectColumns = new LinkedList<>();
        selectColumns.add(logicalWorkFlowCreator.createConnectorField(STRING_COLUMN,STRING_COLUMN, ColumnType.TEXT));
        selectColumns.add(logicalWorkFlowCreator.createConnectorField(INTEGER_COLUMN,INTEGER_COLUMN,ColumnType.INT));
        selectColumns.add(logicalWorkFlowCreator.createConnectorField(BOOLEAN_COLUMN,BOOLEAN_COLUMN,ColumnType.BOOLEAN));

        return logicalWorkFlowCreator.addColumnName(STRING_COLUMN).addColumnName
                (INTEGER_COLUMN).addColumnName(BOOLEAN_COLUMN).addSelect(selectColumns).addEqualFilter(STRING_COLUMN,
                TEXT,false,false).addWindow(WindowType.TEMPORAL,10)
                        .getLogicalWorkflow();
    }

    private class ResultHandler implements IResultHandler {




        @Override
        public void processException(String queryId, ExecutionException exception) {
            System.out.println(queryId + " " + exception.getMessage());
            exception.printStackTrace();
        }




        @Override
        public void processResult(QueryResult result) {


            for (Row row : result.getResultSet()) {
                System.out.println("********************>>"+row.getCell(STRING_COLUMN).getValue());
                if (!TEXT.equals(row.getCell(STRING_COLUMN).getValue())){
                    filterCorrect = false;
                }else{
                    number++;
                }

            }

        }



    }

}
