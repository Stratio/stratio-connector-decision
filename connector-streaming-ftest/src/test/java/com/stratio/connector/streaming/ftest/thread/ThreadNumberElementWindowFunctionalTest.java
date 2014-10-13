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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

public class ThreadNumberElementWindowFunctionalTest  extends GenericStreamingTest {

    /**
     * The Log.
     */
    final Logger logger = LoggerFactory.getLogger(this.getClass());
    public static final int ELEMENTS_WRITE = 500;
    private static final int WINDOW_ELEMENTS = 10;



    



    public static String STRING_COLUMN = "string_column";
    public static String INTEGER_COLUMN = "integer_column";
    public static String BOOLEAN_COLUMN = "boolean_column";

    Set<Integer> returnSet = new HashSet<>();


    TableMetadata tableMetadata;



    Integer  windowNumber = 0 ;
    Boolean correctNumberOfElement = true;


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

    @After
    public void tearDown() throws UnsupportedException, ExecutionException {
        sConnector.getMetadataEngine().dropTable(getClusterName(),new TableName(CATALOG,TABLE));
        sConnector.close(getClusterName());
    }



    @Test
    public void tesElmentWindow() throws InterruptedException, UnsupportedException {

        LogicalWorkFlowCreator logicalWorkFlowCreator = new LogicalWorkFlowCreator(CATALOG, TABLE,
                getClusterName());

        LinkedList<LogicalWorkFlowCreator.ConnectorField> selectColumns = new LinkedList<>();
        selectColumns.add(logicalWorkFlowCreator.createConnectorField(STRING_COLUMN,STRING_COLUMN,ColumnType.TEXT));
        selectColumns.add(logicalWorkFlowCreator.createConnectorField(INTEGER_COLUMN,INTEGER_COLUMN,ColumnType.INT));
        selectColumns.add(logicalWorkFlowCreator.createConnectorField(BOOLEAN_COLUMN,BOOLEAN_COLUMN,ColumnType.BOOLEAN));
        LogicalWorkflow logicalWokflow = logicalWorkFlowCreator
                .addColumnName(STRING_COLUMN).addColumnName(INTEGER_COLUMN).addColumnName(BOOLEAN_COLUMN).addSelect
                        (selectColumns).addWindow(WindowType.NUM_ROWS,WINDOW_ELEMENTS)
                .getLogicalWorkflow();

        StreamingRead stremingRead = new StreamingRead(sConnector, getClusterName(), tableMetadata, logicalWokflow,
                new ResultHandler());

        stremingRead.start();
        logger.debug(" ********************** Quering......");
        Thread.sleep(20000);


        StreamingInserter stramingInserter = new StreamingInserter(sConnector, getClusterName(), tableMetadata);
        stramingInserter.numOfElement(ELEMENTS_WRITE+8).elementPerSecond(500);
        stramingInserter.start();
        Thread.sleep(30000);
        stremingRead.end();
        stramingInserter.end();
        Thread.sleep(10000);
        assertEquals("the numberDefaultText of windows is correct", ELEMENTS_WRITE/WINDOW_ELEMENTS, (Object)windowNumber);
        assertTrue("the elements in the windows are correct",correctNumberOfElement);




    }


    private class ResultHandler implements IResultHandler {




        @Override
        public void processException(String queryId, ExecutionException exception) {
            System.out.println(queryId + " " + exception.getMessage());
            exception.printStackTrace();
        }




        @Override
        public void processResult(QueryResult result) {
            windowNumber++;
            if (result.getResultSet().size() != WINDOW_ELEMENTS){
                correctNumberOfElement = false;

            }

        }



    }

}
