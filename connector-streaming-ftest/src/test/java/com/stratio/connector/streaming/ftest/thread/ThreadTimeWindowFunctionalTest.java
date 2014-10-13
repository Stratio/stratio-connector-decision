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
import  org.slf4j.Logger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
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

public class ThreadTimeWindowFunctionalTest  extends GenericStreamingTest {

    /**
     * The Log.
     */
    final Logger logger = LoggerFactory.getLogger(this.getClass());
    public static final int ELEMENTS_WRITE = 500;



    private static final String OTHER_TEXT = "OTHER...... ";
    private static final int WAIT_TIME = 20000;



    public static String STRING_COLUMN = "string_column";
    public static String INTEGER_COLUMN = "integer_column";
    public static String BOOLEAN_COLUMN = "boolean_column";

    boolean correctOrder = true;
    Set<Integer> returnSet = new HashSet<>();


    TableMetadata tableMetadata;



    Boolean correct = true;
    Boolean correctType = true;
    Boolean returnTypes = true;

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
        sConnector.getMetadataEngine().dropTable(getClusterName(), new TableName(CATALOG, TABLE));
        sConnector.close(getClusterName());
    }

    @Test
    public void testStopReadBeforeStopWrite() throws InterruptedException, UnsupportedException {


        logger.debug("********************** Inserting ......");
        StreamingInserter stramingInserter = new StreamingInserter(sConnector, getClusterName(), tableMetadata);
        stramingInserter.start();

        LogicalWorkflow logicalWokflow = createLogicalWorkFlow();

        StreamingRead stremingRead = new StreamingRead(sConnector, getClusterName(), tableMetadata, logicalWokflow,
                        new ResultHandler((Select) logicalWokflow.getLastStep()));

        stremingRead.start();
        logger.debug("********************** Quering......");
        Thread.sleep(WAIT_TIME);

        stremingRead.end();
        logger.debug("********************** END Quering Test......");
        Thread.sleep(WAIT_TIME);
        logger.debug(" ********************** Change Test Quering......");
        stramingInserter.changeOtuput(OTHER_TEXT);
        Thread.sleep(WAIT_TIME);

        logger.debug(" ********************** END Insert......");
        stramingInserter.end();
        Thread.sleep(WAIT_TIME);

        assertTrue("all is correct", correct);
        assertTrue("Result is ordered", correctOrder);
        assertTrue("The types are correct", correctType);
        assertTrue("Return types", returnTypes);

    }

    @Test
    public void testStopWriteBeforeStopRead() throws InterruptedException, UnsupportedException {

        StreamingInserter stramingInserter = new StreamingInserter(sConnector, getClusterName(), tableMetadata);
        stramingInserter.start();

        LogicalWorkflow logicalWokflow = createLogicalWorkFlow();

        ResultHandler resultHandler = new ResultHandler((Select) logicalWokflow.getLastStep());
        StreamingRead stremingRead = new StreamingRead(sConnector, getClusterName(), tableMetadata, logicalWokflow, resultHandler);

        stremingRead.start();
        logger.debug("********************** Quering......");
        Thread.sleep(WAIT_TIME);

        logger.debug("********************** END Insert......");
        stramingInserter.end();
        Thread.sleep(10000); // it must be at least bigger than the windows time
        resultHandler.mustNotReadMore();
        logger.debug("TEST ********************** Wait for stoped read......");
        Thread.sleep(2 * WAIT_TIME);

        stremingRead.end();
        logger.debug("TEST ********************** END Quering......");
        Thread.sleep(WAIT_TIME);

        assertTrue("all is correct", correct);
        assertTrue("Result is ordered", correctOrder);
        assertTrue("The types are correct", correctType);
        assertTrue("Return types", returnTypes);

    }

    @Test
    public void testInsertConcreteNumber() throws InterruptedException, UnsupportedException {

        LogicalWorkflow logicalWokflow = createLogicalWorkFlow();

        StreamingRead stremingRead = new StreamingRead(sConnector, getClusterName(), tableMetadata, logicalWokflow,
                        new ResultHandler((Select) logicalWokflow.getLastStep()));

        stremingRead.start();
        logger.debug("********************** Quering......");
        Thread.sleep(20000);

        StreamingInserter stramingInserter = new StreamingInserter(sConnector, getClusterName(), tableMetadata);
        stramingInserter.numOfElement(ELEMENTS_WRITE).elementPerSecond(ELEMENTS_WRITE);
        stramingInserter.start();
        Thread.sleep(50000);
        stremingRead.end();
        stramingInserter.end();

        assertEquals("the number of elements read is correct", ELEMENTS_WRITE, returnSet.size());
        assertTrue("all is correct", correct);
        assertTrue("Result is ordered", correctOrder);
        assertTrue("The types are correct", correctType);
        assertTrue("Return types", returnTypes);

    }

    private LogicalWorkflow createLogicalWorkFlow() throws UnsupportedException {
        LogicalWorkFlowCreator logicalWorkFlowCreator = new LogicalWorkFlowCreator(CATALOG, TABLE,
                        getClusterName());

        LinkedList<LogicalWorkFlowCreator.ConnectorField> selectColumns = new LinkedList<>();
        selectColumns.add(logicalWorkFlowCreator.createConnectorField(STRING_COLUMN, STRING_COLUMN, ColumnType.TEXT));
        selectColumns.add(logicalWorkFlowCreator.createConnectorField(INTEGER_COLUMN, INTEGER_COLUMN, ColumnType.INT));
        selectColumns.add(logicalWorkFlowCreator.createConnectorField(BOOLEAN_COLUMN, BOOLEAN_COLUMN,
                        ColumnType.BOOLEAN));
        return logicalWorkFlowCreator.addColumnName(STRING_COLUMN)
                        .addColumnName(INTEGER_COLUMN).addColumnName(BOOLEAN_COLUMN).addSelect(selectColumns)
                .addWindow(WindowType.TEMPORAL, 5)
                        .getLogicalWorkflow();
    }

    @Test
    public void testManyThread() throws UnsupportedException, ExecutionException, InterruptedException {
        LogicalWorkflow logicalWokflow = new LogicalWorkFlowCreator(CATALOG, TABLE, getClusterName())
                        .addColumnName(STRING_COLUMN).addColumnName(INTEGER_COLUMN).addColumnName(BOOLEAN_COLUMN)
                .addWindow(WindowType.TEMPORAL, 20)
                        .getLogicalWorkflow();
        sConnector.getQueryEngine().asyncExecute("query1", logicalWokflow,
                        new ResultHandler((Select) logicalWokflow.getLastStep()));
        Thread.sleep(WAIT_TIME);
        sConnector.getQueryEngine().asyncExecute("query2", logicalWokflow,
                        new ResultHandler((Select) logicalWokflow.getLastStep()));
        Thread.sleep(WAIT_TIME);
        sConnector.getQueryEngine().asyncExecute("query3", logicalWokflow,
                        new ResultHandler((Select) logicalWokflow.getLastStep()));
        Thread.sleep(WAIT_TIME);
        sConnector.getQueryEngine().stop("query3");
        sConnector.getQueryEngine().asyncExecute("query4", logicalWokflow,
                        new ResultHandler((Select) logicalWokflow.getLastStep()));
        Thread.sleep(WAIT_TIME);
        sConnector.getQueryEngine().stop("query2");
        sConnector.getQueryEngine().asyncExecute("query5", logicalWokflow,
                        new ResultHandler((Select) logicalWokflow.getLastStep()));
        Thread.sleep(WAIT_TIME);
        sConnector.getQueryEngine().stop("query1");
        sConnector.getQueryEngine().asyncExecute("query6", logicalWokflow,
                        new ResultHandler((Select) logicalWokflow.getLastStep()));
        sConnector.getQueryEngine().stop("query4");

        Thread.sleep(WAIT_TIME);

        sConnector.getQueryEngine().stop("query5");
        sConnector.getQueryEngine().stop("query6");

    }

    private class ResultHandler implements IResultHandler {

        boolean mustRead = true;

        private ColumnName[] orderendColumnaName;

        public ResultHandler(Select select) {

            orderendColumnaName = select.getColumnMap().keySet().toArray(new ColumnName[0]);

        }

        @Override
        public void processException(String queryId, ExecutionException exception) {
            logger.error(queryId + " " + exception.getMessage());
            exception.printStackTrace();
        }

        public void mustNotReadMore() {
            boolean mustRead = false;

        }

        @Override
        public void processResult(QueryResult result) {
            if (!mustRead) {
                correct = false;
            }

            testTypes(result);
            for (Row row : result.getResultSet()) {
                testOrder(row);
                testElementNumber(row);
            }

        }

        private void testElementNumber(Row row) {
            Integer cellValue = ((Double) row.getCell(INTEGER_COLUMN).getValue()).intValue();
            returnSet.add(cellValue); // To remove duplicates
            Cell cell = row.getCell(STRING_COLUMN);
            if (cell != null) {
                Object value = cell.getValue();
                if (OTHER_TEXT.equals(value)) {
                    correct = false;
                }
            }
        }

        private void testOrder(Row row) {
            String[] recoveredColumn = row.getCells().keySet().toArray(new String[0]);
            for (int i = 0; i < recoveredColumn.length; i++) {
                if (!orderendColumnaName[i].getName().equals(recoveredColumn[i])) {
                    correctOrder = false;
                }
            }
        }

        private void testTypes(QueryResult result) {
            List<ColumnMetadata> columnMetadataList = result.getResultSet().getColumnMetadata();
            if (columnMetadataList == null) {
                returnTypes = false;
            } else {
                ColumnMetadata[] columnMetadata = columnMetadataList.toArray(new ColumnMetadata[0]);

                if (!columnMetadata[0].getType().equals(ColumnType.TEXT)
                                || !columnMetadata[1].getType().equals(ColumnType.INT)
                                || !columnMetadata[2].getType().equals(ColumnType.BOOLEAN)) {
                    correctType = false;
                }
            }
        }

    }

}
