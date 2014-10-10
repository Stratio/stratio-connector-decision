package com.stratio.connector.streaming.ftest.thread;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
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

import com.stratio.connector.commons.ftest.workFlow.LogicalWorkFlowCreator;
import com.stratio.connector.streaming.core.StreamingConnector;
import com.stratio.connector.streaming.ftest.thread.actions.StreamingInserter;
import com.stratio.connector.streaming.ftest.thread.actions.StreamingRead;
import com.stratio.meta.common.connector.ConnectorClusterConfig;
import com.stratio.meta.common.connector.IResultHandler;
import com.stratio.meta.common.data.Cell;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.exceptions.ConnectionException;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.logicalplan.LogicalWorkflow;
import com.stratio.meta.common.logicalplan.Select;
import com.stratio.meta.common.metadata.structures.ColumnMetadata;
import com.stratio.meta.common.result.QueryResult;
import com.stratio.meta2.common.data.ClusterName;
import com.stratio.meta2.common.data.ColumnName;
import com.stratio.meta2.common.data.TableName;

import com.stratio.meta2.common.metadata.ColumnType;
import com.stratio.meta2.common.metadata.TableMetadata;

public class ThreadTimeWindowFunctionalTest {

    public static final int ELEMENTS_WRITE = 500;


    private static Random random = new Random(new Date().getTime());

    private static final String OTHER_TEXT = "OTHER...... ";
    private static final int WAIT_TIME = 20000;
    private static final String CATALOG_NAME = "catalog_name_"+ Math.abs(random.nextLong());
    private static final String TABLE_NAME = "table_name";
    String ZOOKEEPER_SERVER = "10.200.0.58";;// "192.168.0.2";
    String KAFKA_SERVER = "10.200.0.58";// "192.168.0.2";

    String KAFKA_PORT = "9092";
    String ZOOKEEPER_PORT = "2181";
    public static String STRING_COLUMN = "string_column";
    public static String INTEGER_COLUMN = "integer_column";
    public static String BOOLEAN_COLUMN = "boolean_column";
    boolean correctOrder = true;
    Set<Integer> returnSet = new HashSet<>();

    StreamingConnector sC;
    TableMetadata tableMetadata;

    ClusterName clusterName = new ClusterName("CLUSTERNAME");

    Boolean correct = true;
      Boolean correctType = true;
    Boolean returnTypes = true;

    @Before
    public void setUp() throws ConnectionException, UnsupportedException, ExecutionException {
        returnSet = new HashSet<>();
        sC = new StreamingConnector();

        sC.init(null);

        Map<String, String> optionsNode = new HashMap<>();

        optionsNode.put("KafkaServer", KAFKA_SERVER);
        optionsNode.put("KafkaPort", KAFKA_PORT);
        optionsNode.put("zooKeeperServer", ZOOKEEPER_SERVER);
        optionsNode.put("zooKeeperPort", ZOOKEEPER_PORT);

        ConnectorClusterConfig config = new ConnectorClusterConfig(clusterName, optionsNode);
        sC.connect(null, config);
        TableName tableName = new TableName(CATALOG_NAME, TABLE_NAME);

        ColumnName columnNameString = new ColumnName(tableName, STRING_COLUMN);
        Map<ColumnName, com.stratio.meta2.common.metadata.ColumnMetadata> columns = new LinkedHashMap();
        columns.put(columnNameString, new com.stratio.meta2.common.metadata.ColumnMetadata(columnNameString, new Object[] {}, ColumnType.VARCHAR));
        ColumnName columnNameInteger = new ColumnName(tableName, INTEGER_COLUMN);
        columns.put(columnNameInteger, new com.stratio.meta2.common.metadata.ColumnMetadata(columnNameInteger, new Object[] {}, ColumnType.INT));
        ColumnName columnNameBoolean = new ColumnName(tableName, BOOLEAN_COLUMN);
        columns.put(columnNameBoolean, new com.stratio.meta2.common.metadata.ColumnMetadata(columnNameBoolean, new Object[] {}, ColumnType.BOOLEAN));
        tableMetadata = new TableMetadata(tableName, Collections.EMPTY_MAP, columns, Collections.EMPTY_MAP,
                        clusterName, Collections.EMPTY_LIST, Collections.EMPTY_LIST);

        try {
            sC.getMetadataEngine().createTable(clusterName, tableMetadata);

        } catch (ExecutionException t) {

        }

    }

    @After
    public void tearDown() throws UnsupportedException, ExecutionException {
        sC.getMetadataEngine().dropTable(clusterName,new TableName(CATALOG_NAME,TABLE_NAME));
        sC.close(clusterName);
    }

    @Test
    public void testStopReadBeforeStopWrite() throws InterruptedException {

        System.out.println("Thread Query ID " + Thread.currentThread().getId());
        System.out.println("TEST ********************** Inserting ......");
        StreamingInserter stramingInserter = new StreamingInserter(sC, clusterName, tableMetadata);
        stramingInserter.start();

        LogicalWorkFlowCreator logicalWorkFlowCreator = new LogicalWorkFlowCreator(CATALOG_NAME, TABLE_NAME,
                clusterName);

        LinkedList<LogicalWorkFlowCreator.ConnectorField> selectColumns = new LinkedList<>();
        selectColumns.add(logicalWorkFlowCreator.createConnectorField(STRING_COLUMN,STRING_COLUMN,ColumnType.TEXT));
        selectColumns.add(logicalWorkFlowCreator.createConnectorField(INTEGER_COLUMN,INTEGER_COLUMN,ColumnType.INT));
        selectColumns.add(logicalWorkFlowCreator.createConnectorField(BOOLEAN_COLUMN,BOOLEAN_COLUMN,ColumnType.BOOLEAN));



        LogicalWorkflow logicalWokflow = logicalWorkFlowCreator.addColumnName(STRING_COLUMN).addColumnName
                (INTEGER_COLUMN).addColumnName(BOOLEAN_COLUMN).addSelect(selectColumns)
                        .getLogicalWorkflow();


        StreamingRead stremingRead = new StreamingRead(sC, clusterName, tableMetadata, logicalWokflow,
                        new ResultHandler((Select)logicalWokflow.getLastStep()));

        stremingRead.start();
        System.out.println("TEST ********************** Quering......");
        Thread.sleep(WAIT_TIME);

        stremingRead.end();
        System.out.println("TEST ********************** END Quering Test......");
        Thread.sleep(WAIT_TIME);
        System.out.println("TEST ********************** Change Test Quering......");
        stramingInserter.changeOtuput(OTHER_TEXT);
        Thread.sleep(WAIT_TIME);

        System.out.println("TEST ********************** END Insert......");
        stramingInserter.end();
        Thread.sleep(WAIT_TIME);

        assertTrue("all is correct", correct);
        assertTrue("Result is ordered",correctOrder);
        assertTrue("The types are correct",correctType);
        assertTrue("Return types",returnTypes);

    }

    @Test
    public void testStopWriteBeforeStopRead() throws InterruptedException {

        StreamingInserter stramingInserter = new StreamingInserter(sC, clusterName, tableMetadata);
        stramingInserter.start();

        LogicalWorkFlowCreator logicalWorkFlowCreator = new LogicalWorkFlowCreator(CATALOG_NAME, TABLE_NAME,
                clusterName);

        LinkedList<LogicalWorkFlowCreator.ConnectorField> selectColumns = new LinkedList<>();
        selectColumns.add(logicalWorkFlowCreator.createConnectorField(STRING_COLUMN,STRING_COLUMN,ColumnType.TEXT));
        selectColumns.add(logicalWorkFlowCreator.createConnectorField(INTEGER_COLUMN,INTEGER_COLUMN,ColumnType.INT));
        selectColumns.add(logicalWorkFlowCreator.createConnectorField(BOOLEAN_COLUMN,BOOLEAN_COLUMN,ColumnType.BOOLEAN));

        LogicalWorkflow logicalWokflow = logicalWorkFlowCreator.addColumnName(STRING_COLUMN).addColumnName
                (INTEGER_COLUMN).addColumnName(BOOLEAN_COLUMN).addSelect(selectColumns)
                        .getLogicalWorkflow();

        ResultHandler resultHandler = new ResultHandler((Select)logicalWokflow.getLastStep());
        StreamingRead stremingRead = new StreamingRead(sC, clusterName, tableMetadata, logicalWokflow, resultHandler);

        stremingRead.start();
        System.out.println("TEST ********************** Quering......");
        Thread.sleep(WAIT_TIME);

        System.out.println("TEST ********************** END Insert......");
        stramingInserter.end();
        Thread.sleep(10000); // it must be at least bigger than the windows time
        resultHandler.mustNotReadMore();
        System.out.println("TEST ********************** Wait for stoped read......");
        Thread.sleep(2 * WAIT_TIME);

        stremingRead.end();
        System.out.println("TEST ********************** END Quering......");
        Thread.sleep(WAIT_TIME);

        assertTrue("all is correct", correct);
        assertTrue("Result is ordered",correctOrder);
        assertTrue("The types are correct",correctType);
        assertTrue("Return types",returnTypes);


    }

    @Test
    public void testInsertConcreteNumber() throws InterruptedException {

        LogicalWorkFlowCreator logicalWorkFlowCreator = new LogicalWorkFlowCreator(CATALOG_NAME, TABLE_NAME,
                clusterName);

        LinkedList<LogicalWorkFlowCreator.ConnectorField> selectColumns = new LinkedList<>();
        selectColumns.add(logicalWorkFlowCreator.createConnectorField(STRING_COLUMN,STRING_COLUMN,ColumnType.TEXT));
        selectColumns.add(logicalWorkFlowCreator.createConnectorField(INTEGER_COLUMN,INTEGER_COLUMN,ColumnType.INT));
        selectColumns.add(logicalWorkFlowCreator.createConnectorField(BOOLEAN_COLUMN,BOOLEAN_COLUMN,ColumnType.BOOLEAN));
        LogicalWorkflow logicalWokflow = logicalWorkFlowCreator
                .addColumnName(STRING_COLUMN).addColumnName(INTEGER_COLUMN).addColumnName(BOOLEAN_COLUMN).addSelect
                        (selectColumns)
                .getLogicalWorkflow();

        StreamingRead stremingRead = new StreamingRead(sC, clusterName, tableMetadata, logicalWokflow,
                new ResultHandler((Select)logicalWokflow.getLastStep()));

        stremingRead.start();
        System.out.println("TEST ********************** Quering......");
        Thread.sleep(20000);


        StreamingInserter stramingInserter = new StreamingInserter(sC, clusterName, tableMetadata);
        stramingInserter.numOfElement(ELEMENTS_WRITE).elementPerSecond(ELEMENTS_WRITE);
        stramingInserter.start();
        Thread.sleep(50000);
        stremingRead.end();
        stramingInserter.end();

        assertEquals("the number of elements read is correct",ELEMENTS_WRITE, returnSet.size());
        assertTrue("all is correct", correct);
        assertTrue("Result is ordered",correctOrder);
        assertTrue("The types are correct",correctType);
        assertTrue("Return types",returnTypes);




    }

    @Test
    public void testManyThread() throws UnsupportedException, ExecutionException, InterruptedException {
        LogicalWorkflow logicalWokflow = new LogicalWorkFlowCreator(CATALOG_NAME, TABLE_NAME, clusterName)
                .addColumnName(STRING_COLUMN).addColumnName(INTEGER_COLUMN).addColumnName(BOOLEAN_COLUMN)
                .getLogicalWorkflow();
        sC.getQueryEngine().asyncExecute("query1",logicalWokflow,new ResultHandler((Select)logicalWokflow.getLastStep()));
        Thread.sleep(WAIT_TIME);
        sC.getQueryEngine().asyncExecute("query2", logicalWokflow, new ResultHandler((Select) logicalWokflow.getLastStep
                ()));
        Thread.sleep(WAIT_TIME);
        sC.getQueryEngine().asyncExecute("query3",logicalWokflow,new ResultHandler((Select)logicalWokflow.getLastStep()));
        Thread.sleep(WAIT_TIME);
        sC.getQueryEngine().stop("query3");
        sC.getQueryEngine().asyncExecute("query4",logicalWokflow,new ResultHandler((Select)logicalWokflow.getLastStep()));
        Thread.sleep(WAIT_TIME);
        sC.getQueryEngine().stop("query2");
        sC.getQueryEngine().asyncExecute("query5",logicalWokflow,new ResultHandler((Select)logicalWokflow.getLastStep()));
        Thread.sleep(WAIT_TIME);
        sC.getQueryEngine().stop("query1");
        sC.getQueryEngine().asyncExecute("query6",logicalWokflow,new ResultHandler((Select)logicalWokflow.getLastStep()));
        sC.getQueryEngine().stop("query4");

        Thread.sleep(WAIT_TIME);

        sC.getQueryEngine().stop("query5");
        sC.getQueryEngine().stop("query6");



    }
    private class ResultHandler implements IResultHandler {

        boolean mustRead = true;


        private ColumnName[] orderendColumnaName;


        public ResultHandler(Select select){


            orderendColumnaName = select.getColumnMap().keySet().toArray(new ColumnName[0]);

        }

        @Override
        public void processException(String queryId, ExecutionException exception) {
            System.out.println(queryId + " " + exception.getMessage());
            exception.printStackTrace();
        }

        public void mustNotReadMore() {
       boolean     mustRead = false;

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
            Integer cellValue = ((Double)row.getCell(INTEGER_COLUMN).getValue()).intValue();
            System.out.println("-><>-"+cellValue);
            returnSet.add(cellValue); //To remove duplicates
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
            for (int i=0;i<recoveredColumn.length;i++){
                if (!orderendColumnaName[i].getName().equals(recoveredColumn[i])){
                    System.out.println(orderendColumnaName[i] +"<-->"+recoveredColumn[i]);
                    correctOrder = false;
                }
            }
        }

        private void testTypes(QueryResult result) {
            List<ColumnMetadata> columnMetadataList = result.getResultSet()
                    .getColumnMetadata();
            if (columnMetadataList==null){
                returnTypes = false;
            }else {
                ColumnMetadata[] columnMetadata = columnMetadataList.toArray(new ColumnMetadata[0]);

                if (!columnMetadata[0].getType().equals(ColumnType.TEXT) ||  ! columnMetadata[1].getType()
                        .equals(ColumnType.INT) || !columnMetadata[2].getType().equals(ColumnType.BOOLEAN)){
                    correctType = false;
                }
            }
        }

    }

}
