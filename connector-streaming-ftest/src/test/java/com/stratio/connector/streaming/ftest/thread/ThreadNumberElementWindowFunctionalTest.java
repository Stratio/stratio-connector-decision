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

public class ThreadNumberElementWindowFunctionalTest {

    /**
     * The Log.
     */
    final Logger logger = LoggerFactory.getLogger(this.getClass());
    public static final int ELEMENTS_WRITE = 500;
    private static final int WINDOW_ELEMENTS = 10;

    private static Random random = new Random(new Date().getTime());

    

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

    Integer  windowNumber = 0 ;
    Boolean correctNumberOfElement = true;


    @Before
    public void setUp() throws ConnectionException, UnsupportedException, ExecutionException {
        returnSet = new HashSet<>();
        windowNumber = 0;
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
    public void tesElmentWindow() throws InterruptedException {

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
                new ResultHandler());

        stremingRead.start();
        logger.debug(" ********************** Quering......");
        Thread.sleep(20000);


        StreamingInserter stramingInserter = new StreamingInserter(sC, clusterName, tableMetadata);
        stramingInserter.numOfElement(ELEMENTS_WRITE+8).elementPerSecond(500);
        stramingInserter.start();
        Thread.sleep(30000);
        stremingRead.end();
        stramingInserter.end();
        Thread.sleep(10000);
        assertEquals("the number of windows is correct", ELEMENTS_WRITE/WINDOW_ELEMENTS, (Object)windowNumber);
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
