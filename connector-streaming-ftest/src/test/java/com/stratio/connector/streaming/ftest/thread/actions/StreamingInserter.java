package com.stratio.connector.streaming.ftest.thread.actions;

import java.util.ArrayList;
import java.util.List;

import com.stratio.connector.streaming.core.StreamingConnector;
import com.stratio.connector.streaming.ftest.GenericStreamingTest;
import com.stratio.meta.common.connector.IStorageEngine;
import com.stratio.meta.common.data.Cell;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta2.common.data.ClusterName;
import com.stratio.meta2.common.metadata.ColumnType;
import com.stratio.meta2.common.metadata.TableMetadata;

public class StreamingInserter extends Thread {

    StreamingConnector streamingConnector;

    private ClusterName clusterName;
    private TableMetadata stream;
    private boolean finishThread = false;
    private List<ColumnType> typesToInsert = null;

    public StreamingInserter(StreamingConnector sC, ClusterName clusterName, TableMetadata stream) {
        super("[StreamingInserter]");
        this.streamingConnector = sC;
        this.clusterName = clusterName;
        this.stream = stream;
    }

    private long elementPerSecond = 10;
    private long numOfElement = 0;

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

    @Override
    public void run() {

        if (typesToInsert == null) {
            typesToInsert = new ArrayList<ColumnType>(3);
            typesToInsert.add(ColumnType.BOOLEAN);
            typesToInsert.add(ColumnType.INT);
            typesToInsert.add(ColumnType.VARCHAR);
        }

        try {
            System.out.println("****************************** STARTING StreamingInserter **********************");
            IStorageEngine storageEngine = streamingConnector.getStorageEngine();
            for (int i = 0; !finishThread; i++) {
                if (numOfElement != 0 && numOfElement - 1 == i)
                    finishThread = true;

                Row row = getRowToInsert(i);

                storageEngine.insert(clusterName, stream, row);

                if ((i % elementPerSecond) == 0)
                    Thread.sleep(1000);

            }
        } catch (UnsupportedException | ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("****************************** ENDING StreamingInserter **********************");
    }

    /**
     * @param i
     * @return
     */
    private Row getRowToInsert(int i) {
        Row row = new Row();
        for (ColumnType colType : typesToInsert) {
            switch (colType) {
            case BOOLEAN:
                row.addCell(GenericStreamingTest.BOOLEAN_COLUMN, new Cell(true));
                break;
            case DOUBLE:
                row.addCell(GenericStreamingTest.DOUBLE_COLUMN, new Cell(new Double(i + 0.5)));
                break;
            case FLOAT:
                row.addCell(GenericStreamingTest.FLOAT_COLUMN, new Cell(new Float(i + 0.5)));
                break;
            case INT:
                row.addCell(GenericStreamingTest.INTEGER_COLUMN, new Cell(i));
                break;
            case BIGINT:
                row.addCell(GenericStreamingTest.LONG_COLUMN, new Cell(i + new Long(Long.MAX_VALUE / 2)));
                break;
            case VARCHAR:
            case TEXT:
                row.addCell(GenericStreamingTest.STRING_COLUMN, new Cell(TEXT));
                break;

            }
        }

        return row;
    }

    private String TEXT = "Text";

    public void changeOtuput(String stringOutput) {
        TEXT = stringOutput;
    }

    public void end() {
        finishThread = true;
    }

}
