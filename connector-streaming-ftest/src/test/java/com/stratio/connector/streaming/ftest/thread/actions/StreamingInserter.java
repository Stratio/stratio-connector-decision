package com.stratio.connector.streaming.ftest.thread.actions;

import com.stratio.connector.streaming.core.StreamingConnector;
import com.stratio.connector.streaming.ftest.thread.ThreadTimeWindowFunctionalTest;
import com.stratio.meta.common.connector.IStorageEngine;
import com.stratio.meta.common.data.Cell;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta2.common.data.ClusterName;
import com.stratio.meta2.common.metadata.TableMetadata;

public class StreamingInserter extends Thread {


    StreamingConnector streamingConnector;

    private ClusterName clusterName;
    private TableMetadata stream;
    private boolean finishThread = false;

    public StreamingInserter(StreamingConnector sC, ClusterName clusterName, TableMetadata stream) {
        super ("[StreamingInserter]");
        this.streamingConnector = sC;
        this.clusterName = clusterName;
        this.stream = stream;
    }
    private long elementPerSecond=10;
    private long numOfElement=0;


    public StreamingInserter elementPerSecond(long elements){
        this.elementPerSecond = elements;
        return this;
    }

    public StreamingInserter numOfElement(long elements){
        this.numOfElement = elements;
        return this;
    }



    @Override
    public void run() {
        try {
            System.out.println("****************************** STARTING StreamingInserter **********************");
            IStorageEngine storageEngine = streamingConnector.getStorageEngine();
            for (int i = 0; !finishThread; i++) {
                if (numOfElement!=0 && numOfElement==i) break;
                Row row = new Row();

                row.addCell(ThreadTimeWindowFunctionalTest.BOOLEAN_COLUMN, new Cell(true));
                row.addCell(ThreadTimeWindowFunctionalTest.INTEGER_COLUMN, new Cell(i));
                row.addCell(ThreadTimeWindowFunctionalTest.STRING_COLUMN, new Cell(TEXT));

                storageEngine.insert(clusterName, stream, row);
                System.out.println("insert name =>" + i);

               // if (i==0) Thread.sleep(10000);
               // storageEngine.insert(clusterName, stream, row);
                if ((i%elementPerSecond)==0) Thread.sleep(1000);
                if (numOfElement!=0 && numOfElement+1==i) finishThread=true;

            }
        } catch (UnsupportedException | ExecutionException   | InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("****************************** ENDING StreamingInserter **********************");
    }


	private static String TEXT = "Text ";


    public void changeOtuput(String stringOutput) {
        TEXT = stringOutput;
    }


    public void end(){
        finishThread = true;
    }

}
