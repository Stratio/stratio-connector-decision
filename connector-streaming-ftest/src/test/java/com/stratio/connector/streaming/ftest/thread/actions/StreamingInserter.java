package com.stratio.connector.streaming.ftest.thread.actions;

import java.util.Map;

import com.stratio.connector.streaming.core.StreamingConnector;
import com.stratio.connector.streaming.ftest.thread.ThreadFunctionalTest;
import com.stratio.meta.common.connector.IStorageEngine;
import com.stratio.meta.common.data.Cell;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta2.common.data.ClusterName;
import com.stratio.meta2.common.metadata.TableMetadata;

public class StreamingInserter extends Thread{
	
	StreamingConnector streamingConnector;
	boolean insert = true;
	private ClusterName clusterName;
	private TableMetadata stream;
	
	public StreamingInserter(StreamingConnector sC, ClusterName clusterName, TableMetadata stream) {
		this.streamingConnector = sC;
		this.clusterName = clusterName;
		this.stream = stream;
	}

	private static String TEXT = "Text ";
	@Override
	public void run(){
		try {
		System.out.println("****************************** STARTING StreamingInserter **********************");
		IStorageEngine storageEngine = streamingConnector.getStorageEngine();
		for (int i=0;insert; i = (i+1)%100000){
			Row row = new Row();
			
			
			row.addCell(ThreadFunctionalTest.BOOLEAN_COLUMN, new Cell(true));
			row.addCell(ThreadFunctionalTest.INTEGER_COLUMN, new Cell(i));
			row.addCell(ThreadFunctionalTest.STRING_COLUMN, new Cell(TEXT));
			
				storageEngine.insert(clusterName, stream, row);
				if ((i%100)==0) Thread.currentThread().sleep(5000);
		}
			} catch (UnsupportedException | ExecutionException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		System.out.println("****************************** ENDING StreamingInserter **********************");
		}

	public void end() {
		insert = false;
		
	}

	public void changeOtuput(String stringOutput) {
			TEXT = stringOutput;
	}
		
	

}
