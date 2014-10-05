package com.stratio.connector.streaming.ftest.thread.actions;

import com.stratio.connector.streaming.core.StreamingConnector;
import com.stratio.meta.common.logicalplan.LogicalWorkflow;
import com.stratio.meta.common.connector.IResultHandler;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta2.common.data.ClusterName;
import com.stratio.meta2.common.metadata.TableMetadata;

public class StreamingRead extends Thread {

	private StreamingConnector streamingConnector;
	private ClusterName clusterName;
	private TableMetadata tableMetadata;
	private LogicalWorkflow logicalWorkFlow;
	private IResultHandler resultHandler;

	public StreamingRead(StreamingConnector sC, ClusterName clusterName,
			TableMetadata tableMetadata, LogicalWorkflow logicalWorkFlow, IResultHandler resultHandler) {
		this.streamingConnector = sC;
		this.clusterName = clusterName;
		this.tableMetadata = tableMetadata;
		this.logicalWorkFlow = logicalWorkFlow;
		this.resultHandler = resultHandler;
	}
	
	
	public void run(){
		try {
			System.out.println("****************************** STARTING StreamingInserter **********************");
			streamingConnector.getQueryEngine().asyncExecute("queryId", logicalWorkFlow,  resultHandler);
		} catch (UnsupportedException | ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void end(){
		try {
			streamingConnector.getQueryEngine().stop("queryId");
		} catch (UnsupportedException | ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
