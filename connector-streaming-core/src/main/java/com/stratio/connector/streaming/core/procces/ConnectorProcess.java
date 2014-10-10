package com.stratio.connector.streaming.core.procces;



import com.stratio.meta.common.exceptions.ExecutionException;
import  com.stratio.meta.common.logicalplan.Project;
/**
 * Created by jmgomez on 3/10/14.
 */
public  interface ConnectorProcess extends Runnable {


        public void endQuery() throws ExecutionException;

		public Project getProject();
}
