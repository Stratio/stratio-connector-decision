package com.stratio.connector.streaming.core.engine.query.queryExecutor.messageProcess; 

import static junit.framework.TestCase.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Timer;

import org.junit.Test;
import org.junit.Before; 
import org.junit.After;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.internal.util.reflection.Whitebox;
import org.powermock.modules.junit4.PowerMockRunner;


import com.stratio.connector.streaming.core.engine.query.ConnectorQueryData;
import com.stratio.connector.streaming.core.engine.query.util.ResultsetCreator;
import com.stratio.meta.common.connector.Operations;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.logicalplan.Window;
import com.stratio.meta.common.statements.structures.window.TimeUnit;
import com.stratio.meta.common.statements.structures.window.WindowType;

/** 
* TimeWindowProcessMessage Tester. 
* 
* @author <Authors name> 
* @since <pre>oct 16, 2014</pre> 
* @version 1.0 
*/
@RunWith(PowerMockRunner.class)
public class TimeWindowProcessMessageTest {

    public static final int NUM_TIME_UNITS = 10;
    private static final int EJECUTION_TIME = 30;

    TimeWindowProcessMessage timeWindowProcessMessage;
    @Mock ResultsetCreator resultSetCreator;


    long time;
    @Before
public void before() throws Exception {

    ConnectorQueryData queryData = new ConnectorQueryData();
    Window window  = new Window(Operations.SELECT_WINDOW, WindowType.TEMPORAL);
    window.setTimeWindow(NUM_TIME_UNITS, TimeUnit.SECONDS);
    queryData.setWindow(window);
    when(resultSetCreator.createResultSet(any(List.class))).thenReturn(resultSetCreator);

    timeWindowProcessMessage = new TimeWindowProcessMessage(queryData,resultSetCreator);

} 

@After
public void after() throws Exception { 
} 


/** 
*
* Method: processMessage(Row row) 
*
*/ 
@Test
public void testProcessMessage() throws Exception {

    time = System.currentTimeMillis();
    while(System.currentTimeMillis() - time<EJECUTION_TIME*1000){
        Row row = mock(Row.class);
        timeWindowProcessMessage.processMessage(row);
    }

    verify(resultSetCreator, times(EJECUTION_TIME / NUM_TIME_UNITS)).send();
} 

/**
*
* Method: end() 
* 
*/
@Test
public void testEnd() throws Exception {

    Timer timer = mock(Timer.class);
    Whitebox.setInternalState(timeWindowProcessMessage,"timer",timer);

    timeWindowProcessMessage.end();
    assertTrue("Is interrupted", (boolean)Whitebox.getInternalState(timeWindowProcessMessage, "isInterrupted"));
    verify(timer,times(1)).cancel();


} 




} 
