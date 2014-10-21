package com.stratio.connector.streaming.core.engine.query.queryexecutor.messageProcess;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.modules.junit4.PowerMockRunner;

import com.stratio.connector.streaming.core.engine.query.ConnectorQueryData;
import com.stratio.connector.streaming.core.engine.query.util.ResultsetCreator;
import com.stratio.meta.common.connector.Operations;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.logicalplan.Window;
import com.stratio.meta.common.statements.structures.window.WindowType;

/**
 * ElementNumberProcessMessage Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>oct 16, 2014</pre>
 */
@RunWith(PowerMockRunner.class)
public class ElementNumberProcessMessageTest {

    public static final int ITERATIONS = 100;
    private static final int NUM_ROWS = 10;
    ElementNumberProcessMessage elementNumberProcessMessage;
    @Mock ResultsetCreator resultSetCreator;

    @Before
    public void before() throws Exception {

        ConnectorQueryData queryData = new ConnectorQueryData();
        Window window = new Window(Operations.SELECT_WINDOW, WindowType.NUM_ROWS);
        window.setNumRows(NUM_ROWS);
        queryData.setWindow(window);
        when(resultSetCreator.createResultSet(any(List.class))).thenReturn(resultSetCreator);
        elementNumberProcessMessage = new ElementNumberProcessMessage(queryData, resultSetCreator);
    }

    @After
    public void after() throws Exception {
    }

    /**
     * Method: processMessage(Row row)
     */
    @Test
    public void testProcessMessage() throws Exception {
        for (int i = 0; i < ITERATIONS; i++) {
            Row row = mock(Row.class);
            elementNumberProcessMessage.processMessage(row);
        }
        verify(resultSetCreator, times(ITERATIONS / NUM_ROWS)).send();
    }

    /**
     * Method: end()
     */
    @Test
    public void testEnd() throws Exception {
        //TODO: Test goes here...
    }

} 
