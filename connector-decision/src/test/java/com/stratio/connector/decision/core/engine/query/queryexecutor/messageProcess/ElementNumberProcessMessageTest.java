/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.  The STRATIO (C) licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.stratio.connector.decision.core.engine.query.queryexecutor.messageProcess;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.modules.junit4.PowerMockRunner;

import com.stratio.connector.decision.core.engine.query.ConnectorQueryData;
import com.stratio.connector.decision.core.engine.query.queryexecutor.messageprocess.ElementNumberProcessMessage;
import com.stratio.connector.decision.core.engine.query.util.ResultsetCreator;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.logicalplan.Window;
import com.stratio.crossdata.common.metadata.Operations;
import com.stratio.crossdata.common.statements.structures.window.WindowType;

/**
 * ElementNumberProcessMessage Tester.
 *
 */
@RunWith(PowerMockRunner.class)
public class ElementNumberProcessMessageTest {

    public static final int ITERATIONS = 100;
    private static final int NUM_ROWS = 10;
    ElementNumberProcessMessage elementNumberProcessMessage;
    @Mock
    ResultsetCreator resultSetCreator;

    @Before
    public void before() throws Exception {

        ConnectorQueryData queryData = new ConnectorQueryData();
        Window window = new Window(new HashSet<Operations>(Arrays.asList(Operations.SELECT_WINDOW)), WindowType.NUM_ROWS);
        window.setNumRows(NUM_ROWS);
        queryData.setWindow(window);
        when(resultSetCreator.create(any(List.class))).thenReturn(resultSetCreator);
        elementNumberProcessMessage = new ElementNumberProcessMessage(queryData, resultSetCreator);
    }

    @After
    public void after() throws Exception {
    }

    /**
     * Method: processMessage(Row row)
     */
    @Test
    public void processMessageTest() throws Exception {
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
    public void endTest() throws Exception {
        // TODO: Test goes here...
    }

}
