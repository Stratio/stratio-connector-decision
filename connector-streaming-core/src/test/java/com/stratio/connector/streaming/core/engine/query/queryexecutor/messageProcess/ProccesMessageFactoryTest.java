/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership. The STRATIO (C) licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.stratio.connector.streaming.core.engine.query.queryexecutor.messageProcess;

import static junit.framework.TestCase.assertTrue;
import static org.mockito.Mockito.mock;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;

import com.stratio.connector.streaming.core.engine.query.ConnectorQueryData;
import com.stratio.connector.streaming.core.engine.query.queryexecutor.messageprocess.ElementNumberProcessMessage;
import com.stratio.connector.streaming.core.engine.query.queryexecutor.messageprocess.ProcessMessageFactory;
import com.stratio.connector.streaming.core.engine.query.queryexecutor.messageprocess.TimeWindowProcessMessage;
import com.stratio.connector.streaming.core.engine.query.util.ResultsetCreator;
import com.stratio.crossdata.common.connector.Operations;
import com.stratio.crossdata.common.logicalplan.Window;
import com.stratio.crossdata.common.statements.structures.window.TimeUnit;
import com.stratio.crossdata.common.statements.structures.window.WindowType;

/**
 * ProccesMessageFactory Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>
 * oct 16, 2014
 * </pre>
 */
@RunWith(PowerMockRunner.class)
public class ProccesMessageFactoryTest {

    @Before
    public void before() throws Exception {
    }

    @After
    public void after() throws Exception {
    }

    /**
     * Method: getProccesMessage(ConnectorQueryData queryData, ResultsetCreator resultSetCreator)
     */
    @Test
    public void testGetProccesMessage() throws Exception {

        ConnectorQueryData queryDataNum = new ConnectorQueryData();
        queryDataNum.setWindow(new Window(Operations.SELECT_WINDOW, WindowType.NUM_ROWS));
        assertTrue("number row is correct",
                        ProcessMessageFactory.getProccesMessage(queryDataNum, mock(ResultsetCreator.class)) instanceof ElementNumberProcessMessage);

        ConnectorQueryData queryDataTemporal = new ConnectorQueryData();
        Window window = new Window(Operations.SELECT_WINDOW, WindowType.TEMPORAL);
        window.setTimeWindow(10, TimeUnit.DAYS);
        queryDataTemporal.setWindow(window);
        assertTrue("number row is correct",
                        ProcessMessageFactory.getProccesMessage(queryDataTemporal, mock(ResultsetCreator.class)) instanceof TimeWindowProcessMessage);

    }

}
