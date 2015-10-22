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

package com.stratio.connector.decision.core.engine.query.queryexecutor;

import static junit.framework.TestCase.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.*;

import com.stratio.connector.commons.TimerJ;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import kafka.serializer.Decoder;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.modules.junit4.PowerMockRunner;

import com.stratio.connector.decision.core.engine.query.ConnectorQueryData;
import com.stratio.connector.decision.core.engine.query.queryexecutor.messageprocess.ProcessMessage;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.logicalplan.Project;
import com.stratio.crossdata.common.logicalplan.Select;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.DataType;
import com.stratio.crossdata.common.metadata.Operations;
import com.stratio.crossdata.common.statements.structures.ColumnSelector;
import com.stratio.crossdata.common.statements.structures.Selector;
import com.stratio.decision.api.IStratioStreamingAPI;
import com.stratio.decision.commons.messages.ColumnNameTypeValue;
import com.stratio.decision.commons.messages.StratioStreamingMessage;
import com.stratio.decision.commons.messages.StreamQuery;

/**
 * DecisionQuery Tester.
 *
 */
@RunWith(PowerMockRunner.class)
public class DecisionQueryTest {

    private static final String QUERY = "query";
    private static final String CATALOG = "catalog";
    private static final String TABLE = "table";
    private static final ClusterName CLUSTER_NAME = new ClusterName("CLUSTER_NAME");
    private static final String QUERY_ID = "queryId";
    private static final String OUTPUT_STREAM = "output_stream";
    private static final String COLUMN1 = "column_1";
    private static final String ALIAS1 = "alias_1";

    private static final String OPERATION = "OPERATION";
    private static final String STREAM_NAME = "STREAM_NAME";
    private static final String SESION_ID = "1234";
    private static final String REQUEST_ID = "r123";
    private static final String REQUEST = "req";
    private static final Long TIMESTAMP = new Long(123);
    private static final List<StreamQuery> QUERIES = Collections.EMPTY_LIST;
    private static final Boolean USERDEFINED = true;

    private static final com.stratio.decision.commons.constants.ColumnType TYPE_1 = com.stratio.decision.commons.constants.ColumnType.BOOLEAN;
    private static final Object VALUE_1_1 = "value_1_1";
    private static final String COLUMN2 = "column_2";;
    private static final com.stratio.decision.commons.constants.ColumnType TYPE_2 = com.stratio.decision.commons.constants.ColumnType.INTEGER;
    private static final Object VALUE_2_1 = "value_2_1";
    private static final Object VALUE_1_2 = "value_1_2";
    private static final Object VALUE_2_2 = "value_2_2";

    DecisionQueryCreator decisionQuery;
    @Mock
    Decoder DECODER;
    @Mock
    Decoder KEYDECODER;
    @Mock
    IStratioStreamingAPI stratioDecisionApi;
    @Mock
    ProcessMessage processMessage;

    @Before
    public void before() throws Exception {

        decisionQuery = new DecisionQueryCreator(createQueryData(), processMessage);
    }

    @After
    public void after() throws Exception {
    }

    /**
     * Method: createQuery(String query, IStratioStreamingAPI stratioDecisionAPI, ConnectorQueryData queryData)
     */
    @Test
    public void createQueryTest() throws Exception {

        String queryId = decisionQuery.createQuery(QUERY, stratioDecisionApi);

        verify(stratioDecisionApi, times(1)).addQuery("catalog_table", QUERY);
        assertEquals("The querySend id is correct", "catalog_table_queryId", queryId);

    }

    @TimerJ
    @Test
    public void listenQueryTest() throws Exception {

        KafkaStream<String, StratioStreamingMessage> kafkaSrteam = mock(KafkaStream.class);
        when(stratioDecisionApi.listenStream(OUTPUT_STREAM)).thenReturn(kafkaSrteam);

        KafkaStream<String, StratioStreamingMessage> returnKafkaStream = decisionQuery.listenQuery(
                        stratioDecisionApi, OUTPUT_STREAM);

        assertEquals("The kafkastream is correct", kafkaSrteam, returnKafkaStream);

    }

    @Test
    public void readMessagesTest() throws Exception {

        KafkaStream<String, StratioStreamingMessage> streams = mock(KafkaStream.class);
        ConsumerIterator kafkaStreamIterator = mock(ConsumerIterator.class);
        when(kafkaStreamIterator.hasNext()).thenReturn(true, true, false);

        List<ColumnNameTypeValue> columns1 = createColumns(VALUE_1_1, VALUE_2_1);
        List<ColumnNameTypeValue> columns2 = createColumns(VALUE_1_2, VALUE_2_2);

        StratioStreamingMessage message1 = new StratioStreamingMessage(OPERATION, STREAM_NAME, SESION_ID, REQUEST_ID,
                        REQUEST, TIMESTAMP, columns1, QUERIES, USERDEFINED);
        StratioStreamingMessage message2 = new StratioStreamingMessage(OPERATION, STREAM_NAME, SESION_ID, REQUEST_ID,
                        REQUEST, TIMESTAMP, columns2, QUERIES, USERDEFINED);

        MessageAndMetadata messageAndMetadata1 = mock(MessageAndMetadata.class);
        when(messageAndMetadata1.message()).thenReturn(message1);
        MessageAndMetadata messageAndMetadata2 = mock(MessageAndMetadata.class);
        when(messageAndMetadata2.message()).thenReturn(message2);
        when(kafkaStreamIterator.next()).thenReturn(messageAndMetadata1, messageAndMetadata2);

        when(streams.iterator()).thenReturn(kafkaStreamIterator);

        decisionQuery.readMessages(streams);

        verify(processMessage, times(2)).processMessage(any(Row.class));


    }

    private List<ColumnNameTypeValue> createColumns(Object value1, Object value2) {
        List<ColumnNameTypeValue> columns = new LinkedList<>();
        columns.add(new ColumnNameTypeValue(COLUMN1, TYPE_1, value1));
        columns.add(new ColumnNameTypeValue(COLUMN2, TYPE_2, value2));
        return columns;
    }

    private ConnectorQueryData createQueryData() {
        ConnectorQueryData queryData = new ConnectorQueryData();
        queryData.setProjection(new Project(new HashSet<Operations>(Arrays.asList(Operations.PROJECT)), new TableName(CATALOG, TABLE), CLUSTER_NAME));
        queryData.setQueryId(QUERY_ID);
        Map<Selector, String> columnMap = new LinkedHashMap<>();
        columnMap.put(new ColumnSelector(new ColumnName(CATALOG, TABLE, COLUMN1)), ALIAS1);
        Map<String, ColumnType> typemap = new LinkedHashMap<>();
        typemap.put(COLUMN1, new ColumnType(DataType.TEXT));
        typemap.put(COLUMN2, new ColumnType(DataType.TEXT));
        Map<Selector, ColumnType> typemapColumnName = new LinkedHashMap<>();
        typemapColumnName.put(new ColumnSelector(new ColumnName(CATALOG, TABLE, COLUMN1)), new ColumnType(DataType.TEXT));
        typemapColumnName.put(new ColumnSelector(new ColumnName(CATALOG, TABLE, COLUMN2)), new ColumnType(DataType.TEXT));
        Select select = new Select(new HashSet<Operations>(Arrays.asList(Operations.SELECT_OPERATOR)), columnMap, typemap, typemapColumnName);
        queryData.setSelect(select);
        return queryData;
    }

}
