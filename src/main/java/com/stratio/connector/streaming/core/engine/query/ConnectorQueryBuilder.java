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

package com.stratio.connector.streaming.core.engine.query;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.commons.util.SelectorHelper;
import com.stratio.connector.streaming.core.engine.query.util.StreamUtil;
import com.stratio.connector.streaming.core.exception.ExecutionValidationException;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.logicalplan.Filter;
import com.stratio.crossdata.common.logicalplan.Select;
import com.stratio.crossdata.common.statements.structures.Operator;
import com.stratio.crossdata.common.statements.structures.Relation;
import com.stratio.crossdata.common.statements.structures.StringSelector;

/**
 * This class builds the queries.
 */
public class ConnectorQueryBuilder {

    /**
     * The log.
     */
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * The query under building.
     */
    private StringBuilder querySb = new StringBuilder();

    /**
     * This method create a query.
     *
     * @param queryData
     *            the query data.
     * @return the string query representation.
     * @throws ExecutionException
     *             in any error happens.
     * @throws UnsupportedException
     *             if any operation is not supported.
     */
    public String createQuery(ConnectorQueryData queryData) throws ExecutionException, UnsupportedException {

        createInputQuery(queryData);
        createProjection(queryData);
        createOutputQuery(queryData);

        return querySb.toString();
    }

    /**
     * Create the query output part.
     *
     * @param queryData
     *            the query data.
     */
    private void createOutputQuery(ConnectorQueryData queryData) {
        String outgoing = StreamUtil.createOutgoingName(StreamUtil.createStreamName(queryData.getProjection()),
                        queryData.getQueryId());
        querySb.append(" insert into ");
        querySb.append(outgoing);
    }

    /**
     * Create the query projection part.
     *
     * @param queryData
     *            the query data.
     * @throws ExecutionValidationException
     *             if any operation is not supported.
     */
    private void createProjection(ConnectorQueryData queryData) throws ExecutionValidationException {

        Select selectionClause = queryData.getSelect();
        Map<ColumnName, String> aliasMapping = selectionClause.getColumnMap();
        Set<ColumnName> columnMetadataList = aliasMapping.keySet();

        // Retrieving the fields
        if (columnMetadataList == null || columnMetadataList.isEmpty()) {
            String message = "The query has to retrieve data";
            logger.error(message);
            throw new ExecutionValidationException(message);
        }

        querySb.append(" select ");

        // Retrieving the alias
        int numFields = columnMetadataList.size();
        int i = 0;
        for (ColumnName colName : columnMetadataList) {

            querySb.append(StreamUtil.createStreamName(queryData.getProjection())).append(".")
                            .append(colName.getName()).append(" as ").append(aliasMapping.get(colName));
            if (++i < numFields) {
                querySb.append(",");
            }
        }

    }

    /**
     * Create the query input part.
     *
     * @param queryData
     *            the query data.
     * @throws UnsupportedException
     *             if any operation is not supported.
     * @throws ExecutionException
     *             in any error happens.
     */
    private void createInputQuery(ConnectorQueryData queryData) throws ExecutionException, UnsupportedException {
        querySb.append("from ");

        String streamName = StreamUtil.createStreamName(queryData.getProjection());

        querySb.append(streamName);
        if (queryData.hasFilterList()) {
            createConditionList(queryData);
        }

    }

    /**
     * Create the condition query part.
     *
     * @param queryData
     *            the query data.
     * @throws UnsupportedException
     *             if any operation is not supported.
     * @throws ExecutionException
     *             in any error happens.
     */
    private void createConditionList(ConnectorQueryData queryData) throws UnsupportedException, ExecutionException {

        querySb.append("[");
        Iterator<Filter> filterIter = queryData.getFilter().iterator();
        while (filterIter.hasNext()) {
            Relation rel = filterIter.next().getRelation();

            String value = SelectorHelper.getValue(String.class, rel.getRightTerm());

            querySb.append(SelectorHelper.getValue(String.class, rel.getLeftTerm())).append(" ")
                            .append(getSiddhiOperator(rel.getOperator())).append(" ");

            if (rel.getRightTerm() instanceof StringSelector) {
                querySb.append("'").append(value).append("'");
            } else {
                querySb.append(value);
            }

            if (filterIter.hasNext()) {
                querySb.append(" and ");
            }
        }
        querySb.append("]");

    }

    /**
     * Turn a crossdata operator into a shiddhi operator.
     *
     * @param operator
     *            the crossdata operator.
     * @return a the siddhi operator.
     * @throws UnsupportedException
     *             if any operation is not supported.
     */
    private String getSiddhiOperator(Operator operator) throws UnsupportedException {

        String siddhiOperator;
        switch (operator) {

        case DISTINCT:
            siddhiOperator = "!=";
            break;
        case EQ:
            siddhiOperator = "==";
            break;
        case GET:
            siddhiOperator = ">=";
            break;
        case GT:
            siddhiOperator = ">";
            break;
        case LET:
            siddhiOperator = "<=";
            break;
        case LT:
            siddhiOperator = "<";
            break;
        default:
            throw new UnsupportedException("Operator " + operator.toString() + "is not supported");

        }

        return siddhiOperator;
    }

}
