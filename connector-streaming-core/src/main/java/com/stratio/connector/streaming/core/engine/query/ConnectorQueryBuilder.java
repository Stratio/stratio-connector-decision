/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 *  See the NOTICE file distributed with this work for additional information
 *  regarding copyright ownership. The STRATIO (C) licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package com.stratio.connector.streaming.core.engine.query;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.streaming.core.engine.query.util.StreamUtil;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.logicalplan.Filter;
import com.stratio.meta.common.logicalplan.Select;
import com.stratio.meta.common.statements.structures.relationships.Operator;
import com.stratio.meta.common.statements.structures.relationships.Relation;
import com.stratio.meta2.common.data.ColumnName;
import com.stratio.meta2.common.statements.structures.selectors.ColumnSelector;
import com.stratio.meta2.common.statements.structures.selectors.Selector;
import com.stratio.meta2.common.statements.structures.selectors.StringSelector;

/**
 * Created by jmgomez on 30/09/14.
 */
public class ConnectorQueryBuilder {

    /**
     * The log.
     */
    final Logger logger = LoggerFactory.getLogger(this.getClass());

    private StringBuilder querySb = new StringBuilder();

    public String createQuery(ConnectorQueryData queryData) throws ExecutionException, UnsupportedException {

        createInputQuery(queryData);
        createProjection(queryData);
        createOutputQuery(queryData);

        return querySb.toString();
    }

    /**
     * @param queryData
     */
    private void createOutputQuery(ConnectorQueryData queryData) {
        String outgoing = StreamUtil
                .createOutgoingName(StreamUtil.createStreamName(queryData.getProjection()), queryData.getQueryId());
        querySb.append(" insert into ");
        querySb.append(outgoing);
    }

    /**
     * @param queryData
     * @throws UnsupportedException
     */
    private void createProjection(ConnectorQueryData queryData) throws UnsupportedException {

        Select selectionClause = queryData.getSelect();
        Map<ColumnName, String> aliasMapping = selectionClause.getColumnMap();
        Set<ColumnName> columnMetadataList = aliasMapping.keySet();

        // Retrieving the fields
        if (columnMetadataList == null || columnMetadataList.isEmpty()) {
            String message = "The query has to retrieve data";
            logger.error(message);
            throw new UnsupportedException(message);
        }

        querySb.append(" select ");

        // Retrieving the alias
        int numFields = columnMetadataList.size();
        int i = 0;
        for (ColumnName colName : columnMetadataList) {

            querySb.append(StreamUtil.createStreamName(queryData.getProjection())).append(".").append(
                    colName.getName()).append(" as ").append(aliasMapping.get(colName));
            if (++i < numFields) {
                querySb.append(",");
            }
        }

    }

    /**
     * @param queryData
     * @return
     * @throws UnsupportedException
     */
    private void createInputQuery(ConnectorQueryData queryData) throws UnsupportedException {
        querySb.append("from ");
        createStreamQuery(queryData);
    }

    /**
     * @param queryData
     * @throws UnsupportedException
     */
    private void createStreamQuery(ConnectorQueryData queryData) throws UnsupportedException {

        String streamName = StreamUtil.createStreamName(queryData.getProjection());

        querySb.append(streamName);
        if (queryData.hasFilterList()) {
            createConditionList(queryData);
        }

    }

    /**
     * @param queryData
     * @throws UnsupportedException
     */
    private void createConditionList(ConnectorQueryData queryData) throws UnsupportedException {

        querySb.append("[");
        Iterator<Filter> filterIter = queryData.getFilter().iterator();
        while (filterIter.hasNext()) {
            Relation rel = filterIter.next().getRelation();

            querySb.append(getFieldName(rel.getLeftTerm())).append(" ").append(getSiddhiOperator(rel.getOperator()))
                    .append(" ");
            if (rel.getRightTerm() instanceof StringSelector) {
                querySb.append("'").append(((StringSelector) rel.getRightTerm()).getValue()).append("'");
            } else {
                switch (rel.getRightTerm().getType()) {
                case BOOLEAN:
                case INTEGER:
                case FLOATING_POINT:
                    querySb.append(rel.getRightTerm().toString());
                    break;
                case COLUMN:
                    querySb.append(getFieldName(rel.getRightTerm()));
                    break;
                case FUNCTION:
                case RELATION:
                case ASTERISK:
                default:
                    throw new UnsupportedException("Type " + rel.getRightTerm().getType() + "unsupported");
                }
            }

            if (filterIter.hasNext()) {
                querySb.append(" and ");
            }
        }
        querySb.append("]");

    }

    private String getFieldName(Selector selector) throws UnsupportedException {
        String field = null;
        if (selector instanceof ColumnSelector) {
            ColumnSelector columnSelector = (ColumnSelector) selector;
            field = columnSelector.getName().getName();
        } else {
            throw new UnsupportedException("Left selector must be a columnSelector in filters");
        }
        return field;
    }

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
