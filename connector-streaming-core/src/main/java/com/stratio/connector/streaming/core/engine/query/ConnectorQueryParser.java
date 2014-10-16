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

import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.logicalplan.Filter;
import com.stratio.meta.common.logicalplan.Limit;
import com.stratio.meta.common.logicalplan.LogicalStep;
import com.stratio.meta.common.logicalplan.Project;
import com.stratio.meta.common.logicalplan.Select;
import com.stratio.meta.common.logicalplan.Window;
import com.stratio.meta.common.statements.structures.relationships.Operator;

/**
 * Created by jmgomez on 15/09/14.
 */
public class ConnectorQueryParser {

    public ConnectorQueryData transformLogicalWorkFlow(Project logicalWorkFlow, String queryId)
            throws UnsupportedException {

        ConnectorQueryData queryData = new ConnectorQueryData();
        queryData.setQueryId(queryId);
        LogicalStep lStep = logicalWorkFlow;

        do {
            if (lStep instanceof Project) {
                if (!queryData.hasProjection()) {
                    queryData.setProjection((Project) lStep);
                } else {
                    throw new UnsupportedOperationException(" # Project > 1");
                }
            } else if (lStep instanceof Filter) {

                Filter step = (Filter) lStep;
                if (Operator.MATCH == step.getRelation().getOperator()) {
                    throw new UnsupportedException("LogicalStep [" + lStep.getClass().getCanonicalName()
                            + " not supported");
                } else {
                    queryData.addFilter((Filter) lStep);
                }
            } else if (lStep instanceof Select) {
                queryData.setSelect((Select) lStep);

            } else if (lStep instanceof Window) {
                queryData.setWindow((Window) lStep);
            } else if (lStep instanceof Limit) {
                throw new UnsupportedException("LogicalStep [" + lStep.getClass().getCanonicalName()
                        + " not yet supported");
            } else {
                throw new UnsupportedException(
                        "LogicalStep [" + lStep.getClass().getCanonicalName() + " not supported");
            }

            lStep = lStep.getNextStep();

        } while (lStep != null);

        checkSupportedQuery(queryData);

        return queryData;
    }

    private void checkSupportedQuery(ConnectorQueryData queryData) {
        if (queryData.getSelect() == null) {
            throw new UnsupportedOperationException("no select found");
        }

    }

}
