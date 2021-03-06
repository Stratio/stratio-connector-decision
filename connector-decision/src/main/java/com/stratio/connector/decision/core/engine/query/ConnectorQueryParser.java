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

package com.stratio.connector.decision.core.engine.query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.decision.core.exception.ExecutionValidationException;
import com.stratio.crossdata.common.logicalplan.Filter;
import com.stratio.crossdata.common.logicalplan.Limit;
import com.stratio.crossdata.common.logicalplan.LogicalStep;
import com.stratio.crossdata.common.logicalplan.Project;
import com.stratio.crossdata.common.logicalplan.Select;
import com.stratio.crossdata.common.logicalplan.Window;
import com.stratio.crossdata.common.statements.structures.Operator;

/**
 * This class turn a logical workflow into a queryData.
 */
public class ConnectorQueryParser {

    /**
     * The Log.
     */
    private final Logger logger = LoggerFactory.getLogger(ConnectorQueryParser.class);

    /**
     * Turn a logical workflow into a query data.
     *
     * @param logicalWorkFlow
     *            the logical workflow.
     * @param queryId
     *            the queryID.
     * @return a queryData.
     * @throws ExecutionValidationException
     *             if any operation is not supported.
     */
    public ConnectorQueryData transformLogicalWorkFlow(Project logicalWorkFlow, String queryId)
                    throws ExecutionValidationException {

        ConnectorQueryData queryData = new ConnectorQueryData();
        queryData.setQueryId(queryId);
        LogicalStep lStep = logicalWorkFlow;

        do {
            extractLogicalStep(queryData, lStep);

            lStep = lStep.getNextStep();

        } while (lStep != null);

        checkSupportedQuery(queryData);

        return queryData;
    }

    /**
     * This method put a concrete logical step in the queryData.
     *
     * @param queryData
     *            the queryData.
     * @param lStep
     *            the logical Step.
     * @throws ExecutionValidationException
     *             if any logicalStep is not supported.
     */
    private void extractLogicalStep(ConnectorQueryData queryData, LogicalStep lStep)
                    throws ExecutionValidationException {
        if (lStep instanceof Project) {
            processProject(queryData, (Project) lStep);
        } else if (lStep instanceof Filter) {
            processFilter(queryData, (Filter) lStep);
        } else if (lStep instanceof Select) {
            queryData.setSelect((Select) lStep);

        } else if (lStep instanceof Window) {
            queryData.setWindow((Window) lStep);
        } else if (lStep instanceof Limit) {
            throw new ExecutionValidationException("LogicalStep [" + lStep.getClass().getCanonicalName()
                            + " not yet supported");
        } else {
            String message = "LogicalStep [" + lStep.getClass().getCanonicalName() + " not supported";
            logger.error(message);
            throw new ExecutionValidationException(message);
        }
    }

    /**
     * This method process the filter logical step before to insert into the queryData.
     *
     * @param queryData
     *            the queryData.
     * @param filter
     *            the Filter
     * @throws ExecutionValidationException
     *             if the filter type is not supported.
     */
    private void processFilter(ConnectorQueryData queryData, Filter filter) throws ExecutionValidationException {

        if (Operator.MATCH == filter.getRelation().getOperator()) {
            String message = "LogicalStep [" + filter.getClass().getCanonicalName() + " not supported";
            logger.error(message);
            throw new ExecutionValidationException(message);
        } else {
            queryData.addFilter(filter);
        }
    }

    /**
     * This method process the prject logical step before to insert into the queryData.
     *
     * @param queryData
     *            the queryData.
     * @param project
     *            the project
     * @throws ExecutionValidationException
     *             if other project exists.
     */
    private void processProject(ConnectorQueryData queryData, Project project) throws ExecutionValidationException {
        if (!queryData.hasProjection()) {
            queryData.setProjection(project);
        } else {
            String message = "It has been found more than one project";
            logger.error(message);
            throw new ExecutionValidationException(message);
        }
    }

    /**
     * Check if the query is correct.
     *
     * @param queryData
     *            the queryData representatio for the query.
     * @throws ExecutionValidationException
     *             if the query is not supported.
     */
    private void checkSupportedQuery(ConnectorQueryData queryData) throws ExecutionValidationException {
        if (queryData.getSelect() == null) {
            final String msg = "no select found";
            logger.error(msg);
            throw new ExecutionValidationException(msg);
        } else if (queryData.getWindow() == null) {
            final String msg = "no window found";
            logger.error(msg);
            throw new ExecutionValidationException(msg);
        }
    }
}
