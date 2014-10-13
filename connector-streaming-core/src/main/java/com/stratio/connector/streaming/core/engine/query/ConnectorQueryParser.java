/*
 * Stratio Meta
 *
 *   Copyright (c) 2014, Stratio, All rights reserved.
 *
 *   This library is free software; you can redistribute it and/or modify it under the terms of the
 *   GNU Lesser General Public License as published by the Free Software Foundation; either version
 *   3.0 of the License, or (at your option) any later version.
 *
 *   This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 *   even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 *   Lesser General Public License for more details.
 *
 *   You should have received a copy of the GNU Lesser General Public License along with this library.
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

    public ConnectorQueryData transformLogicalWorkFlow(Project logicalWorkFlow, String queryId) throws UnsupportedException {

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

            } else  if (lStep instanceof Window) {
                queryData.setWindow((Window) lStep);
            }else if (lStep instanceof Limit) {
                throw new UnsupportedException("LogicalStep [" + lStep.getClass().getCanonicalName()
                        + " not yet supported");
            } else{
                throw new UnsupportedException("LogicalStep [" + lStep.getClass().getCanonicalName() + " not supported");
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
