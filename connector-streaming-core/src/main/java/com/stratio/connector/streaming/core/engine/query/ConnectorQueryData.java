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

import java.util.ArrayList;
import java.util.Collection;

import com.stratio.meta.common.logicalplan.Filter;
import com.stratio.meta.common.logicalplan.Limit;
import com.stratio.meta.common.logicalplan.Project;
import com.stratio.meta.common.logicalplan.Select;
import com.stratio.meta.common.logicalplan.Window;

/**
 * This class is a representation of a ElasticSearch query. Created by jmgomez on 15/09/14.
 */
public class ConnectorQueryData {

    /**
     * The projection.
     */
    private Project projection = null;

    /**
     * The filters.
     */
    private Collection<Filter> filterList = new ArrayList<>();

    /**
     * The select.
     */
    private Select select;
    private Limit limit;
    private String queryId;
    private Window window;

    /**
     * Add a filter.
     *
     * @param filter the filter.
     */
    public void addFilter(Filter filter) {

        filterList.add(filter);
    }

    /**
     * This method ask query if has projection.
     *
     * @return true if the query has projection. False in other case.
     */
    public boolean hasProjection() {

        return projection != null;
    }

    /**
     * Get the projection.
     *
     * @return the projection,
     */
    public Project getProjection() {

        return projection;
    }

    /**
     * Set the projection.
     *
     * @param projection the projection.
     */
    public void setProjection(Project projection) {

        this.projection = projection;
    }

    /**
     * Get the filter.
     *
     * @return the filter.
     */
    public Collection<Filter> getFilter() {
        return filterList;
    }

    /**
     * This method ask query if has filter list.
     *
     * @return true if the query has filter list. False in other case.
     */
    public boolean hasFilterList() {
        return !filterList.isEmpty();
    }

    /**
     * return the select.
     *
     * @return the select.
     */
    public Select getSelect() {
        return select;
    }

    /**
     * Add a select type.
     *
     * @param select the select.
     */
    public void setSelect(Select select) {
        this.select = select;

    }

    public Limit getLimit() {
        return limit;
    }

    public void setLimit(Limit limit) {
        this.limit = limit;
    }

    public String getQueryId() {
        return queryId;
    }

    public void setQueryId(String queryId) {
        this.queryId = queryId;
    }

    /**
     * @return
     */
    public Window getWindow() {
        return window;
    }

    public void setWindow(Window window) {
        this.window = window;
    }
}
