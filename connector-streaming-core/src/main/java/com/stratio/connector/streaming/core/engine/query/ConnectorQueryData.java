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

import java.util.ArrayList;
import java.util.Collection;


import com.stratio.meta.common.logicalplan.Filter;
import com.stratio.meta.common.logicalplan.Limit;
import com.stratio.meta.common.logicalplan.Project;
import com.stratio.meta.common.logicalplan.Select;

/**
 * This class is a representation of a ElasticSearch query.
 * Created by jmgomez on 15/09/14.
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
     * The matchList;
     */
    private Collection<Filter> matchList = new ArrayList<>();

    /**
     * The select.
     */
    private Select select;
    private Limit limit;

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
     * Add filter to the matchList.
     *
     * @param filter the filter to add.
     */
    public void addMatch(Filter filter) {

        matchList.add(filter);
    }

    /**
     * This method ask query if has match list.
     *
     * @return true if the query has match list. False in other case.
     */

    public boolean hasMatchList() {

        return !matchList.isEmpty();
    }

    /**
     * Return The matchList
     *
     * @return the matchList
     */
    public Collection<Filter> getMatchList() {
        return matchList;
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

    public void setLimit(Limit limit) {
        this.limit = limit;
    }

    public Limit getLimit() {
        return limit;
    }
}
