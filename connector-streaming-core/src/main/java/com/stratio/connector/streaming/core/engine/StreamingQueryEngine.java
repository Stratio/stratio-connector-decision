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
package com.stratio.connector.streaming.core.engine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.engine.UniqueProjectQueryEngine;
import com.stratio.connector.streaming.core.connection.StreamingConnectionHandler;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.logicalplan.LogicalWorkflow;
import com.stratio.meta.common.result.QueryResult;
import com.stratio.streaming.api.IStratioStreamingAPI;

public class StreamingQueryEngine extends UniqueProjectQueryEngine<IStratioStreamingAPI> {




    /**
     * The log.
     */
    final Logger logger = LoggerFactory.getLogger(this.getClass());

    public StreamingQueryEngine(StreamingConnectionHandler connectionHandle) {

        super(connectionHandle);

    }



    @Override
    protected QueryResult execute(LogicalWorkflow logicalWorkflow, Connection<IStratioStreamingAPI> connection)
            throws UnsupportedException, ExecutionException {

    	 throw new UnsupportedException("execute not supported in Streaming connector");
    }
}
