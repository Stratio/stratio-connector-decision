/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership. The STRATIO (C) licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.stratio.connector.streaming.ftest.functionalMetadata;

import org.junit.Ignore;
import org.junit.Test;

import com.stratio.connector.commons.ftest.functionalMetadata.GenericMetadataCreateFT;
import com.stratio.connector.commons.ftest.helper.IConnectorHelper;
import com.stratio.connector.streaming.ftest.helper.StreamingConnectorHelper;
import com.stratio.crossdata.common.exceptions.ConnectionException;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.exceptions.InitializationException;

/**
 * Created by jmgomez on 5/09/14.
 */
public class StreamingCreateMetadataFunctionalFT extends GenericMetadataCreateFT {

    @Override
    protected IConnectorHelper getConnectorHelper() {
        StreamingConnectorHelper sConnectorHelper = null;
        try {
            sConnectorHelper = new StreamingConnectorHelper(getClusterName());
            return sConnectorHelper;
        } catch (ConnectionException e) {
            e.printStackTrace();
        } catch (InitializationException e) {
            e.printStackTrace();
        }
        return sConnectorHelper;
    }

    @Override
    @Test
    @Ignore
    public void createCatalogTest() throws ConnectorException {
    }

    @Override
    @Test
    @Ignore
    public void createCatalogWithOptionsTest() throws ConnectorException {
    }

    @Override
    @Test
    @Ignore
    public void createCatalogExceptionCreateTwoCatalogTest() throws ConnectorException {
    }

    @Override
    @Test
    @Ignore
    public void createTableWithoutTableTest() throws ConnectorException {
    }

}
