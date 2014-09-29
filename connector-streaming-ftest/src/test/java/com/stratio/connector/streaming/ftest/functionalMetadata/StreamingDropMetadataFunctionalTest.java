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

package com.stratio.connector.streaming.ftest.functionalMetadata;

import com.stratio.connector.commons.ftest.functionalMetadata.GenericMetadataCreateTest;
import com.stratio.connector.commons.ftest.functionalMetadata.GenericMetadataDropTest;
import com.stratio.connector.commons.ftest.helper.IConnectorHelper;
import com.stratio.connector.streaming.ftest.helper.StreamingConnectorHelper;
import com.stratio.meta.common.exceptions.ConnectionException;
import com.stratio.meta.common.exceptions.InitializationException;

/**
 * Created by jmgomez on 5/09/14.
 */
public class StreamingDropMetadataFunctionalTest extends GenericMetadataDropTest {

    @Override
    protected IConnectorHelper getConnectorHelper() {
    	StreamingConnectorHelper esConnectorHelper = null;
        try {
            esConnectorHelper = new StreamingConnectorHelper(getClusterName());
            return esConnectorHelper;
        } catch (ConnectionException e) {
            e.printStackTrace();
        } catch (InitializationException e) {
            e.printStackTrace();
        }
        return esConnectorHelper;
    }
}
