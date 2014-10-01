package com.stratio.connector.streaming.core.engine.query.util;

import com.stratio.meta.common.logicalplan.Project;
import com.stratio.meta2.common.data.TableName;

/**
 * Created by jmgomez on 1/10/14.
 */
public class StreamUtil {

    public static String createStreamName(Project project) {
        return createStreamName(project.getCatalogName(), project.getTableName().getName());
    }

    public static String createStreamName(TableName tableName) {
        return createStreamName(tableName.getCatalogName().getName(), tableName.getName());
    }

    private static String createStreamName(String catalog, String table) {
        return catalog + "_" + table;
    }

    public static String createOutgoingName(String streamName, String metaQueryId) {
        return streamName + "_" + metaQueryId.replace("-", "_");
    }
}
