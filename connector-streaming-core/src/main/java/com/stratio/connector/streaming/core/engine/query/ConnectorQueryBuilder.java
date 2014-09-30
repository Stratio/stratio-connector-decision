package com.stratio.connector.streaming.core.engine.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.logicalplan.Filter;
import com.stratio.meta.common.logicalplan.Project;
import com.stratio.meta.common.logicalplan.Select;
import com.stratio.meta.common.statements.structures.relationships.Operator;
import com.stratio.meta.common.statements.structures.relationships.Relation;
import com.stratio.meta.common.statements.structures.window.TimeUnit;
import com.stratio.meta.common.statements.structures.window.Window;
import com.stratio.meta.common.statements.structures.window.WindowType;
import com.stratio.meta2.common.statements.structures.selectors.ColumnSelector;
import com.stratio.meta2.common.statements.structures.selectors.Selector;
import com.stratio.meta2.common.statements.structures.selectors.StringSelector;

/**
 * Created by jmgomez on 30/09/14.
 */
public class ConnectorQueryBuilder {

    public String createQuery(ConnectorQueryData queryData) throws ExecutionException {
        Project projection = queryData.getProjection();

        // TODO String metaQueryId = queryData.getSelect().getQueryID();
        String metaQueryId = "01234";// TODO
        String streamName = projection.getCatalogName() + "_" + projection.getTableName().getName();
        String outgoing = streamName + "_" + metaQueryId.replace("-", "_");

        StringBuilder querySb = new StringBuilder("from ");
        querySb.append(streamName);
        if (queryData.hasFilterList()) {
            querySb.append("[");
            Iterator<Filter> filterIter = queryData.getFilter().iterator();
            while (filterIter.hasNext()) {
                Relation rel = filterIter.next().getRelation();
                querySb.append(getFieldName(rel.getLeftTerm())).append(" ")
                                .append(getSiddhiOperator(rel.getOperator())).append(" ");
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
                    case FUNCTION:
                    case RELATION:
                    case ASTERISK:
                    default:
                        throw new ExecutionException("Type " + rel.getRightTerm().getType() + "unsupported");
                    }
                }

                if (filterIter.hasNext()) {
                    querySb.append(" and ");
                }
            }
            querySb.append("]");
        }

        // TODO test if(queryData.hasWindow())
        Window window = new Window(WindowType.TEMPORAL);
        window.setTimeWindow(5, TimeUnit.SECONDS);
        if (window != null) {
            if (window.getType() == WindowType.TEMPORAL) {
                querySb.append("#window.timeBatch( ").append(window.getDurationInMilliseconds())
                                .append(" milliseconds)");
            } else if (window.getType() == WindowType.NUM_ROWS) {
                // TODO not supported
            }
        }

        List<String> ids = new ArrayList<>();
        Select selectionClause = queryData.getSelect();
        Set<String> columnMetadataList = selectionClause.getColumnMap().keySet();

        if (columnMetadataList == null || columnMetadataList.isEmpty()) {
            throw new ExecutionException("The query has to retrieve data");
        } else {

            for (String columnName : columnMetadataList) {
                String[] splitColumnName = columnName.split("\\.");
                ids.add(splitColumnName[splitColumnName.length - 1]);
            }

        }

        String idsStr = Arrays.toString(ids.toArray()).replace("[", "").replace("]", "");
        querySb.append(" select ").append(idsStr).append(" insert into ");
        querySb.append(outgoing);
        return querySb.toString();
    }

    private static String getFieldName(Selector selector) throws ExecutionException {
        String field = null;
        if (selector instanceof ColumnSelector) {
            ColumnSelector columnSelector = (ColumnSelector) selector;
            field = columnSelector.getName().getName();
        } else
            throw new ExecutionException("Left selector must be a columnSelector in filters");
        return field;
    }

    private static String getSiddhiOperator(Operator operator) {
        // TODO validation
        String siddhiOperator = operator.toString();
        if (siddhiOperator.equalsIgnoreCase("=")) {
            siddhiOperator = "==";
        } else if (siddhiOperator.equalsIgnoreCase("<>")) {
            siddhiOperator = "!=";
        }
        return siddhiOperator;
    }

}
