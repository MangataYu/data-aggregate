package com.mangatayy.tools.data.aggregate.service;

import com.mangatayy.tools.data.aggregate.constant.AggregateConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.jexl2.Expression;
import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.jexl2.MapContext;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.support.JdbcUtils;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author yuyong
 * @date 2022/3/24 16:33
 */
@Slf4j
public class PmResultSetExtractor implements ResultSetExtractor<Object> {
    private final static JexlEngine JEXL_ENGINE = new JexlEngine();
    private int rowCount;
    private int columnCount;
    private String insertSql;
    private int createTimeIndex;

    private boolean containEnciColumn = false;

    private RowProcessor rowProcessor;

    private final List<Object[]> records = new ArrayList<>(AggregateConstants.INSERT_BATCH_SIZE);
    private final LocalDateTime createTime = LocalDateTime.now();

    private final boolean removeNullEnci;
    private final String targetTable;
    private final JdbcTemplate jdbcTemplate;
    private final Map<String, String> kpiCounters;
    private final Map<String, Integer> paramMap;

    public PmResultSetExtractor(String targetTable, JdbcTemplate jdbcTemplate, Map<String, String> kpiCounters,
                                Map<String, Integer> paramMap, boolean removeNullEnci) {
        this.targetTable = targetTable;
        this.jdbcTemplate = jdbcTemplate;
        this.kpiCounters = kpiCounters;
        this.paramMap = paramMap;
        this.removeNullEnci = removeNullEnci;
    }

    //@Override
    public void processRow(ResultSet rs) throws SQLException {
        rowCount = rowProcessor.process(rs, rowCount);
        if (AggregateConstants.INSERT_BATCH_SIZE == records.size()) {
            jdbcTemplate.batchUpdate(insertSql, new PmBatchPreparedStatementSetter(records));
            log.info("batch target table name {}, line number {} list size: {}", targetTable, rowCount, records.size());
            records.clear();
        }
    }

    private int processRowValue(ResultSet resultSet, int currentCount) throws SQLException {
        List<Object> row = new ArrayList<>(16);
        Map<String, Object> varMap = new HashMap<>();
        for (int i = 0; i < columnCount; i++) {
            if (i == createTimeIndex ) {
                row.add(createTime);
            } else {
                row.add(resultSet.getObject(i+1));
            }
        }
        paramMap.forEach((fieldName, index) -> varMap.put(fieldName, row.get(index)));
        kpiCounters.forEach((fieldName, formula) -> {
            Expression expression = JEXL_ENGINE.createExpression(formula);
            row.add(expression.evaluate(new MapContext(varMap)));
        });
        records.add(row.toArray());
        return currentCount + 1;
    }

    @Override
    public Object extractData(ResultSet rs) throws SQLException, DataAccessException {
        if (this.rowCount == 0) {
            ResultSetMetaData metaData = rs.getMetaData();
            this.columnCount = metaData.getColumnCount();
            StringBuilder allColumns = new StringBuilder();
            StringBuilder values = new StringBuilder();
            for (int i = 0; i < this.columnCount; ++i) {
                String columnName = JdbcUtils.lookupColumnName(metaData, i + 1).toLowerCase();
                if (columnName.equals(AggregateConstants.ENCI)) {
                    containEnciColumn = true;
                }
                if (AggregateConstants.CREATE_TIME.equals(columnName)) {
                    createTimeIndex = i;
                }
                allColumns.append(columnName).append(",");
                values.append("?,");
                if (paramMap.containsKey(columnName)) {
                    paramMap.put(columnName, i);
                }
            }
            kpiCounters.forEach((fieldName, formula) -> {
                allColumns.append(fieldName).append(",");
                values.append("?,");
            });
            this.insertSql = "insert into " + targetTable + " (" + allColumns.substring(0, allColumns.length() - 1)
                    + ") values(" + values.substring(0, values.length() - 1) + ")";
            log.debug("insert sql :{}", insertSql);
            if (removeNullEnci && containEnciColumn) {
                rowProcessor = (resultSet, currentCount) -> {
                    Object enci = resultSet.getObject(AggregateConstants.ENCI);
                    if (enci == null) {
                        log.debug("enci is null, skip this row");
                        return currentCount + 1;
                    }
                    return processRowValue(resultSet, currentCount);
                };
            } else {
                rowProcessor = this::processRowValue;
            }
        }
        while (rs.next()) {
            processRow(rs);
        }
        if (!records.isEmpty()) {
            jdbcTemplate.batchUpdate(insertSql, new PmBatchPreparedStatementSetter(records));
            log.info("batch left target table name {}, line number {} list size: {}", targetTable, rowCount, records.size());
            records.clear();
        }
        return null;
    }

    interface RowProcessor {
        int process(ResultSet resultSet, int currentCount) throws SQLException;
    }

    public static class PmBatchPreparedStatementSetter implements BatchPreparedStatementSetter {
        private final List<Object[]> records;

        public PmBatchPreparedStatementSetter(List<Object[]> records) {
            this.records = records;
        }
        @Override
        public void setValues(PreparedStatement ps, int i) throws SQLException {
            Object[] data = records.get(i);
            for (int j = 0; j < data.length; j++) {
                ps.setObject(j + 1, data[j]);
            }
        }

        @Override
        public int getBatchSize() {
            return records.size();
        }
    }
}





