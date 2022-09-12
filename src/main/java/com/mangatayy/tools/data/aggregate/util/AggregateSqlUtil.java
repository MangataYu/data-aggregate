package com.mangatayy.tools.data.aggregate.util;

import com.mangatayy.tools.data.aggregate.constant.AggregateConstants;
import com.mangatayy.tools.data.aggregate.dao.model.AggregateCounter;
import com.mangatayy.tools.data.aggregate.dao.model.AggregateJob;
import com.baomidou.mybatisplus.core.toolkit.StringUtils;

import java.util.List;

/**
 * @author yuyong
 * @date 2021/12/22 10:36
 */
public class AggregateSqlUtil {

    public static String aggregateSql(AggregateJob job, List<AggregateCounter> counterList) {
        String sourceNes = neFields(job.getSource_ne_fields());
        String selectFields = selectFields(counterList, job.getFunction_field());
        String sourceFields = neFields(job.getSource_ne_fields());
        String intoFields = intoFields(counterList);
        boolean isMulti = job.getPeriod_type_value() != null && job.getRegion_type_value() != null;
        String typeSqlPart = isMulti ? (", period_type, region_type") : "";
        String typeSqlVal = isMulti ? ("," + job.getPeriod_type_value() + ", " + job.getRegion_type_value()): "";
        String sql = " INSERT INTO " + job.getTarget_table() + " (" + intoFields + sourceFields +
                " start_time," + AggregateConstants.CREATE_TIME + typeSqlPart + " )" +
                " SELECT " + selectFields + sourceNes + "?, ?" + typeSqlVal +
                " FROM " + job.getSource_table() + "  WHERE start_time >= ? AND start_time < ? ";
        String groupFields = sourceNes;
        if (StringUtils.isNotBlank(groupFields)) {
            groupFields = groupFields.substring(0, groupFields.length() - 1);
            sql += " GROUP BY " + groupFields;
        }
        return sql;

    }

    private static String selectFields(List<AggregateCounter> counterList, String functionName) {
        StringBuilder selectFieldSql = new StringBuilder();
        if ("space_aggregate_function".equals(functionName)) {
            counterList.forEach(counter -> selectFieldSql.append(counter.getSpace_aggregate_function())
                    .append("(").append(counter.getCounter_field_name()).append("),"));
        } else {
            counterList.forEach(counter -> selectFieldSql.append(counter.getAggregate_function())
                    .append("(").append(counter.getCounter_field_name()).append("),"));
        }
        return selectFieldSql.toString();
    }

    private static String intoFields(List<AggregateCounter> counterList) {
        StringBuilder intoFieldSql = new StringBuilder();
        counterList.forEach(counter -> intoFieldSql.append(counter.getCounter_field_name()).append(","));
        return intoFieldSql.toString();
    }

    private static String neFields(String fields) {
        StringBuilder fieldSql = new StringBuilder();
        if (StringUtils.isNotBlank(fields)) {
            String[] fieldArr = fields.split(",");
            for (String s : fieldArr) {
                fieldSql.append(s).append(",");
            }
        }
        return fieldSql.toString();
    }
}
