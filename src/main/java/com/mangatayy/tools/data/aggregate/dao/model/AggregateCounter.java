package com.mangatayy.tools.data.aggregate.dao.model;

import com.mangatayy.tools.data.aggregate.constant.AggregateConstants;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

/**
 * @author yuyong
 * @date 2021/12/16 15:00
 */
@Data
@TableName(value = AggregateConstants.COUNTER_TABLE, schema = AggregateConstants.SCHEMA)
public class AggregateCounter {
    @TableId
    private Long id;

    private String counter_name;

    private String counter_name_cn;

    private String counter_field_name;

    private String aggregate_function;

    private String space_aggregate_function;

    private String counter_unit;

    private Integer calculate_param_flag;

    private Integer calculate_flag;

    private String calculate_formula;

    private String table_origin;

    private String table_alias;

    private String network_type;

}
