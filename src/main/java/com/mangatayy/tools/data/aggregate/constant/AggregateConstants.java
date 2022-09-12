package com.mangatayy.tools.data.aggregate.constant;

import java.time.format.DateTimeFormatter;

/**
 * @author yuyong
 * @date 2022/8/11 17:13
 */
public interface AggregateConstants {
    String SCHEMA = "pm";
    String COUNTER_TABLE = "tm_aggregate_counter";

    String JOB_TABLE = "tm_aggregate_job";

    int FLAG_ON = 1;

    int FLAG_OFF = 0;

    String MAPPER_PACKAGE = "com.asiainfo.retina.data.gather.dao.mapper";

    int IS_NOT_PARSED = 0;

    String CREATE_TIME = "createtime";

    int INSERT_BATCH_SIZE = 5000;

    String ENCI = "enci";

    DateTimeFormatter yyyyMMddHHmm = DateTimeFormatter.ofPattern("yyyyMMddHHmm");
    DateTimeFormatter HHmmssSSS = DateTimeFormatter.ofPattern("HHmmssSSS");
}
