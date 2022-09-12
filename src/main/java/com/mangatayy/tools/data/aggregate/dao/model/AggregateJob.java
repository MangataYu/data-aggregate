package com.mangatayy.tools.data.aggregate.dao.model;

import com.mangatayy.tools.data.aggregate.constant.AggregateConstants;
import com.mangatayy.tools.data.aggregate.util.PeriodType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

/**
 * 汇聚任务配置信息<br>
 * 汇聚任务的数据时间start_time计算公式为<br>
 * start_time = ${time}-crontab_period_offset_minutes-crontab_period_type*crontab_period_delay <br>
 * 1）当auto_flag = 1即任务由程序定时调度，此时需要配置job_cron_exp定时调度周期表达式和crontab_period_type
 * 如果 job_cron_exp="0 3/15 * * * ？",crontab_period_type=mi15,crontab_period_offset_minutes=3,crontab_period_delay=2
 * 那么当程序第一次执行时 16:03:00时，${time}=16:03:00, start_time= 16:03:00 - 3 - 15 * 2 = 15:30:00<br>
 * 2)当auto_flag=0，parent_job_id不为空，triggered_condition不为空，此时程序被parent_job_id的任务触发执行，
 * 此时的${time}为父任务的start_time,例如，triggered_condition="HHmmss=010000",crontab_period_type=day,
 * crontab_period_offset_minutes=60,crontab_period_delay=1,即任务在父任务执行01:00:00的start_time时被触发执行，
 * 那么当前任务的start_time="2022-08-15 01:00:00" - 60 - day * 1 = "2022-08-14 00:00:00"
 * @author yuyong
 * @date 2021/12/17 15:04
 */
@Data
@TableName(value = AggregateConstants.JOB_TABLE, schema = AggregateConstants.SCHEMA)
public class AggregateJob {

    @TableId
    private Long id;

    private String job_name;

    /**
     * Pm性能数据源表
     */
    private String source_table;
    /**
     * Pm性能源数据对应的要汇聚的基础counter配置表名
     */
    private String source_counter_table;
    /**
     * 汇聚时从counter配置表选取的汇聚函数字段，只有两个aggregate_function和space_aggregate_function
     */
    private String function_field;
    /**
     * Pm汇聚的目标表
     */
    private String target_table;
    /**
     * 性能数据表在汇聚时需要提取的字段，如 eci, operator_corp 等多个字段用英文逗号分隔
     */
    private String source_ne_fields;
    /**
     * 当前任务是否被暂停执行，即是否停用任务，0：没有停用，在用；1：已经停用，不再执行<br>
     * 默认值0
     */
    private Integer parse_flag;

    private Integer job_level;

    /**
     * 是否需要进行数据的预处理 （底层15分钟数据的处理，部分指标需要提前计算） 0不需要，1需要<br>
     * 默认值0
     */
    private Integer pre_treat_flag;

    /**
     * 是否需要删除target_table的target_time时段数据记录，0:不需要;1:需要<br>
     * 此参数用于手动汇聚，或者重复汇聚时，此时目标表target_table中已经存在要汇聚的时段的数据<br>
     * 默认值0
     */
    private Integer delete_target_time_flag;

    /**
     * job父级ID，与trigger_child_condition有关，当父任务符合trigger_child_condition设置的时间时，将启动children列表中的任务<br>
     * 当auto_flag为1时，此属性会失效
     */
    private Long parent_job_id;
    /**
     * 当前任务被父级任务触发的条件，都是以父级任务start_time为条件，触发天粒度即填写 HH = 23 HH为时间格式
     */
    private String triggered_condition;

    /**
     * 汇聚job定时表达式 和auto_flag为1时必填
     */
    private String job_cron_exp;

    /**
     * 当前任务是否自动执行， 0：不自动执行，需要被触发，1：自动执行，随程序启动后定时调度，需要同时配置 job_cron_exp<br>
     * 默认值为0
     */
    private Integer auto_flag;

    /**
     * 要汇聚的时间周期，也是任务执行周期，必填参数
     */
    private PeriodType crontab_period_type;
    /**
     * 任务执行的业务数据时间start_time，与程序执行时间的偏移分钟数，差值，默认为0 <br>
     * auto_flag为1时，为程序执行时间与数据业务时间的差值，被触发执行时（auto_flag=0）为父级任务的startTime与当前要执行的任务的数据业务时间的差值
     */
    private Integer crontab_period_offset_minutes;
    /**
     * 任务执行的业务数据时间start_time，与程序执行时间的偏移周期数 默认值为0
     */
    private Integer crontab_period_delay;

    /**
     * 业务周期类型值，即period_type字段自动填充的值，为空时不处理
     */
    private Integer period_type_value;
    /**
     * 业务维度类型值，即region_type字段自动填充的值，为空时不处理
     */
    private Integer region_type_value;

    /**
     * 子任务列表，即parent_job_id=this.id的，且不自动执行的（auto_flag=0）所有记录
     */
    @TableField(exist = false)
    private List<AggregateJob> children = new ArrayList<>();

    @TableField(exist = false)
    private Boolean removeNullEnci;

}

