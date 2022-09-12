package com.mangatayy.tools.data.aggregate.entity;

import com.fasterxml.jackson.annotation.JsonFormat;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * @author yuyong
 * @date 2022/8/22 15:33
 */
@Data
@ApiModel(value = "任务执行信息")
public class JobRequestVO {
    @ApiModelProperty(value = "任务ID 查询时与job_name不同时生效，优先查ID")
    private Long job_id;
    @ApiModelProperty(value = "任务名称 查询时与job_id不同时生效，优先查ID")
    private String job_name;
    @ApiModelProperty(value = "要执行的任务的数据时间")
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime start_time;
    @ApiModelProperty(value = "DataOS时间适配")
    private String task_id;

    @ApiModelProperty(value = "DataOS调度时间偏移 默认为0")
    private int delay_minutes = 0;
}
