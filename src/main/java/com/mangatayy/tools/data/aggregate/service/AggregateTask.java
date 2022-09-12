package com.mangatayy.tools.data.aggregate.service;

import com.mangatayy.tools.data.aggregate.constant.AggregateConstants;
import com.mangatayy.tools.data.aggregate.dao.model.AggregateCounter;
import com.mangatayy.tools.data.aggregate.dao.model.AggregateJob;
import com.mangatayy.tools.data.aggregate.util.AggregateSqlUtil;
import com.mangatayy.tools.data.aggregate.util.PeriodType;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * @author yuyong
 * @date 2022/8/12 10:43
 */
@Slf4j
public class AggregateTask extends Thread {
    protected final JdbcTemplate jdbcTemplate;
    protected final AggregateJobCounterService jobCounterService;
    protected final AggregateJob job;
    private LocalDateTime dataTime;
    private LocalDateTime parentStartTime;
    private final static Logger aggregateLogger = LoggerFactory.getLogger("aggregateLogger");

    public AggregateTask(JdbcTemplate jdbcTemplate, AggregateJobCounterService jobCounterService, AggregateJob job) {
        this.jdbcTemplate = jdbcTemplate;
        this.jobCounterService = jobCounterService;
        this.job = job;
    }

    public AggregateTask(JdbcTemplate jdbcTemplate, AggregateJobCounterService jobCounterService,
                         AggregateJob job, LocalDateTime dataTime) {
        this.jdbcTemplate = jdbcTemplate;
        this.jobCounterService = jobCounterService;
        this.job = job;
        this.dataTime = dataTime;
    }

    public AggregateTask(JdbcTemplate jdbcTemplate, AggregateJobCounterService jobCounterService,
                         AggregateJob job, String threadName, LocalDateTime parentStartTime) {
        super(threadName);
        this.jdbcTemplate = jdbcTemplate;
        this.jobCounterService = jobCounterService;
        this.job = job;
        this.parentStartTime = parentStartTime;
    }

    public Long getJobId() {
        return job.getId();
    }

    public void run() {
        LocalDateTime startTime;
        if (dataTime != null) {
            startTime = dataTime;
        } else if (parentStartTime != null) {
            startTime = calculateTime(parentStartTime, job);
        } else {
            startTime = calculateTime(LocalDateTime.now().withSecond(0).withNano(0), job);
        }
        LocalDateTime endTime = getEndTime(job.getCrontab_period_type(), startTime);
        String threadName = Thread.currentThread().getName();
        log.info("job {} time {} start ", job.getJob_name(), startTime);
        aggregateLogger.info("START thread:{},job_id:{},job_name:{},data_time:{},children:{},exception:-", threadName, job.getId(),
                job.getJob_name(), startTime, listChildJobName());
        try {
            if (job.getDelete_target_time_flag() == AggregateConstants.FLAG_ON) {
                log.info("start to delete from {} where start_time >= {} end start_time < {} ", job.getTarget_table(), startTime, endTime);
                deleteTargetRecord(job.getTarget_table(), startTime, endTime);
            }
            if (job.getPre_treat_flag() == AggregateConstants.FLAG_ON) {
                executePreTask(startTime, endTime);
            } else {
                executeTask(startTime, endTime);
            }

            aggregateLogger.info("END thread:{},job_id:{},job_name:{},data_time:{},children:...(omit),exception:-", threadName, job.getId(),
                    job.getJob_name(), startTime);
        } catch (Exception e) {
            log.error("job {} error", job.getJob_name(), e);
            aggregateLogger.error("ERROR-END thread:{},job_id:{},job_name:{},data_time:{},children:{},exception:", threadName, job.getId(),
                    job.getJob_name(), startTime, listChildJobName(), e);
        } finally {
            triggerChild(startTime);
        }
        log.info("job {} time {} end ", job.getJob_name(), startTime);

    }


    public void executeTask(LocalDateTime startTime, LocalDateTime endTime) {
        LocalDateTime createTime = LocalDateTime.now();
        log.debug("job_id :{}, job_name:{} target_table:{} start_time:{} end_time:{}", job.getId(),
                job.getJob_name(), job.getTarget_table(), startTime, endTime);
        List<AggregateCounter> counters = jobCounterService.queryCounters(job.getSource_counter_table());
        String aggregateSql = AggregateSqlUtil.aggregateSql(job, counters);
        log.debug("agg sql: {}", aggregateSql);
        log.info("job {} start aggregate {} ... ...", job.getJob_name(), startTime);
        int row = jdbcTemplate.update(aggregateSql, startTime, createTime, startTime, endTime);
        log.info("job {} finish aggregate {} row :{}", job.getJob_name(), startTime, row);
    }

    public void executePreTask(LocalDateTime startTime, LocalDateTime endTime) {
        List<AggregateCounter> counters = jobCounterService.queryCounters(job.getSource_counter_table());
        Map<String, Integer> param = new HashMap<>();
        Map<String, String> kpiCounters = new LinkedHashMap<>();
        counters.forEach(counter -> {
            if (counter.getCalculate_param_flag() == AggregateConstants.FLAG_ON) {
                param.put(counter.getCounter_field_name().toLowerCase(), null);
            }
            if (counter.getCalculate_flag() == AggregateConstants.FLAG_ON) {
                kpiCounters.put(counter.getCounter_field_name().toLowerCase(), counter.getCalculate_formula());
            }
        });
        log.debug("job_id :{}, job_name:{} target_table:{} start_time:{} end_time:{}", job.getId(),
                job.getJob_name(), job.getTarget_table(), startTime, endTime);
        String sql = "select * from " + job.getSource_table() + " where start_time >= ? and start_time < ? ";
        // 正式环境代码
        PmResultSetExtractor extractor = new PmResultSetExtractor(job.getTarget_table(), jdbcTemplate, kpiCounters, param, job.getRemoveNullEnci());
        jdbcTemplate.query(sql, extractor, startTime, endTime);
        // 正式环境代码 end
        log.info("job {} end start_time {}", job.getJob_name(), startTime);
    }

    private LocalDateTime calculateTime(LocalDateTime time, AggregateJob job) {
        LocalDateTime dataTime = time;
        switch (job.getCrontab_period_type()) {
            case mi15:
                dataTime = time.minusMinutes(15L * job.getCrontab_period_delay());
                break;
            case hour:
                dataTime = time.minusHours(job.getCrontab_period_delay());
                break;
            case day:
                dataTime = time.minusDays(job.getCrontab_period_delay());
                break;
            case week:
                dataTime = time.minusWeeks(job.getCrontab_period_delay());
                break;
            case month:
                dataTime = time.minusMonths(job.getCrontab_period_delay());
                break;
            default:
                break;
        }
        return dataTime.minusMinutes(job.getCrontab_period_offset_minutes());
    }

    private LocalDateTime getEndTime(PeriodType periodType, LocalDateTime startTime) {
        switch (periodType) {
            case mi15:
                return startTime.plusMinutes(15);
            case hour:
                return startTime.plusHours(1);
            case day:
                return startTime.plusDays(1);
            case week:
                return startTime.plusWeeks(1);
            case month:
                return startTime.plusMonths(1);
            default:
                return null;
        }
    }

    private void deleteTargetRecord(String targetTable, LocalDateTime startTime, LocalDateTime endTime) {
        String deleteSql = " delete from " + targetTable + " where start_time >= ? and start_time < ? ";
        int count = jdbcTemplate.update(deleteSql, startTime, endTime);
        log.info("finish delete from {} where start_time >= {} end start_time < {} row {}", targetTable, startTime, endTime, count);
    }

    private void triggerChild(LocalDateTime startTime) {
        if (job.getChildren().isEmpty()) {
            log.debug("job {} no child to trigger", job.getJob_name());
            return;
        }
        log.debug("job {} trigger children start ", job.getJob_name());
        job.getChildren().forEach(childJob -> {
            childJob.setRemoveNullEnci(job.getRemoveNullEnci());
            if (isPropertyTime(startTime, childJob.getTriggered_condition())) {
                String tag = "-" + LocalDateTime.now().format(AggregateConstants.HHmmssSSS);
                new AggregateTask(jdbcTemplate, jobCounterService, childJob,
                        job.getJob_name() + "->" + childJob.getJob_name() + tag, startTime).start();
            } else {
                log.debug("job {} it's not time to trigger child job {}  ", job.getJob_name(), childJob.getJob_name());
            }
        });
    }

    private boolean isPropertyTime(LocalDateTime startTime, String condition) {
        String[] conditionParts = condition.split("=");
        if (conditionParts.length != 2) {
            return false;
        }
        String current = DateTimeFormatter.ofPattern(conditionParts[0].trim()).format(startTime);
        return current.equals(conditionParts[1].trim());
    }

    private List<String> listChildJobName() {
        List<String> list = new ArrayList<>();
        job.getChildren().forEach(job -> list.add(job.getJob_name()));
        return list;
    }

}
