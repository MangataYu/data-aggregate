package com.mangatayy.tools.data.aggregate.config;

import com.mangatayy.tools.data.aggregate.dao.model.AggregateJob;
import com.mangatayy.tools.data.aggregate.service.AggregateJobCounterService;
import com.mangatayy.tools.data.aggregate.service.AggregateTask;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author yuyong
 * @date 2022/8/11 16:28
 */
@Configuration
@RequiredArgsConstructor
@Slf4j
public class AggregateTaskAutoScheduledConfig {

    private final AggregateJobCounterService jobCounterService;
    private final JdbcTemplate jdbcTemplate;

    @Value("${aggregate-task.remove-null-enci:true}")
    private boolean removeNullEnci;
    @Value("${aggregate-task.thread-pool-size:20}")
    private int availableProcessors;
    @Value("${aggregate-task.auto-schedule-task:true}")
    private boolean autoScheduleTask;

    @Bean
    public TaskScheduler taskScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setThreadNamePrefix("agg-pool-");
        // 设置拒绝策略
        scheduler.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        log.info("availableProcessors: {}", availableProcessors);
        scheduler.setPoolSize(availableProcessors);
        // 等待所有任务结束后再关闭线程池
        scheduler.setWaitForTasksToCompleteOnShutdown(true);
        return scheduler;
    }

    @Bean
    public AggregateCronTaskRegistrar aggregateCronTaskRegistrar(TaskScheduler taskScheduler) {
        AggregateCronTaskRegistrar registrar = new AggregateCronTaskRegistrar(taskScheduler);
        if (autoScheduleTask) {
            Map<Long, AggregateJob> autoJobMap = jobCounterService.queryAutoJobTree();
            autoJobMap.values().forEach(job -> {
                job.setRemoveNullEnci(removeNullEnci);
                if (job.getJob_cron_exp() != null && !job.getJob_cron_exp().isEmpty()) {
                    try {
                        registrar.addCronTask(new AggregateTask(jdbcTemplate, jobCounterService, job), job.getJob_cron_exp());
                        log.info("add auto cron job SUCCESS id:{}, name:{}, crontab:{}", job.getId(), job.getJob_name(), job.getJob_cron_exp());
                    } catch (Exception e) {
                        log.error("add auto cron job ERROR id:{}, name:{}, crontab:{}",
                                job.getId(), job.getJob_name(), job.getJob_cron_exp(), e);
                    }
                } else {
                    log.info("job id :{} name:{} cron expression is null", job.getId(), job.getJob_name());
                }
            });
        } else {
            log.warn("WARN auto schedule task setting is close, didn't schedule any task");
        }
        return registrar;
    }

}
