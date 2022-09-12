package com.mangatayy.tools.data.aggregate.config;

import com.mangatayy.tools.data.aggregate.service.AggregateTask;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.config.CronTask;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;

/**
 * @author yuyong
 * @date 2022/8/31 19:13
 */
@RequiredArgsConstructor
@Slf4j
public class AggregateCronTaskRegistrar implements DisposableBean {

    private final Map<Long, ScheduledFuture<?>> scheduledTasks = new ConcurrentHashMap<>(16);

    private final TaskScheduler taskScheduler;


    public void addCronTask(AggregateTask task, String cronExpression) {
        if (task != null) {
            if (this.scheduledTasks.containsKey(task.getJobId())) {

                removeCronTask(task.getJobId());
            }
            this.scheduledTasks.put(task.getJobId(), scheduleCronTask(new CronTask(task, cronExpression)));
        }
    }


    public void removeCronTask(Long jobId) {
        ScheduledFuture<?> future = this.scheduledTasks.remove(jobId);
        log.info("remove old task :{}", jobId);
        if (future != null) {
            future.cancel(true);
        }
    }

    public ScheduledFuture<?> scheduleCronTask(CronTask cronTask) {
        return this.taskScheduler.schedule(cronTask.getRunnable(), cronTask.getTrigger());
    }


    @Override
    public void destroy() {
        for (ScheduledFuture<?> task : this.scheduledTasks.values()) {
            task.cancel(true);
        }

        this.scheduledTasks.clear();
    }
}
