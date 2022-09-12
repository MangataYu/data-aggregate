package com.mangatayy.tools.data.aggregate.service.impl;

import com.mangatayy.tools.data.aggregate.constant.AggregateConstants;
import com.mangatayy.tools.data.aggregate.dao.model.AggregateJob;
import com.mangatayy.tools.data.aggregate.entity.JobRequestVO;
import com.mangatayy.tools.data.aggregate.service.AggregateJobCounterService;
import com.mangatayy.tools.data.aggregate.service.AggregateTaskTest;
import com.mangatayy.tools.data.aggregate.service.IJobService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * @author yuyong
 * @date 2022/8/22 14:24
 */
@Service
@RequiredArgsConstructor
@Slf4j
@Profile(value = {"test"})
public class JobServiceTestImpl implements IJobService {
    private final AggregateJobCounterService jobCounterService;
    private final JdbcTemplate jdbcTemplate;

    @Value("${aggregate-task.remove-null-enci:true}")
    private boolean removeNullEnci;

    //查询所有任务

    //查询正在执行的JOB
    public List<AggregateJob> queryAllOn() {
        return jobCounterService.queryAllOnJob();
    }

    //立即执行某个job
    public void executeNow(JobRequestVO requestVO) throws ExecutionException, InterruptedException {
        AggregateJob job = null;
        if (requestVO.getJob_id() != null) {
            job = jobCounterService.queryOne(requestVO.getJob_id());

        } else if (requestVO.getJob_name() != null) {
            job = jobCounterService.queryOneByName(requestVO.getJob_name());

        }
        if (requestVO.getTask_id() != null && !requestVO.getTask_id().isEmpty()) {
            requestVO.setStart_time(LocalDateTime.parse(requestVO.getTask_id(), AggregateConstants.yyyyMMddHHmm));
        }
        if (job != null) {
            job.setRemoveNullEnci(removeNullEnci);
            runSingleTask(new AggregateTaskTest(jdbcTemplate, jobCounterService, job, requestVO.getStart_time()));
        } else {
            throw new RuntimeException("job id or name not found");
        }
    }

    public void executeWithChildren(JobRequestVO requestVO) throws ExecutionException, InterruptedException {
        AggregateJob job = null;
        if (requestVO.getJob_id() != null) {
            job = jobCounterService.queryOne(requestVO.getJob_id());

        } else if (requestVO.getJob_name() != null) {
            job = jobCounterService.queryOneByName(requestVO.getJob_name());
        }
        if (job != null) {
            job = jobCounterService.queryJobWithChildren(job.getId());
            job.setRemoveNullEnci(removeNullEnci);
            AggregateTaskTest task = new AggregateTaskTest(jdbcTemplate, jobCounterService, job, requestVO.getStart_time());
            runSingleTask(task);
        } else {
            throw new RuntimeException("job id or name not found");
        }
    }

    private void runSingleTask(Thread task) throws ExecutionException, InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        Future<?> future = executorService.submit(task);
        future.get();
        executorService.shutdown();
    }


    public void reScheduledTask(Long id) {
        log.info("===test====");
    }

    public void reScheduledAll() {
        log.info("===test====");
    }

}
