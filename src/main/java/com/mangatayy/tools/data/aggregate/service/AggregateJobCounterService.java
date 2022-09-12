package com.mangatayy.tools.data.aggregate.service;

import com.mangatayy.tools.data.aggregate.dao.mapper.AggregateCounterMapper;
import com.mangatayy.tools.data.aggregate.dao.mapper.AggregateJobMapper;
import com.mangatayy.tools.data.aggregate.dao.model.AggregateCounter;
import com.mangatayy.tools.data.aggregate.constant.AggregateConstants;
import com.mangatayy.tools.data.aggregate.dao.model.AggregateJob;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author yuyong
 * @date 2022/3/24 15:10
 */
@Service
@RequiredArgsConstructor
public class AggregateJobCounterService {

    private final AggregateJobMapper jobMapper;
    private final AggregateCounterMapper counterMapper;

    public List<AggregateCounter> queryCounters(String tableName) {
        return counterMapper.selectList(new LambdaQueryWrapper<AggregateCounter>()
                .eq(AggregateCounter::getTable_origin, tableName));
    }

    public AggregateJob queryOne(Long id) {
        return jobMapper.selectById(id);
    }

    public AggregateJob queryOneByName(String jobName) {
       List<AggregateJob> jobs = jobMapper.selectList(new LambdaQueryWrapper<AggregateJob>()
               .eq(AggregateJob::getJob_name, jobName));
       if (jobs != null && !jobs.isEmpty()) {
           return jobs.get(0);
       }
       return null;
    }


    /**查询所有开启的任务
     * @return job map key: id, value: job
     */
    public List<AggregateJob> queryAllOnJob() {
        return jobMapper.selectList(new LambdaQueryWrapper<AggregateJob>()
                .eq(AggregateJob::getParse_flag, AggregateConstants.IS_NOT_PARSED));
    }


    public Map<Long, AggregateJob> queryAutoJobTree() {
        Map<Long, AggregateJob> map = queryTreeMap();
        map.values().removeIf(job -> job.getAuto_flag() == AggregateConstants.FLAG_OFF);
        return map;
    }

    public AggregateJob queryJobWithChildren(Long id) {
        Map<Long, AggregateJob> map = queryTreeMap();
        return map.get(id);
    }

    private Map<Long, AggregateJob> queryTreeMap() {
        List<AggregateJob> jobs = queryAllOnJob();
        Map<Long, AggregateJob> map = jobs.stream().collect(Collectors.toMap(AggregateJob::getId, a->a));
        jobs.forEach(job -> {
            if (job.getParent_job_id() != null && job.getAuto_flag() == AggregateConstants.FLAG_OFF
                    && map.containsKey(job.getParent_job_id())) {
                map.get(job.getParent_job_id()).getChildren().add(job);
            }
        });
        return map;
    }

}
