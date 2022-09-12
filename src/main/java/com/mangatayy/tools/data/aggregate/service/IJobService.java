package com.mangatayy.tools.data.aggregate.service;

import com.mangatayy.tools.data.aggregate.dao.model.AggregateJob;
import com.mangatayy.tools.data.aggregate.entity.JobRequestVO;

import java.util.List;

/**
 * @author yuyong
 * @date 2022/9/7 16:06
 */
public interface IJobService {
    List<AggregateJob> queryAllOn();

    void executeNow(JobRequestVO requestVO) throws Exception;

    void executeWithChildren(JobRequestVO requestVO) throws Exception;

    void reScheduledTask(Long id);

    void reScheduledAll();
}
