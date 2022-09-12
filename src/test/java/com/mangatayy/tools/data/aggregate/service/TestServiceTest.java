package com.mangatayy.tools.data.aggregate.service;


import com.mangatayy.tools.data.aggregate.dao.mapper.AggregateCounterMapper;
import com.mangatayy.tools.data.aggregate.dao.model.AggregateCounter;
import com.mangatayy.tools.data.aggregate.dao.model.AggregateJob;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

/**
 * @author yuyong
 * @date 2022/8/9 11:23
 */
@SpringBootTest
@Slf4j
class TestServiceTest {
    @Autowired
    private AggregateCounterMapper mapper;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private AggregateJobCounterService service;

    @Test
    public void test() {
        List<AggregateCounter> list = mapper.selectList(Wrappers.query());
        log.info("size {}", list.size());
    }

    @Test
    public void testQuery() throws Exception {
        Map<Long, AggregateJob> map = service.queryAutoJobTree();
        ObjectMapper mapper = new ObjectMapper();
        log.info("{}", mapper.writeValueAsString(map));
        //LocalDateTime time = LocalDateTime.of(2022, 8, 7, 0, 0);
        //map.values().forEach(job -> new AggregateTask(jdbcTemplate, service, job, time, job.getJob_name()).run());

    }

    @Test
    public void testCommonTask() {
        AggregateJob job = service.queryOne(2L);
        LocalDateTime startTime = LocalDateTime.now();
        new AggregateTask(jdbcTemplate, service, job, startTime).run();
    }


    public static void main(String[] args) {
        LocalDateTime now = LocalDateTime.now().minusDays(2);
        LocalDateTime time = LocalDateTime.of(2022, 8, 28, 0, 0).minusMinutes(8640);
        int num = now.getDayOfWeek().getValue();
        String weekday = DateTimeFormatter.ofPattern("eeHH").format(time);
        log.info("time {} week: {} num:{}",time, weekday, num);
    }

    @Test
    public void test2() {
        LocalDateTime time = LocalDateTime.of(2022, 8, 28, 23, 45);
        Map<Long, AggregateJob> map = service.queryAutoJobTree();
        map.values().forEach(job -> {
            job.setRemoveNullEnci(true);
            if ("pm.to_pm_wx_lte_enb_eutrancell_main_15mi".equals(job.getSource_table())) {
                ObjectMapper mapper = new ObjectMapper();
                try {
                    log.info("job json: {}", mapper.writeValueAsString(job));
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
                new AggregateTask(jdbcTemplate, service, job, time).run();
            }
            //new AggregateTask(jdbcTemplate, service, job, time,job.getJob_name()).run();
        });
    }
    @Test
    public void test3() {
        LocalDateTime time = LocalDateTime.of(2022, 9, 1, 20, 45);
        AggregateJob job = service.queryOne(1L);
        job.setRemoveNullEnci(true);
        new AggregateTask(jdbcTemplate, service, job, time).run();
    }
}