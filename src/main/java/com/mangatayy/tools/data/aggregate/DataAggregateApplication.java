package com.mangatayy.tools.data.aggregate;

import com.mangatayy.tools.data.aggregate.constant.AggregateConstants;
import lombok.extern.slf4j.Slf4j;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * @author yuyong
 * @date 2022/8/8 15:26
 */
@SpringBootApplication
@Slf4j
@MapperScan(basePackages = AggregateConstants.MAPPER_PACKAGE)
@EnableScheduling
public class DataAggregateApplication {

    public static void main(String[] args) {
        SpringApplication.run(DataAggregateApplication.class, args);
        log.info("===========DataAggregateApplication started===========");
    }
}
