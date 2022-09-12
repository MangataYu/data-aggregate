package com.mangatayy.tools.data.aggregate.controller;

import com.mangatayy.tools.data.aggregate.service.IJobService;
import io.swagger.annotations.Api;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author yuyong
 * @date 2022/8/8 15:56
 */
@RestController
@RequestMapping(value = "job")
@Api(tags = "汇聚任务接口")
@Slf4j
@RequiredArgsConstructor
public class JobController {
    private final IJobService iJobService;

  /*  @PostMapping(value = "queryAllOn")
    @ApiOperation(value = "查询所有在用任务")
    public Response<List<AggregateJob>> queryAllOn() {
        return new Response<>(iJobService.queryAllOn());
    }
    @PostMapping(value = "manualExec")
    @ApiOperation(value = "手动执行某个任务")
    public Response<String> executeOne(@RequestBody JobRequestVO jobRequestVO) {
        try {
            iJobService.executeNow(jobRequestVO);
            return Response.success("ok");
        } catch (Exception e) {
            log.error("execute error", e);
            return Response.fail(e.getMessage());
        }
    }
    @PostMapping(value = "manualExecWithChild")
    @ApiOperation(value = "手动执行某个任务包含后继任务")
    public Response<String> executeSeries(@RequestBody JobRequestVO jobRequestVO) {
        try {
            iJobService.executeWithChildren(jobRequestVO);
            return Response.success("ok");
        } catch (Exception e) {
            log.error("execute error", e);
            return Response.fail(e.getMessage());
        }
    }
    @PostMapping(value = "executeDataOsJob")
    @ApiOperation(value = "处理DataOS任务")
    public Response<String> executeDataOsJob(@RequestBody JobRequestVO jobRequestVO) {
        try {
            log.info("DATA OS schedule job_name:{},task_id:{}", jobRequestVO.getJob_name(), jobRequestVO.getTask_id());
            iJobService.executeNow(jobRequestVO);
            return Response.success("ok");
        } catch (Exception e) {
            log.error("data os execute error", e);
            return Response.fail(e.getMessage());
        }
    }

    @GetMapping(value = "reScheduledTask")
    @ApiOperation(value = "刷新汇聚任务重新调度")
    public Response<String> reScheduledTask(@RequestParam Long id) {
        try {
            iJobService.reScheduledTask(id);
            return Response.success("ok");
        } catch (Exception e) {
            log.error("reScheduledTask error", e);
            return Response.fail(e.getMessage());
        }
    }

    @GetMapping(value = "reScheduledAll")
    @ApiOperation(value = "刷新所有自动启动的汇聚任务重新调度")
    public Response<String> reScheduledAll() {
        try {
            iJobService.reScheduledAll();
            return Response.success("ok");
        } catch (Exception e) {
            log.error("reScheduledTask error", e);
            return Response.fail(e.getMessage());
        }
    }*/

}
