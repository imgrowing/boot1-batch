package my.study.boot1batch.job.httplog.skip;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import my.study.boot1batch.domain.httplog.HttpLog;
import my.study.boot1batch.domain.httplog.HttpLogRepository;
import my.study.boot1batch.exception.MySkippableException;
import my.study.boot1batch.job.support.TimestampJobParameter;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.*;
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;

import javax.persistence.EntityManagerFactory;

import java.util.Set;

import static java.util.stream.Collectors.toList;
import static my.study.boot1batch.job.httplog.skip.SkipJobConfig.JOB_NAME;

@Slf4j
@RequiredArgsConstructor
@Configuration
@ConditionalOnProperty(name = "job.name", havingValue = JOB_NAME)
public class SkipJobConfig {


    private final JobBuilderFactory jobBuilderFactory;

    private final StepBuilderFactory stepBuilderFactory;

    private final EntityManagerFactory entityManagerFactory;

    private final HttpLogRepository httpLogRepository;

    public static final String JOB_NAME = "skipJob";
    private static final int CHUNK_SIZE = 5;

    @Value("${skip.limit:10}")
    private Integer skipLimit;

    private static final Set<Long> BAD_ID_LIST = ImmutableSet.of(4L, 7L, 8L);

    /*
    FIXME: (0) chunk 를 준비하고, transaction도 시작한다
    FIXME: (1) chunk 도중에 특정 item에서 skippable exception이 발생하면,
    FIXME: (2) 해당 chunk는 rollback 되고, 해당 chunk가 다시 시작된다.
    FIXME: (3) 다시 시작된 chunk에서는 해당 item 은 skip 되어 처리되지는 않으나 exception을 출력하며, 이 때 skip limit에 도달했는지 검사한다.
    FIXME: (4) skip limit에 도달했으면 step과 job이 fail 처리된다.
    FIXME: (0) 으로 다시 순환
    정리하자면, skip 가능한 exception이 발생한 경우, 해당 chunk를 rollback 하고, 다시 chunk를 시작한다.
    하지만 exception이 발생한 item은 재시작한 chunk에서는 포함되지 않은 상태로 진행된다.
    재시작한 chunk가 exception이 발생한 item의 위치에 도달했을 때 (처리는 하지 않지만) skip 누적 카운트를 계산하여 skip limit와 비교한다.
    이 때 skip limit을 초과하면 step, job 이 fail 된다.
    FIXME skip or retry가 발생하면 해당 chunk는 무조건 rollback 된다. 하지만 noRollback(Exception)을 지정하면 rollback을 하지 않고 처리를 계속 수행한다.
    FIXME 따라서 noRollback(Exception)을 지정하면 위의 (2) 번에서 (0)으로 돌아가지 않고 계속 진행하게 된다.

    10개의 데이터를 준비, chunkSize는 5
    chunk 1에서 1개의 exception 발생: id가 4
    chunk 2에서 2개의 exception 발생: id가 7, 8

    skipLimit 값에 따라 어떻게 동작하는지 확인 & 정리
    o skipLimit 5인 경우: 1, 2, 3, 5, 6, 9, 10 처리됨. 4, 7, 8 처리되지 않음
      - Preparing chunk execution for StepContext
      - transactionImpl.begin
      - page 0 쿼리 실행 - id: 1, 2, 3, 4, 5
      - reader.read(): 1
      - reader.read(): 2
      - reader.read(): 3
      - reader.read(): 4
      - reader.read(): 5
      - processor.process: 1
      - processor.process: 2
      - processor.process: 3
      - processor.process: 4 ==> Exception
      - Initiating transaction rollback on application exception
      - rolling back
        -> chunk rollback 완료
      - Preparing chunk execution for StepContext
        -> chunk 다시 시작됨
      - transactionImpl.begin
        -> transaction  다시 시작됨
        -> reader.read()는 발생하지 않음
      - processor.process: 1
      - processor.process: 2
      - processor.process: 3
      - FaultTolerantChunkProcessor  : Skipping after failed process
        -> id4 exception 이 출력되기 전에 표시됨. 미리 마음의 준비?
      - id 4에 대해 Exception 발생 로그
        process()에서 로그가 남지 않는 것으로 봐서, Spring Batch가 내부적으로 exception 정보를 보관하고 있었을 지도
      - processor.process() id: 5
      - FaultTolerantChunkProcessor  : Attempting to write: [items=[HttpLog(id=1, uri=/hello1, requestAt=2020-01-01 00:00:01.0, completed=true), HttpLog(id=2, uri=/hello2, requestAt=2020-01-01 00:00:01.0, completed=true), HttpLog(id=3, uri=/hello3, requestAt=2020-01-01 00:00:01.0, completed=true), HttpLog(id=5, uri=/hello5, requestAt=2020-01-01 00:00:01.0, completed=true)], skips=[[exception=my.study.boot1batch.exception.MySkippableException: bad id: 4, item=HttpLog(id=4, uri=/hello4, requestAt=2020-01-01 00:00:01.0, completed=false)]]]
        -> items 배열에는 1, 2, 3, 5가 있고, skips 배열에는 4와 exception 정보가 포함됨
      - writer.write(): [1, 2, 3, 5]
        -> 4는 write 대상에서 빠짐
      - TaskletStep  : Applying contribution: [StepContribution: read=0, written=4, filtered=0, readSkips=0, writeSkips=0, processSkips=1, exitStatus=EXECUTING]
        -> processSkips 카운트 증가됨

      - page 1 조회: 6, 7, 8, 9, 10
      - reader.read(): 6
      - reader.read(): 7
      - reader.read(): 8
      - reader.read(): 9
      - reader.read(): 10
      - processor.process() id: 6
      - processor.process() id: 7 ==> error
      - rollback 실행됨
      - chunk 다시 시작
      - transaction begin
      - processor.process(): 6
      - FaultTolerantChunkProcessor  : Skipping after failed process
      - 7 exception 로그
      - processor.process() id: 8 ==> error
      - rollback 실행됨
      - chunk 다시 시작
      - transaction begin
      - processor.process(): 6
      - 7은 두 번째 여서 그런지 exception 로그도 출력되지 않음
      - FaultTolerantChunkProcessor  : Skipping after failed process
      - 8 exception 로그
      - processor.process(): 9
      - processor.process(): 10
      - FaultTolerantChunkProcessor  : Attempting to write: [items=[HttpLog(id=6, uri=/hello6, requestAt=2020-01-01 00:00:01.0, completed=true), HttpLog(id=9, uri=/hello9, requestAt=2020-01-01 00:00:01.0, completed=true), HttpLog(id=10, uri=/hello10, requestAt=2020-01-01 00:00:01.0, completed=true)], skips=[[exception=my.study.boot1batch.exception.MySkippableException: bad id: 7, item=HttpLog(id=7, uri=/hello7, requestAt=2020-01-01 00:00:01.0, completed=false)], [exception=my.study.boot1batch.exception.MySkippableException: bad id: 8, item=HttpLog(id=8, uri=/hello8, requestAt=2020-01-01 00:00:01.0, completed=false)]]]
      - writer.write(): [6, 9, 10]
      - TaskletStep  : Saving step execution before commit: StepExecution: id=183, version=2, name=skipJobStep, status=STARTED, exitStatus=EXECUTING, readCount=10, filterCount=0, writeCount=7 readSkipCount=0, writeSkipCount=0, processSkipCount=3, commitCount=2, rollbackCount=3, exitDescription=
        -> commit count 3 으로 기록됨 (총 3회 rollback이 발생했었음)
      - AbstractJob           : Upgrading JobExecution status: StepExecution: id=183, version=5, name=skipJobStep, status=COMPLETED, exitStatus=COMPLETED, readCount=10, filterCount=0, writeCount=7 readSkipCount=0, writeSkipCount=0, processSkipCount=3, commitCount=3, rollbackCount=3, exitDescription=

    o skipLimit 2인 경우: 1, 2, 3, 5 처리됨. 4, 6~ 처리되지 않음
      - 첫 번째 chunk는 위와 동일
      - 두 번째 chunk에서 8의 exception만 출력하는 부분에서 skip limit이 초과했다는 메시지와 함께 step, job 이 실패함
      - 이 케이스에서는 rollback이 4에서 한 번, 6에서 한 번, 8에서 한 번 발생하였다.
     */

    @Bean
    public Job job() {
        return jobBuilderFactory.get(JOB_NAME)
                .start(step())
                .incrementer(new TimestampJobParameter())
                .build();
    }

    @Bean
    public Step step() {
        return stepBuilderFactory.get(JOB_NAME + "Step")
                .<HttpLog, HttpLog>chunk(CHUNK_SIZE)
                .reader(jpaPagingItemReader())
                .processor(completingProcessor())
                .writer(writer())
                .faultTolerant()
                .skipLimit(skipLimit)
                .skip(MySkippableException.class)
                .noRollback(MySkippableException.class) // skip or retry가 발생하면 해당 chunk는 무조건 rollback 되지만, noRollback(Exception)을 지정하면 rollback을 하지 않고 처리를 계속 수행한다.
                .build();
    }

    @Bean
    public TaskExecutor taskExecutor() {
        return new SimpleAsyncTaskExecutor();
    }

    @Bean
    public ItemReader<HttpLog> jpaPagingItemReader() {
        JpaPagingItemReader<HttpLog> reader = new JpaPagingItemReader<HttpLog>() {
            @Override
            public int getPage() {
                int page = super.getPage();
                log.info("getPage() : {}", page);
                return page;
            }

            @Override
            public HttpLog read() throws Exception {
                HttpLog httpLog = super.read();
                log.info("read() id: {}", (httpLog != null) ? httpLog.getId() : null);
                return httpLog;
            }
        };
        reader.setName("jpaPagingItemReader");
        reader.setEntityManagerFactory(entityManagerFactory);
        reader.setPageSize(CHUNK_SIZE);
        reader.setQueryString("SELECT l FROM HttpLog l ORDER BY l.id");
        return reader;
    }

    @Bean
    public ItemProcessor<HttpLog, HttpLog> completingProcessor() {
        return httpLog -> {
            Long httpLogId = httpLog.getId();

            if (BAD_ID_LIST.contains(httpLogId)) {
                log.error("processor.process() id: {} ==> error", httpLogId);
                throw new MySkippableException("bad id: " + httpLogId);
            }

            log.warn("processor.process() id: {}", httpLogId);
            httpLog.setCompleted(true);
            return httpLog;
        };
    }


    @Bean
    public ItemWriter<HttpLog> writer() {
        ItemWriter<HttpLog> writer = httpLogs -> {
            log.warn("writer.write(): " + httpLogs.stream().map(HttpLog::getId).collect(toList()));
            httpLogRepository.save(httpLogs);
        };
        return writer;
    }
}
