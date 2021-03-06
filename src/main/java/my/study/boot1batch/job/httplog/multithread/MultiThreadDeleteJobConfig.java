package my.study.boot1batch.job.httplog.multithread;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import my.study.boot1batch.domain.httplog.HttpLog;
import my.study.boot1batch.domain.httplog.HttpLogRepository;
import my.study.boot1batch.job.support.TimestampJobParameter;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import javax.persistence.EntityManagerFactory;

import static java.util.stream.Collectors.toList;
import static my.study.boot1batch.job.httplog.multithread.MultiThreadDeleteJobConfig.JOB_NAME;

@Slf4j
@RequiredArgsConstructor
@Configuration
@ConditionalOnProperty(name = "job.name", havingValue = JOB_NAME)
public class MultiThreadDeleteJobConfig {

    private final JobBuilderFactory jobBuilderFactory;

    private final StepBuilderFactory stepBuilderFactory;

    private final EntityManagerFactory entityManagerFactory;

    private final HttpLogRepository httpLogRepository;

    public static final String JOB_NAME = "multiThreadDeleteJob";

    private static final int CHUNK_SIZE = 5;
    private static final int THREAD_COUNT = 4;
    // 스레드 N개에서 각각 chunk를 채우기 위한 row의 수는 THREAD_COUNT x CHUNK_SIZE 이다.
    // 따라서 chunk를 채우지 못한 상태에서 읽어온 page의 row가 소진되면 next page를 조회하는 쿼리가 실행된다.
    // page query가 writer의 결과에 영향을 받는 경우에는(update, delete가 select의 where절에 영향을 주는 경우)
    // 반드시 PAGE_SIZE == THREAD_COUNT x CHUNK_SIZE 로 일치시켜야 한다.
    private static final int PAGE_SIZE = CHUNK_SIZE * THREAD_COUNT;

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
                .processor(loggingItemProcessor())
                .writer(deletingWriter())
                .taskExecutor(taskExecutor()) // 이렇게만 해도 비동기로 동작한다. 스레드의 수는 default로 4개이고, throttleLimit()을 통해 지정할 수 있다.
                .throttleLimit(THREAD_COUNT)
                .build();
    }

    @Bean
    public TaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(THREAD_COUNT);
        executor.setMaxPoolSize(THREAD_COUNT);
        executor.setThreadNamePrefix("taskExecutor-");
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.initialize();
//        return executor;
        return new SimpleAsyncTaskExecutor();
    }

    /*
    FIXME: 정리) 삭제하는 row가 reader의 쿼리 대상인 경우에는 멀티스레드 방식을 사용할 수 없다.
     */

    @Bean
    public ItemReader<HttpLog> jpaPagingItemReader() {
        JpaPagingItemReader<HttpLog> reader = new JpaPagingItemReader<HttpLog>() {
            @Override
            public int getPage() {
                int page = super.getPage();
                log.warn("reader.getPage(): {}", page);
                return page;
            }

            // JpaPagingItemReader는 thread safe
            @Override
            public HttpLog read() throws Exception {
                HttpLog item = super.read();
                log.info("reader.read() id: {}", item != null ? item.getId() : null);
                return item;
            }
        };

        reader.setSaveState(false); // multi-thread 구성으로 사용하기 위해서는 ExecutionContext에 state를 저장하지 않아야 함
        reader.setName("httpLogJpaPagingItemReader");
        reader.setEntityManagerFactory(entityManagerFactory);
        reader.setPageSize(PAGE_SIZE);
        reader.setQueryString("SELECT l FROM HttpLog l ORDER BY l.id");
        return reader;
    }

    @Bean
    public ItemProcessor<HttpLog, HttpLog> loggingItemProcessor() {
        return httpLog -> {
            log.info("processor.process() id: {}", httpLog.getId());
            return httpLog;
        };
    }

    @Bean
    public ItemWriter<HttpLog> deletingWriter() {
        ItemWriter<HttpLog> writer = httpLogs -> {
            log.warn("writer.write() -> delete: " + httpLogs.stream().map(HttpLog::getId).collect(toList()));

            httpLogRepository.delete(httpLogs);
        };
        return writer;
    }
}
