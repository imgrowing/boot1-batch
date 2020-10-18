package my.study.boot1batch.job.httplog.partition;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import my.study.boot1batch.domain.httplog.HttpLog;
import my.study.boot1batch.domain.httplog.HttpLogRepository;
import my.study.boot1batch.job.support.TimestampJobParameter;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.*;
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import javax.persistence.EntityManagerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static java.util.stream.Collectors.toList;
import static my.study.boot1batch.job.httplog.partition.PartitionDeleteWithPage0JobConfig.JOB_NAME;

@Slf4j
@RequiredArgsConstructor
@Configuration
@ConditionalOnProperty(name = "job.name", havingValue = JOB_NAME)
public class PartitionDeleteWithPage0JobConfig {

    private final JobBuilderFactory jobBuilderFactory;

    private final StepBuilderFactory stepBuilderFactory;

    private final EntityManagerFactory entityManagerFactory;

    private final HttpLogRepository httpLogRepository;

    public static final String JOB_NAME = "partitionDeleteWithPage0Job";
    private static final int CHUNK_SIZE = 5;
    private static final int GRID_SIZE = 3;

    // 스레드 수는 gridSize와 일치시켰다.
    // gridSize > threadCount 인 경우에는 일감을 더 많이 만든(잘게 쪼갠) 상황이고, 스레드가 적절히 나누어 처리하게 된다.
    private static final int THREAD_COUNT = GRID_SIZE;
    // reader가 range(minId~maxId) 단위로 query하기 때문에, writer에서 어떤 동작을 하더라도 pageSize는 동작에 영향을 주지 않는다.
    private static final int PAGE_SIZE = 10;

    @Bean
    public Job job() {
        return jobBuilderFactory.get(JOB_NAME)
                .start(managerStep())
                .incrementer(new TimestampJobParameter())
                .build();
    }

    @Bean
    public Step managerStep() {
        return stepBuilderFactory.get("managerStep")
                .partitioner("workerStep", partitioner())
                .gridSize(GRID_SIZE)
                .step(workerStep())
                .taskExecutor(taskExecutor()) // 이렇게만 해도 비동기로 동작한다. 스레드의 수는 default로 4개이고, throttleLimit()을 통해 지정할 수 있다.
                .build();
    }

    @Bean
    public Step workerStep() {
        return stepBuilderFactory.get("workerStep")
                .<HttpLog, HttpLog>chunk(CHUNK_SIZE)
                .reader(jpaPagingItemReader(null, null))
                .processor(loggingItemProcessor())
                .writer(deletingWriter())
                .build();
    }

    /*
    FIXME: JpaPagingItemReader 를 reader로 사용할 경우 반환타입에 대하여
    인터페이스인 ItemReader를 반환하면 doRead()에서 entityManager가 없어서 NPE 발생한다.
    JpaPagingItemReader는 ItemStreamReader를 구현하며, open() -> doOpen()에서 entityManager를 세팅하는 부분이 있어서 이 부분이 호출되어야 한다.
    ItemReader 인터페이스로 반환하면 인터페이스에 open()이 없어서 아예 호출되지 않게 된다.
    따라서 ItemStreamReader 혹은 이를 구현했는지 알 수 있는 구체 타입인 JpaPagingItemReader로 반환해야한다.
     */
    @Bean(destroyMethod = "") // close() 호출시 에러가 발생하는데, close() 호출을를 막아준다.
    @StepScope
    public ItemStreamReader<HttpLog> jpaPagingItemReader(
            @Value("#{stepExecutionContext['startId']}") Long startId,
            @Value("#{stepExecutionContext['endId']}") Long endId
    ) {
        JpaPagingItemReader<HttpLog> reader = new JpaPagingItemReader<HttpLog>() {
            @Override
            public int getPage() {
                // FIXME: writer에서 삭제하는 로직을 수행하므로, paging query 결과에 영향을 줄 수 있다. 따라서 page 0를 계속 반환하여 영향을 받지 않도록 한다.
                int page = 0;
                log.warn("reader.getPage(): {}, [{} ~ {}]", page, startId, endId);
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

//        reader.setSaveState(false); // multi-thread 구성으로 사용하기 위해서는 ExecutionContext에 state를 저장하지 않아야 함
        reader.setName("httpLogJpaPagingItemReader");
        reader.setEntityManagerFactory(entityManagerFactory);
        reader.setPageSize(PAGE_SIZE);
        reader.setQueryString("SELECT l FROM HttpLog l WHERE l.id BETWEEN :startId AND :endId ORDER BY l.id");
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("startId", startId);
        parameters.put("endId", endId);
        reader.setParameterValues(parameters);
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

    /*
     * Partitioner는 수행할 작업을 N개로 분할하고, 분할한 일의 단위를 알 수 있도록 각각 ExecutionContext에 정보를 담은 후 Map<String, ExecutionContext>로 반환한다.
     * 각 ExecutionContext는 worker step 마다 하나씩 전달되며, worker step에서는 stepExecutionContext 로 부터 일감의 대상 범위(정보)를 참조한다.
     * FIXME: (참고) partition의 key는 (당연히) map 내에서 중복되지 안아야 한다.
     * FIXME: (참고) partition 된 개수가 gridSize 보다 크거나 작아도 상관 없다.
     */
    @Bean
    public Partitioner partitioner() {
        return new Partitioner() {
            private static final String PARTITION_KEY = "partition";

            @Override
            public Map<String, ExecutionContext> partition(int gridSize) {
                Long minLogId = httpLogRepository.findMinIdWithNativeQuery();
                Long maxLogId = httpLogRepository.findMaxIdWithNativeQuery();
                log.warn("partitioner - min {} ~ max {}", minLogId, maxLogId);

                if (minLogId == null || maxLogId == null) {
                    return Collections.emptyMap();
                }

                Map<String, ExecutionContext> map = new HashMap<>();
                long targetSize = (maxLogId - minLogId) / gridSize + 1; // 정수 나누기는 소숫점 버림이므로 +1을 해 준다.

                int number = 0;
                long start = minLogId;
                long end = start + targetSize - 1;

                while (start <= maxLogId) {
                    ExecutionContext context = new ExecutionContext();
                    map.put(PARTITION_KEY + number, context);

                    if (end >= maxLogId) {
                        end = maxLogId;
                    }
                    
                    context.putLong("startId", start);
                    context.putLong("endId", end);

                    start += targetSize;
                    end += targetSize;
                    number++;
                }

                log.warn("partition map: {}", map);

                return map;
            }
        };
    }

    @Bean
    public TaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(THREAD_COUNT);
        executor.setMaxPoolSize(THREAD_COUNT);
        executor.setThreadNamePrefix("taskExecutor-");
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.initialize();
        return executor;
        //return new SimpleAsyncTaskExecutor();
    }
}
