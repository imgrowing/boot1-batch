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
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;

import javax.persistence.EntityManagerFactory;

import static java.util.stream.Collectors.toList;
import static my.study.boot1batch.job.httplog.multithread.MultiThreadJobConfig.JOB_NAME;

@Slf4j
@RequiredArgsConstructor
@Configuration
@ConditionalOnProperty(name = "job.name", havingValue = JOB_NAME)
public class MultiThreadJobConfig {

    private final JobBuilderFactory jobBuilderFactory;

    private final StepBuilderFactory stepBuilderFactory;

    private final EntityManagerFactory entityManagerFactory;

    private final HttpLogRepository httpLogRepository;

    public static final String JOB_NAME = "multiThreadJob";
    private static final int CHUNK_SIZE = 5;
    private static final int PAGE_SIZE = 10;

    @Bean
    public Job job() {
        return jobBuilderFactory.get(JOB_NAME)
                .start(multiThreadStep())
                .incrementer(new TimestampJobParameter())
                .build();
    }

    @Bean
    public Step multiThreadStep() {
        return stepBuilderFactory.get(JOB_NAME + "Step")
                .<HttpLog, HttpLog>chunk(CHUNK_SIZE)
                .reader(jpaPagingItemReader())
                .writer(completingWriter())
                .taskExecutor(taskExecutor()) // 이렇게만 해도 비동기로 동작한다. 스레드의 수는 default로 4개이고, throttleLimit()을 통해 지정할 수 있다.
                //.throttleLimit(4)
                .build();
    }

    @Bean
    public TaskExecutor taskExecutor() {
        return new SimpleAsyncTaskExecutor();
    }

    @Bean
    public ItemReader<HttpLog> jpaPagingItemReader() {
        JpaPagingItemReader<HttpLog> reader = new JpaPagingItemReader<HttpLog>() {
            // taskExecutor 와 throttleLimit의 조합으로 사용할 경우 read()를 synchronized 처리해 주어야 함
            // 그렇지 않으면 여러 스레드가 read에 동시에 진입하여 같은 데이터를 읽어오는 상황이 발생함
            // synchronized 처리를 추가하기 위해 override 하였음. JpaPagingItemReader는 thread safe 하지 않음
            @Override
            public synchronized HttpLog read() throws Exception {
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
    public ItemWriter<HttpLog> completingWriter() {
        ItemWriter<HttpLog> writer = httpLogs -> {
            log.warn("writer.write(): " + httpLogs.stream().map(HttpLog::getId).collect(toList()));

            httpLogs.forEach(httpLog -> {
                httpLog.setCompleted(true);
                httpLogRepository.save(httpLog);
            });
        };
        return writer;
    }
}
