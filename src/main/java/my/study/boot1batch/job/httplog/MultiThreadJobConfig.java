package my.study.boot1batch.job.httplog;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import my.study.boot1batch.domain.httplog.HttpLog;
import my.study.boot1batch.domain.httplog.HttpLogRepository;
import my.study.boot1batch.job.TimestampJobParameter;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;

import javax.persistence.EntityManagerFactory;

import static java.util.stream.Collectors.toList;

@Slf4j
@RequiredArgsConstructor
@Configuration
public class MultiThreadJobConfig {

    private final JobBuilderFactory jobBuilderFactory;

    private final StepBuilderFactory stepBuilderFactory;

    private final EntityManagerFactory entityManagerFactory;

    private final HttpLogRepository httpLogRepository;

    private static final int CHUNK_SIZE = 5;

    @Bean
    public Job multiThreadJob() {
        return jobBuilderFactory.get("multiThreadJob")
                .start(multiThreadStep())
                .incrementer(new TimestampJobParameter())
                .build();
    }

    @Bean
    public Step multiThreadStep() {
        return stepBuilderFactory.get("multiThreadStep")
                .<HttpLog, HttpLog>chunk(CHUNK_SIZE)
                .reader(httpLogJpaPagingItemReader())
                .writer(httpLogItemWriter())
                .taskExecutor(taskExecutor()) // 이렇게만 해도 비동기로 동작한다. 스레드의 수는 default로 4개이고, throttleLimit()을 통해 지정할 수 있다.
                //.throttleLimit(4)
                .build();
    }

    @Bean
    public TaskExecutor taskExecutor() {
        return new SimpleAsyncTaskExecutor();
    }

    @Bean
    public ItemReader<HttpLog> httpLogJpaPagingItemReader() {
        JpaPagingItemReader<HttpLog> reader = new JpaPagingItemReader<HttpLog>() {
            // synchronized 처리를 추가하기 위해 override 하였음. JpaPagingItemReader는 thread safe 하지 않음
            // synchronized 를 지정하지 않으면, reader에서 여러 스레드가 동시에 같은 item을 꺼내가는 일이 발생함
            @Override
            public synchronized HttpLog read() throws Exception {
                return super.read();
            }
        };

        reader.setSaveState(false); // multi-thread 구성으로 사용하기 위해서는 ExecutionContext에 state를 저장하지 않아야 함
        reader.setName("httpLogJpaPagingItemReader");
        reader.setEntityManagerFactory(entityManagerFactory);
        reader.setPageSize(10);
        reader.setQueryString("SELECT l FROM HttpLog l ORDER BY l.id");
        return reader;
    }

    @Bean
    public ItemWriter<HttpLog> httpLogItemWriter() {
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
