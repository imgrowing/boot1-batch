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
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.persistence.EntityManagerFactory;

import static java.util.stream.Collectors.toList;
import static my.study.boot1batch.job.httplog.DeleteHttpLogJpaPagingJobConfig.JOB_NAME;

@Slf4j
@RequiredArgsConstructor
@Configuration
@ConditionalOnProperty(name = "job.name", havingValue = JOB_NAME)
public class DeleteHttpLogJpaPagingJobConfig {

    private final JobBuilderFactory jobBuilderFactory;

    private final StepBuilderFactory stepBuilderFactory;

    private final EntityManagerFactory entityManagerFactory;

    private final HttpLogRepository httpLogRepository;

    public static final String JOB_NAME = "deleteHttpLogJpaPagingJob";
    private static final int CHUNK_SIZE = 5;
    private static final int PAGE_SIZE = 10;

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
                .writer(deletingWriter())
                .build();
    }

    @Bean
    public ItemReader<HttpLog> jpaPagingItemReader() {
        JpaPagingItemReader<HttpLog> reader = new JpaPagingItemReader<HttpLog>() {
            @Override
            public int getPage() {
                log.warn("reader.getPage()");
                return 0;
            }
        };

        reader.setName("deleteHttpLogJpaPagingItemReader");
        reader.setEntityManagerFactory(entityManagerFactory);
        reader.setPageSize(PAGE_SIZE);
        reader.setQueryString("SELECT l FROM HttpLog l ORDER BY l.requestAt");
        return reader;
    }

    @Bean
    public ItemWriter<HttpLog> deletingWriter() {
        ItemWriter<HttpLog> writer = httpLogs -> {
            log.warn("writer.write(): " + httpLogs.stream().map(HttpLog::getId).collect(toList()));
            httpLogRepository.delete(httpLogs);
        };
        return writer;
    }
}
