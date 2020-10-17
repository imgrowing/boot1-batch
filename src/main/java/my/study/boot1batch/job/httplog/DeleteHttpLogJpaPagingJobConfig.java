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

import javax.persistence.EntityManagerFactory;

@Slf4j
@RequiredArgsConstructor
@Configuration
public class DeleteHttpLogJpaPagingJobConfig {

    private final JobBuilderFactory jobBuilderFactory;

    private final StepBuilderFactory stepBuilderFactory;

    private final EntityManagerFactory entityManagerFactory;

    private final HttpLogRepository httpLogRepository;

    private static final int CHUNK_SIZE = 1000;

    @Bean
    public Job deleteHttpLogJpaPagingJob() {
        return jobBuilderFactory.get("deleteHttpLogJpaPagingJob")
                .start(deleteHttpLogJpaPagingStep())
                .incrementer(new TimestampJobParameter())
                .build();
    }

    @Bean
    public Step deleteHttpLogJpaPagingStep() {
        return stepBuilderFactory.get("deleteHttpLogJpaPagingStep")
                .<HttpLog, HttpLog>chunk(CHUNK_SIZE)
                .reader(deleteHttpLogJpaPagingItemReader())
                .writer(deleteHttpLogItemWriter())
                .build();
    }

    @Bean
    public ItemReader<HttpLog> deleteHttpLogJpaPagingItemReader() {
        JpaPagingItemReader<HttpLog> reader = new JpaPagingItemReader<HttpLog>() {
            @Override
            public int getPage() {
                log.warn("reader.getPage()");
                return 0;
            }
        };

        reader.setName("deleteHttpLogJpaPagingItemReader");
        reader.setEntityManagerFactory(entityManagerFactory);
        reader.setPageSize(CHUNK_SIZE);
        reader.setQueryString("SELECT l FROM HttpLog l ORDER BY l.requestAt");
        return reader;
    }

    @Bean
    public ItemWriter<HttpLog> deleteHttpLogItemWriter() {
        ItemWriter<HttpLog> writer = items -> {
            log.warn("writer.write()");
            httpLogRepository.delete(items);
        };
        return writer;
    }
}
