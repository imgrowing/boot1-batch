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
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;

import java.util.List;

@Slf4j
@RequiredArgsConstructor
@Configuration
public class DeleteHttpLogJpaPagingTaskletJobConfig {

    private final JobBuilderFactory jobBuilderFactory;

    private final StepBuilderFactory stepBuilderFactory;

    private final HttpLogRepository httpLogRepository;

    private static final int CHUNK_SIZE = 5;

    @Bean
    public Job deleteHttpLogJpaPagingTaskletJob() {
        return jobBuilderFactory.get("deleteHttpLogJpaPagingTaskletJob")
                .start(deleteHttpLogJpaPagingTaskletStep())
                .incrementer(new TimestampJobParameter())
                .build();
    }

    @Bean
    public Step deleteHttpLogJpaPagingTaskletStep() {
        return stepBuilderFactory.get("deleteHttpLogJpaPagingTaskletStep")
                .tasklet((contribution, chunkContext) -> {

                    Page<HttpLog> logPage = httpLogRepository.findAll(new PageRequest(0, CHUNK_SIZE));

                    if (logPage.hasContent()) {
                        List<HttpLog> httpLogs = logPage.getContent();
                        httpLogRepository.delete(httpLogs);
                        return RepeatStatus.CONTINUABLE;
                    }

                    return RepeatStatus.FINISHED;
                })
                .build();
    }
}
