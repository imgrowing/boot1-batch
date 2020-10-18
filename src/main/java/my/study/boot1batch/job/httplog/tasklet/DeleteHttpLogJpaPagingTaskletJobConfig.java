package my.study.boot1batch.job.httplog.tasklet;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import my.study.boot1batch.domain.httplog.HttpLog;
import my.study.boot1batch.domain.httplog.HttpLogRepository;
import my.study.boot1batch.job.support.TimestampJobParameter;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;

import java.util.List;

import static my.study.boot1batch.job.httplog.tasklet.DeleteHttpLogJpaPagingTaskletJobConfig.JOB_NAME;

@Slf4j
@RequiredArgsConstructor
@Configuration
@ConditionalOnProperty(name = "job.name", havingValue = JOB_NAME)
public class DeleteHttpLogJpaPagingTaskletJobConfig {

    private final JobBuilderFactory jobBuilderFactory;

    private final StepBuilderFactory stepBuilderFactory;

    private final HttpLogRepository httpLogRepository;

    public static final String JOB_NAME = "deleteHttpLogJpaPagingTaskletJob";
    private static final int CHUNK_SIZE = 5;

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
