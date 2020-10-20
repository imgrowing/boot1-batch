package my.study.boot1batch.job.httplog.skip;

import com.google.common.collect.ImmutableSet;
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
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
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
import static my.study.boot1batch.job.httplog.skip.SkipWriterJobConfig.JOB_NAME;

@Slf4j
@RequiredArgsConstructor
@Configuration
@ConditionalOnProperty(name = "job.name", havingValue = JOB_NAME)
public class SkipWriterJobConfig {


    private final JobBuilderFactory jobBuilderFactory;

    private final StepBuilderFactory stepBuilderFactory;

    private final EntityManagerFactory entityManagerFactory;

    private final HttpLogRepository httpLogRepository;

    public static final String JOB_NAME = "skipWriterJob";
    private static final int CHUNK_SIZE = 5;

    @Value("${skip.limit:10}")
    private Integer skipLimit;

    private static final Set<Long> BAD_ID_LIST = ImmutableSet.of(4L, 7L, 8L);

    /*
    FIXME: writer.write(list)에서 exception이 발생하면, rollback 한 후 item 하나만으로 chunk를 구성하여 writer.write(item) 으로 호출한다.
    FIXME: 그런데 좀 이상한 것은 단건 chunk가 끝나고 나면, 원래 chunk 크기 만큼의 write가 한 번 더 발생한다. 이 때는 별도의 writer.write(list)가 호출된 것은 아니다.
    FIXME: writer에서 발생할 수 있는 Exception에 대해서는 skip 하려는 경우, 트랜잭션 처리에 대해 면밀하게 확인하여야 할 것 같다.
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
                //.noRollback(MySkippableException.class) // skip or retry가 발생하면 해당 chunk는 무조건 rollback 되지만, noRollback(Exception)을 지정하면 rollback을 하지 않고 처리를 계속 수행한다.
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
            log.warn("processor.process() id: {}", httpLogId);
            httpLog.setCompleted(true);
            return httpLog;
        };
    }


    @Bean
    public ItemWriter<HttpLog> writer() {
        ItemWriter<HttpLog> writer = httpLogs -> {
            log.warn("writer.write(): " + httpLogs.stream().map(HttpLog::getId).collect(toList()));

            for (HttpLog httpLog : httpLogs) {
                if (BAD_ID_LIST.contains(httpLog.getId())) {
                    log.error("writer.write() id: {} ==> error", httpLog.getId());
                    throw new MySkippableException("bad id: " + httpLog.getId());
                }
            }

            httpLogRepository.save(httpLogs);
        };
        return writer;
    }
}
