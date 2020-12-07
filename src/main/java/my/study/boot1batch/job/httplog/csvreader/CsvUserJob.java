package my.study.boot1batch.job.httplog.csvreader;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import my.study.boot1batch.domain.user.User;
import my.study.boot1batch.domain.user.UserRepository;
import my.study.boot1batch.job.support.TimestampJobParameter;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

import javax.persistence.EntityManagerFactory;
import java.util.Date;

import static my.study.boot1batch.job.httplog.csvreader.CsvUserJob.JOB_NAME;

@Slf4j
@RequiredArgsConstructor
@Configuration
@ConditionalOnProperty(name = "job.name", havingValue = JOB_NAME)
public class CsvUserJob {

    protected static final String JOB_NAME = "csvUserJob";

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;

    private final EntityManagerFactory entityManagerFactory;

    private final UserRepository userRepository;

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
                .<LoginAtDto, User>chunk(CHUNK_SIZE)
                .reader(reader())
                .processor(processor())
                .writer(writer())
                .build();
    }

    @Bean
    @StepScope
    public FlatFileItemReader<LoginAtDto> reader() {
        FlatFileItemReader<LoginAtDto> reader = new FlatFileItemReader<>();
        // TODO CSV에서 quote(") 문자로 감싸져 있는 필드도 정상적으로 읽어올 수 있음
        //reader.setResource(new ClassPathResource("users_lastLoginAt.csv"));
        reader.setResource(new ClassPathResource("users_lastLoginAt_quote.csv"));

        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setNames(new String[] { "loginId", "lastLoginAt" });

        BeanWrapperFieldSetMapper<LoginAtDto> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(LoginAtDto.class);

        DefaultLineMapper<LoginAtDto> lineMapper = new DefaultLineMapper<>();
        lineMapper.setLineTokenizer(lineTokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);

        reader.setLineMapper(lineMapper);

        return reader;
    }

    @Bean
    public ItemProcessor<LoginAtDto, User> processor() {
        return dto -> {
            Date lastLoginAt = dto.getLastLoginAtAsDate();
            if (lastLoginAt == null) {
                return null;
            }

            User user = userRepository.findByLoginId(dto.getLoginId());
            user.setLastLoginAt(lastLoginAt);
            return user;
        };
    }

    @Bean
    public ItemWriter<User> writer() {
        JpaItemWriter<User> writer = new JpaItemWriter<>();
        writer.setEntityManagerFactory(entityManagerFactory);
        /*
         TODO ItemProcessor에서 User entity의 변경사항이 발생하기 때문에,
         TODO 로그만 출력하는 itemWriter를 사용하더라도 users에 저장이 된다.
         TODO 하지만 JpaItemWriter를 사용하지 않으니, CHUNK_SIZE의 마지막 row에서 updatedAt이 현재시각으로 갱신되지 않는 현상이 발견됨
         TODO JpaItemWriter로 변경하니 updatedAt이 정상적으로 갱신됨
        ItemWriter<User> writer = new ItemWriter<User>() {
            @Override
            public void write(List<? extends User> items) throws Exception {
                for (User user : items) {
                    log.info("write user: {}", user);
                }
            }
        };
         */
        return writer;
    }
}
