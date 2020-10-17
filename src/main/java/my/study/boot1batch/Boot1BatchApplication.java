package my.study.boot1batch;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@EnableBatchProcessing
@SpringBootApplication
public class Boot1BatchApplication {

    public static void main(String[] args) {
        System.exit(
                SpringApplication.exit( // Spring Boot를 종료시켜 주고 exit code를 반환한다.
                        SpringApplication.run(Boot1BatchApplication.class, args)
                )
        );
    }

}
