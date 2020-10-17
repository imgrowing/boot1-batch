package my.study.boot1batch.domain.httplog;

import org.springframework.data.jpa.repository.JpaRepository;

public interface HttpLogRepository extends JpaRepository<HttpLog, Long> {

}
