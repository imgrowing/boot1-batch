package my.study.boot1batch.domain.httplog;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

public interface HttpLogRepository extends JpaRepository<HttpLog, Long> {

    /* Custom RepositoryImpl로도 커버 가능하지만, Native Query로 구해 보았음 */

    @Query(value = "SELECT MAX(id) FROM http_logs", nativeQuery = true)
    Long findMaxIdWithNativeQuery();

    @Query(value = "SELECT MIN(id) FROM http_logs", nativeQuery = true)
    Long findMinIdWithNativeQuery();

}
