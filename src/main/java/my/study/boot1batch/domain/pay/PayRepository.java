package my.study.boot1batch.domain.pay;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface PayRepository extends JpaRepository<Pay, Long> {
    List<Pay> findAllBySuccessStatusIsTrue();
}
