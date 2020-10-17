package my.study.boot1batch.domain.httplog;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import javax.persistence.*;
import java.util.Date;

@Slf4j
@Getter
@Setter
@NoArgsConstructor
@ToString
@Entity
@Table(name = "http_logs")
public class HttpLog {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String uri;

    @Temporal(TemporalType.TIMESTAMP)
    private Date requestAt;

    private boolean completed;
}
