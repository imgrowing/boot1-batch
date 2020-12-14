package my.study.boot1batch.domain.user;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import javax.persistence.*;
import java.util.Date;

@Getter
@Setter
@NoArgsConstructor
@Entity
@Table(name = "users")
public class User {
    private static final DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy.MM.dd HH:mm:ss.SSS");

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String loginId;

    @Temporal(TemporalType.TIMESTAMP)
    private Date lastLoginAt;

    @Temporal(TemporalType.TIMESTAMP)
    private Date updatedAt;

    @PostUpdate
    private void postUpdate() {
        this.updatedAt = new Date();
    }

    @Override
    public String toString() {
        return "User{" +
                "id=" + id +
                ", loginId='" + loginId + '\'' +
                ", lastLoginAt=" + formatDate(lastLoginAt) +
                ", updatedAt=" + formatDate(updatedAt) +
                '}';
    }

    private String formatDate(Date date) {
        if (date != null) {
            return formatter.print(new DateTime(date.getTime()));
        }
        return null;
    }
}
