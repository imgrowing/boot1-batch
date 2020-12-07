package my.study.boot1batch.domain.user;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.persistence.*;
import java.util.Date;

@Getter
@Setter
@NoArgsConstructor
@Entity
@Table(name = "users")
public class User {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String loginId;

    private Date lastLoginAt;

    private Date updatedAt;

    @PostUpdate
    private void postUpdate() {
        this.updatedAt = new Date();
    }
}
