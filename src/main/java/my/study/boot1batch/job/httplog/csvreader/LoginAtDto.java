package my.study.boot1batch.job.httplog.csvreader;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.Date;

@Getter
@Setter
@NoArgsConstructor
@ToString
public class LoginAtDto {
    private static final DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS");


    private String loginId;

    private String lastLoginAt;

    public Date getLastLoginAtAsDate() {
        if (StringUtils.isNotBlank(lastLoginAt)) {
            return formatter.parseDateTime(lastLoginAt).toDate();
        }
        return null;
    }
}
