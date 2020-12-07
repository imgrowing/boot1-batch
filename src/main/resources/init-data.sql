##################################
# http_logs
##################################
DROP TABLE IF EXISTS http_logs;
CREATE TABLE http_logs
(
    id        BIGINT NOT NULL AUTO_INCREMENT,
    uri       VARCHAR(255),
    requestAt DATETIME(6),
    completed TINYINT,
    PRIMARY KEY (id)
) ENGINE = InnoDB;

# DATA
SELECT @INSERT_COUNT := 40;

DELIMITER $$
DROP PROCEDURE IF EXISTS loopInsert$$
CREATE PROCEDURE loopInsert()
BEGIN
    DECLARE i INT DEFAULT 1;

    WHILE i <= @INSERT_COUNT
        DO
            -- N번 반복
            INSERT INTO http_logs (id, uri, requestAt, completed) VALUES (i, concat('/hello', i), date_add('2020-01-01 00:00:00', INTERVAL 1 SECOND), 0);
            SET i = i + 1;
        END WHILE;
END$$
DELIMITER $$

CALL loopInsert;

ALTER TABLE http_logs
    ADD INDEX http_logs_idx1 (requestAt);

##################################
# users
##################################
DROP TABLE IF EXISTS users;

CREATE TABLE users
(
    id          BIGINT NOT NULL AUTO_INCREMENT,
    loginId     VARCHAR(50),
    lastLoginAt DATETIME(6),
    updatedAt   DATETIME(6),
    PRIMARY KEY (id)
) ENGINE = InnoDB;

# DATA
SELECT @INSERT_COUNT := 1000;
TRUNCATE users;

DELIMITER $$
DROP PROCEDURE IF EXISTS loopInsert$$
CREATE PROCEDURE loopInsert()
BEGIN
    DECLARE i INT DEFAULT 1;

    WHILE i <= @INSERT_COUNT
        DO
            -- N번 반복
            INSERT INTO users (id, loginId, lastLoginAt, updatedAt) VALUES (i, concat('id', i), NULL, '2020-01-01 00:00:00');
            SET i = i + 1;
        END WHILE;
END$$
DELIMITER $$

CALL loopInsert;


##################################
# pay
##################################
DROP TABLE IF EXISTS pay;

CREATE TABLE pay
(
    id            BIGINT NOT NULL AUTO_INCREMENT,
    amount        BIGINT,
    successStatus BOOLEAN,
    PRIMARY KEY (id)
) ENGINE = InnoDB;

INSERT INTO pay (amount, successStatus)
SELECT amount, FALSE
FROM pay2
LIMIT 100000;

SELECT *
FROM pay
ORDER BY id
LIMIT 1000;
UPDATE pay
SET successStatus = FALSE
WHERE id > 0;


USE study;

DROP TABLE pay2;

CREATE TABLE pay2
(
    id           BIGINT NOT NULL AUTO_INCREMENT,
    amount       BIGINT,
    tx_name      VARCHAR(255),
    tx_date_time DATETIME,
    PRIMARY KEY (id)
) ENGINE = InnoDB;

INSERT INTO pay2 (amount, tx_name, tx_date_time)
VALUES (1000, 'trade1', '2018-09-10 00:00:00');
INSERT INTO pay2 (amount, tx_name, tx_date_time)
VALUES (2000, 'trade2', '2018-09-10 00:00:00');
INSERT INTO pay2 (amount, tx_name, tx_date_time)
VALUES (3000, 'trade3', '2018-09-10 00:00:00');
INSERT INTO pay2 (amount, tx_name, tx_date_time)
VALUES (4000, 'trade4', '2018-09-10 00:00:00');

-- N번 실행
INSERT INTO pay2 (amount, tx_name, tx_date_time)
SELECT amount, tx_name, tx_date_time
FROM pay2
LIMIT 10000000
;
