drop table http_logs;
create table http_logs (
                           id          bigint not null auto_increment,
                           uri         varchar(255),
                           requestAt   datetime(6),
                           completed   tinyint,
                           primary key (id)
) engine = InnoDB;


select @INSERT_COUNT := 40;

DELIMITER $$
DROP PROCEDURE IF EXISTS loopInsert$$
CREATE PROCEDURE loopInsert()
BEGIN
    DECLARE i INT DEFAULT 1;

    WHILE i <= @INSERT_COUNT DO -- N번 반복
    insert into http_logs (id, uri, requestAt, completed) VALUES (i, concat('/hello', i), date_add('2020-01-01 00:00:00', interval 1 second), 0);
    SET i = i + 1;
        END WHILE;
END$$
DELIMITER $$

CALL loopInsert;

alter table http_logs ADD INDEX http_logs_idx1 (requestAt);



# use study;

drop table pay;

create table pay (
                     id         bigint not null auto_increment,
                     amount     bigint,
                     successStatus     boolean,
                     primary key (id)
) engine = InnoDB;

insert into pay (amount, successStatus)
select amount, false from pay2 limit 100000;

select * from pay  order by id limit 1000;
update pay set successStatus = false where id > 0;


use study;

drop table pay2;

create table pay2 (
                      id         bigint not null auto_increment,
                      amount     bigint,
                      tx_name     varchar(255),
                      tx_date_time datetime,
                      primary key (id)
) engine = InnoDB;

insert into pay2 (amount, tx_name, tx_date_time) VALUES (1000, 'trade1', '2018-09-10 00:00:00');
insert into pay2 (amount, tx_name, tx_date_time) VALUES (2000, 'trade2', '2018-09-10 00:00:00');
insert into pay2 (amount, tx_name, tx_date_time) VALUES (3000, 'trade3', '2018-09-10 00:00:00');
insert into pay2 (amount, tx_name, tx_date_time) VALUES (4000, 'trade4', '2018-09-10 00:00:00');

-- N번 실행
insert into pay2 (amount, tx_name, tx_date_time)
select amount, tx_name, tx_date_time from pay2 limit 10000000
;


use study;

drop table http_logs;

create table http_logs (
                           id          bigint not null auto_increment,
                           uri         varchar(255),
                           requestAt   datetime(6),
                           completed   tinyint,
                           primary key (id)
) engine = InnoDB;
