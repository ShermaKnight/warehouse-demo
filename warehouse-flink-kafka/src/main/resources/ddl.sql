create table `event`
(
    `id`          bigint       not null primary key auto_increment,
    `uuid`        varchar(128) not null,
    `division_id` varchar(16)  not null,
    `project_id`  varchar(16)  not null,
    `version`     varchar(16),
    `name`        varchar(16),
    `event_id`    varchar(48),
    `event_type`  varchar(16)
);