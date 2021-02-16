create table parser.log
(
    id         int auto_increment
        primary key,
    id_url     int                                   null,
    error_line int                                   null,
    error_type varchar(1000)                         null,
    error      varchar(1000) charset utf8mb4         null,
    date_add   timestamp default current_timestamp() not null,
    constraint log_url_id_fk
        foreign key (id_url) references parser.url (id)
)
    comment 'Лог ошибок для парсера';

create table parser.url
(
    id                 int auto_increment
        primary key,
    url                varchar(1000)                          null,
    is_parsed          tinyint(1) default 0                   null,
    number_of_attempts int        default 1                   not null,
    date_added         timestamp  default current_timestamp() not null,
    date_parsed        timestamp                              null
)
    comment 'URL "карточек" недвижимости';