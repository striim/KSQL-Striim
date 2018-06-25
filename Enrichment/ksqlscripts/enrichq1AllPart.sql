-- 1. Source of ClickStream.
CREATE STREAM clickstream (_time bigint,time varchar, ip varchar, request varchar, status int, userid int, bytes bigint, agent varchar) with (kafka_topic = 'clickstream', value_format = 'json');

-- 2.users lookup table
CREATE TABLE WEB_USERS (user_id int, registered_At long, username varchar, first_name varchar, last_name varchar, city varchar, level varchar) with (key='user_id', kafka_topic = 'users', value_format = 'json');

-- 3.Clickstream enriched with user account data
CREATE STREAM customer_clickstream WITH (PARTITIONS=4) as SELECT 1 as group_key,userid, u.first_name, u.last_name, u.level, time, ip, request, status, agent FROM clickstream c LEFT JOIN web_users u ON c.userid = u.user_id;

-- 4.Create KTable for eventually taking wall clock time.
CREATE TABLE custClickStream as select * from customer_clickstream ;
