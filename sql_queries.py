import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events(
    artist        varchar,
    auth          varchar,
    firstname     varchar,
    gender        varchar,
    iteminsession integer,
    lastname      varchar,
    length        FLOAT,
    level         varchar,
    location      varchar,
    method        varchar,
    page          varchar,
    registration  FLOAT,
    sessionid     integer,
    song          varchar,
    status        integer,
    ts            bigint,
    useragent     varchar,
    userid        integer
);
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs(
    num_songs integer,
    artist_id varchar,
    artist_latitude varchar,
    artist_longitude varchar,
    artist_location varchar,
    artist_name varchar,
    song_id varchar,
    title varchar,
    duration FLOAT,
    year integer
);
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
    songplay_id INT IDENTITY(0,1) PRIMARY KEY sortkey, 
    start_time timestamp NOT NULL, 
    user_id int NOT NULL,
    level varchar NOT NULL, 
    song_id varchar, 
    artist_id varchar, 
    session_id int NOT NULL, 
    location varchar NOT NULL, 
    user_agent varchar NOT NULL)
diststyle all;
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
    user_id int PRIMARY KEY sortkey, 
    first_name varchar NOT NULL, 
    last_name varchar NOT NULL, 
    gender varchar NOT NULL, 
    level varchar NOT NULL)
diststyle all;
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (
    song_id varchar PRIMARY KEY sortkey, 
    title varchar NOT NULL, 
    artist_id varchar NOT NULL, 
    year int NOT NULL, 
    duration numeric NOT NULL)
diststyle all;
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
    artist_id varchar NOT NULL sortkey, 
    name varchar NOT NULL, 
    location varchar, 
    latitude varchar,
    longitude varchar)
diststyle all;
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
    start_time timestamp PRIMARY KEY sortkey, 
    hour int NOT NULL, 
    day int NOT NULL, 
    week int NOT NULL, 
    month int NOT NULL, 
    year int NOT NULL, 
    weekday int NOT NULL)
diststyle even;
""")

# STAGING TABLES

staging_events_copy = ("""copy staging_events from {} iam_role {} region 'us-west-2' json {} """).format(config.get('S3','LOG_DATA'),config.get('IAM_ROLE','ARN'), config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""copy staging_songs from {} iam_role {} region 'us-west-2' json 'auto' """).format(config.get('S3','SONG_DATA'),config.get('IAM_ROLE','ARN'))

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, 
                       artist_id, session_id, location, user_agent)
SELECT TIMESTAMP 'epoch' + se.ts/1000 *INTERVAL '1second' as start_time,
    se.userid::integer as user_id,
    se.level,
    ss.song_id,
    ss.artist_id,
    se.sessionid as session_id,
    se.location,
    se.useragent as user_agent
FROM staging_events se 
JOIN staging_songs ss
ON (se.song = ss.title) AND (se.artist = ss.artist_name) AND (se.length = ss.duration)
WHERE se.page = 'NextSong' and se.userid is not null;
""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT userid as user_id,
    firstname as first_name,
    lastname as last_name,
    gender,
    level
FROM staging_events
WHERE page = 'NextSong' and userid is not null;
""")

song_table_insert = ("""INSERT INTO songs(song_id, title, artist_id, year, duration) 
SELECT DISTINCT song_id,
    title,
    artist_id,
    year,
    duration 
FROM staging_songs;
""")

artist_table_insert = ("""INSERT INTO artists(artist_id, name, location, latitude, longitude) 
SELECT artist_id,
    artist_name as name,
    artist_location as location,
    artist_latitude as latitude,
    artist_longitude as longitude
FROM staging_songs;
""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT a.start_time,
    EXTRACT (HOUR FROM a.start_time), 
    EXTRACT (DAY FROM a.start_time),
    EXTRACT (WEEK FROM a.start_time), 
    EXTRACT (MONTH FROM a.start_time),
    EXTRACT (YEAR FROM a.start_time), 
    EXTRACT (WEEKDAY FROM a.start_time) 
FROM
    (SELECT TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time FROM staging_events WHERE page = 'NextSong') a;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
