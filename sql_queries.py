import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
LOG_DATA = config.get('S3', 'LOG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')
REGION = config.get('S3', 'REGION')
ARN = config.get('IAM_ROLE', 'ARN')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS times"

# CREATE TABLES
# Note: the ordering of the columns must match the data from s3
staging_events_table_create = ("""
CREATE TABLE staging_events (
    artist          VARCHAR,
    auth            VARCHAR,
    firstName       VARCHAR,
    gender          VARCHAR,
    itemInSession   INTEGER,
    lastName        VARCHAR,
    length          FLOAT,
    level           VARCHAR,
    location        VARCHAR,
    method          VARCHAR,
    page            VARCHAR,
    registration    VARCHAR,
    sessionId       INTEGER,
    song            VARCHAR,
    status          INTEGER,
    ts              BIGINT,
    userAgent       VARCHAR,
    userId          INTEGER
);
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs (
    song_id         VARCHAR,
    num_songs       INTEGER,
    title           VARCHAR,
    artist_name     VARCHAR,
    artist_latitude DOUBLE PRECISION,
    year            INTEGER,
    duration        DOUBLE PRECISION,
    artist_id       VARCHAR,
    artist_longitude DOUBLE PRECISION,
    artist_location VARCHAR
);
""")

songplay_table_create = ("""
CREATE TABLE songplays (
    songplay_id INT IDENTITY(0,1) PRIMARY KEY,
    start_time TIMESTAMP NOT NULL SORTKEY,
    user_id INT NOT NULL DISTKEY,
    level VARCHAR,
    song_id VARCHAR,
    artist_id VARCHAR,
    session_id INT,
    location VARCHAR,
    user_agent VARCHAR
    );
""")

user_table_create = ("""
CREATE TABLE users (
    user_id INT PRIMARY KEY SORTKEY,
    first_name VARCHAR,
    last_name VARCHAR,
    gender VARCHAR(1),
    level VARCHAR
    );
""")

song_table_create = ("""
CREATE TABLE songs (
    song_id VARCHAR PRIMARY KEY SORTKEY,
    title VARCHAR NOT NULL,
    artist_id VARCHAR,
    year INT,
    duration DOUBLE PRECISION NOT NULL
    );
""")

artist_table_create = ("""
CREATE TABLE artists (
    artist_id VARCHAR PRIMARY KEY SORTKEY,
    name VARCHAR NOT NULL,
    location VARCHAR,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION
    );
""")

time_table_create = ("""
CREATE TABLE times (
    start_time TIME SORTKEY,
    hour INT,
    day INT,
    week INT,
    month INT,
    year INT,
    weekday INT
    );
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events 
FROM {}
iam_role {}
region {}
FORMAT AS json {}
""").format(LOG_DATA, ARN, REGION, LOG_JSONPATH)

staging_songs_copy = ("""
COPY staging_songs 
FROM {}
iam_role {}
region {}
FORMAT AS json 'auto'
""").format(SONG_DATA, ARN, REGION)

# FINAL TABLES
songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT
    TIMESTAMP 'epoch' + staging_events.ts/1000 * INTERVAL '1 second',
    staging_events.userId,
    staging_events.level,
    staging_songs.song_id,
    staging_songs.artist_id,
    staging_events.sessionId,
    staging_events.location,
    staging_events.userAgent
FROM staging_events
INNER JOIN staging_songs 
ON staging_events.song = staging_songs.title 
AND staging_events.artist = staging_songs.artist_name
WHERE staging_events.page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO users
SELECT DISTINCT 
    userId, firstName, lastName, gender, level
FROM staging_events
WHERE page = 'NextSong'
AND userId IS NOT NULL
""")

song_table_insert = ("""
INSERT INTO songs
SELECT DISTINCT 
    song_id, title, artist_id, year, duration
FROM staging_songs
""")

artist_table_insert = ("""
INSERT INTO artists
SELECT DISTINCT 
    artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM staging_songs
""")

time_table_insert = ("""
INSERT INTO times
SELECT DISTINCT 
    TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time,
    DATE_PART("hour", start_time) AS hour,
    DATE_PART("day", start_time) AS day,
    DATE_PART("week", start_time) AS week,
    DATE_PART("month", start_time) AS month,
    DATE_PART("year", start_time) AS year,
    DATE_PART("dow", start_time) AS weekday
FROM staging_events
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create,
                        user_table_create, artist_table_create, song_table_create, songplay_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop,
                      song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, artist_table_insert, song_table_insert,
                        time_table_insert]
