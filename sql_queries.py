import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
LOG_DATA = config.get('S3', 'LOG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')
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

staging_events_table_create = ("""
CREATE TABLE staging_events (
    artist VARCHAR(128) NOT NULL,
    auth VARCHAR(128)
    firstName VARCHAR(128),
    lastName VARCHAR(128),
    gender VARCHAR(1)
    itemInSession INTEGER,
    length FLOAT,
    level VARCHAR(4)
    location VARCHAR(128),
    method VARCHAR(32),
    page VARCHAR(32),
    registration FLOAT,
    sessionId INTEGER,
    song VARCHAR(128),
    status INTEGER,
    ts BIGINT,
    userAgent VARCHAR(256),
    userId INT NOT NULL,
);
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs (
    song_id VARCHAR(18),
    num_songs INT,
    title VARCHAR(128),
    year INT,
    duration DOUBLE PRECISION,
    artist_id VARCHAR(18),
    artist_name VARCHAR(128),
    artist_latitude DOUBLE PRECISION,
    artist_longitide DOUBLE PRECISION,
    artist_location VARCHAR(128),
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id IDENTITY(0,1) PRIMARY KEY,
    start_time TIMESTAMP NOT NULL SORTKEY,
    user_id INT NOT NULL DISTKEY,
    level VARCHAR(4),
    song_id VARCHAR(18),
    artist_id VARCHAR(18),
    session_id INT,
    location VARCHAR(128),
    user_agent VARCHAR(256),
    FOREIGN KEY (user_id) REFERENCES users (user_id),
    FOREIGN KEY (song_id) REFERENCES songs (song_id),
    FOREIGN KEY (artist_id) REFERENCES artists (artist_id)
    );
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id INT PRIMARY KEY SORTKEY,
    first_name VARCHAR(128),
    last_name VARCHAR(128),
    level VARCHAR(4),
    gender VARCHAR(1)
    );
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR(18) PRIMARY KEY SORTKEY,
    title VARCHAR(128) NOT NULL,
    artist_id VARCHAR(18),
    year INT,
    duration DOUBLE PRECISION NOT NULL,
    FOREIGN KEY (artist_id) REFERENCES artists (artist_id)
    );
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR(18) PRIMARY KEY SORTKEY,
    name VARCHAR(128) NOT NULL,
    location VARCHAR(128),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION
    );
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS times (
    start_time TIME UNIQUE SORTKEY,
    hour INT CHECK (hour >= 0 AND hour <= 23),
    day INT CHECK (day >= 1 AND day <= 31),
    week INT CHECK (week >= 1 AND week <= 52),
    month INT CHECK (month >= 1 AND month <= 12),
    year INT,
    weekday INT CHECK (weekday >= 0 AND weekday <= 6)
    );
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events 
FROM {}
iam_role {}
region 'us-west-2'
FORMAT AS json {}
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
COPY staging_songs 
FROM {}
iam_role {}
region 'us-west-2'
FORMAT AS json 'auto'
""").format(SONG_DATA, ARN)

# FINAL TABLES
songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT songplay_id IDENTITY(0,1) PRIMARY KEY,
    TIMESTAMP 'epoch' + staging_events.ts/1000 * INTERVAL '1 second',
    staging_events.userId,
    staging_events.level,
    staging_songs.song_id,
    staging_songs.artist_id,
    staging_events.sessionId,
    staging_events.location,
    staging_events.userAgent,
FROM staging_events
INNER JOIN staging_songs 
ON staging_events.song = staging_songs.title 
AND staging_events.artist = staging_songs.artist_name
WHERE staging_events.page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO users
SELECT userId, firstName, lastName, gender, level
FROM staging_events
WHERE page = 'NextSong'
ON CONFLICT (user_id) DO UPDATE SET level = EXCLUDED.level
""")

song_table_insert = ("""
INSERT INTO songs
SELECT song_id, title, artist_id, year, duration
FROM staging_songs
ON CONFLICT (song_id) DO NOTHING
""")

artist_table_insert = ("""
INSERT INTO artists
SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM staging_songs
ON CONFLICT (artist_id) DO NOTHING
""")

time_table_insert = ("""
INSERT INTO times
SELECT 
    TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time,
    DATE_PART("hour", start_time) AS hour,
    DATE_PART("day", start_time) AS day,
    DATE_PART("week", start_time) AS week,
    DATE_PART("month", start_time) AS month,
    DATE_PART("dow", start_time) AS weekday
FROM staging_events
ON CONFLICT (start_time) DO NOTHING
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create,
                        user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop,
                      song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, artist_table_insert, song_table_insert,
                        time_table_insert]
