import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS times"

# CREATE TABLES

staging_events_table_create= ("""
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
    start_time TIMESTAMP NOT NULL,
    user_id INT NOT NULL,
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
    user_id INT PRIMARY KEY,
    first_name VARCHAR(128),
    last_name VARCHAR(128),
    level VARCHAR(4),
    gender VARCHAR(1)
    );
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR(18) PRIMARY KEY,
    title VARCHAR(128) NOT NULL,
    artist_id VARCHAR(18),
    year INT,
    duration DOUBLE PRECISION NOT NULL,
    FOREIGN KEY (artist_id) REFERENCES artists (artist_id)
    );
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR(18) PRIMARY KEY,
    name VARCHAR(128) NOT NULL,
    location VARCHAR(128),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION
    );
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS times (
    start_time TIME UNIQUE,
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
""").format(config['LOG_DATA'], config['ARN'], config['LOG_JSONPATH'])

staging_songs_copy = ("""
COPY staging_songs 
FROM {}
iam_role {}
region 'us-west-2'
FORMAT AS json 'auto'
""").format(config.SONG_DATA, config.ARN)

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
