import configparser


# Read configuration file. 
config = configparser.ConfigParser()
config.read('dwh.cfg')

# Get IAM Role data from config.
iam_role = config.get("IAM_ROLE", "ARN")

# Get path to the data files within the S3 Buckets.
logdata_source = config.get("S3", "LOG_DATA")
songdata_source = config.get("S3", "SONG_DATA")

# Get JSONPATH file for logdata file
log_jsonpath = config.get("S3", "LOG_JSONPATH")


# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"


# CREATE TABLES
staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist        VARCHAR,
        auth          VARCHAR,
        firstName     VARCHAR,
        gender        VARCHAR(1),
        itemInSession SMALLINT,
        lastName      VARCHAR,
        length        NUMERIC,
        level         VARCHAR(4),
        location      VARCHAR,
        method        VARCHAR(6),
        page          VARCHAR(20),
        registration  NUMERIC,
        sessionId     SMALLINT,
        song          VARCHAR,
        status        SMALLINT,
        ts            TIMESTAMP,
        userAgent     VARCHAR,
        userId        INT
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs         SMALLINT,
        artist_id         VARCHAR,
        artist_latitude   NUMERIC(7,5),
        artist_longitude  NUMERIC(8,5),
        artist_location   VARCHAR,
        artist_name       VARCHAR,
        song_id           VARCHAR,
        title             VARCHAR,
        duration          NUMERIC,
        year              INT
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id  BIGINT     IDENTITY(0,1) NOT NULL,
        start_time   TIMESTAMP  NOT NULL,
        user_id      INT        NOT NULL,
        level        CHAR(4)    NOT NULL,
        song_id      VARCHAR    NOT NULL,
        artist_id    VARCHAR,
        session_id   INT,
        location     VARCHAR    NOT NULL,
        user_agent   VARCHAR,
        PRIMARY KEY (songplay_id),
        FOREIGN KEY (user_id)    REFERENCES users(user_id),
        FOREIGN KEY (song_id)    REFERENCES songs(song_id),
        FOREIGN KEY (artist_id)  REFERENCES artists(artist_id),
        FOREIGN KEY (start_time) REFERENCES time(start_time)
    )
    distkey(song_id)
    sortkey(start_time);
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id     INT NOT NULL,
        first_name  VARCHAR,
        last_name   VARCHAR,
        gender      CHAR(1),
        level       CHAR(4),
        PRIMARY KEY (user_id)
    )
    diststyle all;
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id    VARCHAR   NOT NULL,
        title      VARCHAR   NOT NULL,
        artist_id  VARCHAR,
        year       SMALLINT,
        duration   REAL,
        PRIMARY KEY (song_id),
        FOREIGN KEY (artist_id)  REFERENCES artists(artist_id)
    )
    distkey(song_id)
    sortkey(song_id);
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id  VARCHAR  NOT NULL,
        name       VARCHAR  NOT NULL,
        location   VARCHAR,
        latitude   NUMERIC(7,5),
        longitude  NUMERIC(8,5),
        PRIMARY KEY (artist_id)
    )
    sortkey(artist_id);
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time  TIMESTAMP  NOT NULL,
        hour        SMALLINT,
        day         SMALLINT,
        week        SMALLINT,
        month       SMALLINT,
        year        SMALLINT,
        weekday     VARCHAR(9),
        PRIMARY KEY (start_time)
    )
    sortkey(start_time);
""")

# STAGING TABLES
staging_events_copy = ("""
    COPY staging_events 
    FROM {}
    IAM_ROLE {}
    TIMEFORMAT 'epochmillisecs'
    REGION 'us-west-2'
    COMPUPDATE off
    JSON {};
""").format(logdata_source, iam_role, log_jsonpath)

staging_songs_copy = ("""
    COPY staging_songs
    FROM {}
    IAM_ROLE {}
    REGION 'us-west-2'
    COMPUPDATE off
    JSON 'auto';
""").format(songdata_source, iam_role)

# FINAL TABLES
songplay_table_insert = ("""
    INSERT INTO songplays (
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent
    )
    SELECT
        s_events.ts,
        s_events.userId,
        s_events.level,
        s_songs.song_id,
        s_songs.artist_id,
        s_events.sessionId,
        s_events.location,
        s_events.userAgent
    FROM staging_events s_events
    JOIN staging_songs s_songs
        ON s_events.song = s_songs.title
    WHERE s_events.page = 'NextSong';
""")

user_table_insert = ("""
    BEGIN TRANSACTION;
    
    INSERT INTO users (
        user_id,
        first_name,
        last_name,
        gender
    )
    SELECT DISTINCT 
        userId,
        firstName,
        lastName,
        gender
    FROM staging_events
    WHERE userId IS NOT NULL
        AND staging_events.page = 'NextSong'
        AND staging_events.userId NOT IN (
            SELECT DISTINCT user_id
            FROM users
        );
    
    UPDATE users
    SET level = latest_level_table.level 
    FROM ( SELECT distinct
                   userId,
                   FIRST_VALUE(level) OVER (
                        PARTITION BY userId
                        ORDER BY ts DESC
                        rows between unbounded preceding and unbounded following
                    ) AS level
            FROM staging_events
    ) latest_level_table
    WHERE users.user_id = latest_level_table.userId;
    
    END TRANSACTION;
""")

song_table_insert = ("""
    INSERT INTO songs (
        song_id,
        title,
        artist_id,
        year,
        duration
    )
    SELECT DISTINCT 
        song_id,
        title,
        artist_id,
        year,
        duration   
    FROM staging_songs
    WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
    INSERT INTO artists (
        artist_id,
        name,
        location,
        latitude,
        longitude
    )
    SELECT DISTINCT 
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO time (
        start_time,
        hour,
        day,
        week,
        month,
        year,
        weekday
    )
    SELECT DISTINCT
        ts,
        EXTRACT(HOUR FROM ts),
        EXTRACT(DAY FROM ts),
        EXTRACT(WEEK FROM ts),
        EXTRACT(MONTH FROM ts),
        EXTRACT(YEAR FROM ts),
        EXTRACT(DOW FROM ts)
    FROM staging_events
    WHERE staging_events.ts IS NOT NULL
        AND page = 'NextSong'
""")

# QUERY LISTS
create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    user_table_create,
    artist_table_create,
    song_table_create,
    time_table_create,
    songplay_table_create
]

drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop
]

copy_table_queries = [
    staging_events_copy,
    staging_songs_copy
]

insert_table_queries = [
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert
]
