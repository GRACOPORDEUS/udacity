import configparser

# Load configurations from dwh.cfg file
config = configparser.ConfigParser()
config.read('dwh.cfg')

# Define variables for file paths and ARN
LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")
ARN = config.get("IAM_ROLE", "ARN")

# DROP TABLES
print("Part 1: Dropping Tables")

# SQL statements to drop existing tables
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
print("Part 2: Creating Tables")

# SQL statements to create tables
staging_events_table_create= ("""
    CREATE TABLE staging_events(
            artist            VARCHAR(500)      NULL,
            auth              VARCHAR(500)      NULL,
            firstName         VARCHAR(500)      NULL,
            gender            VARCHAR(500)       NULL,
            ItemInSession     INTEGER           NULL,
            lastName          VARCHAR(500)      NULL,
            length            FLOAT             NULL,
            level             VARCHAR(500)      NULL,
            location          VARCHAR(500)      NULL,
            method            VARCHAR(500)      NULL,
            page              VARCHAR(500)      NULL,
            registration      VARCHAR(500)      NULL,
            sessionId         INTEGER           NOT NULL        SORTKEY DISTKEY,
            song              NVARCHAR          NULL,
            status            INTEGER           NULL,
            ts                BIGINT            NOT NULL, 
            userAgent         VARCHAR(500)      NULL, 
            userId            INTEGER           NULL     
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs(
            song_id            VARCHAR(500)      NOT NULL       SORTKEY DISTKEY,
            artist_id          VARCHAR(500)      NOT NULL,
            artist_latitude    FLOAT             NULL,
            artist_longitude   FLOAT             NULL,
            artist_location    VARCHAR(500)      NULL,
            artist_name        VARCHAR(500)      NULL,
            duration           FLOAT             NULL,
            num_songs          INT               NULL,
            title              NVARCHAR          NULL,
            year               INT               NULL
    );
""")

songplay_table_create = ("""
    CREATE TABLE songplays(
            songplay_id         INTEGER IDENTITY(0,1)   NOT NULL        PRIMARY KEY,
            start_time          TIMESTAMP               NOT NULL,
            user_id             INTEGER                 NOT NULL        SORTKEY DISTKEY,
            level               VARCHAR(500)            NOT NULL,
            song_id             VARCHAR(500)            NOT NULL,
            artist_id           VARCHAR(500)            NOT NULL,
            session_id          VARCHAR(500)            NOT NULL,
            location            VARCHAR(500)            NOT NULL,
            user_agent          VARCHAR(500)            NOT NULL
    );
""")

user_table_create = ("""
    CREATE TABLE users(
            user_id            VARCHAR(500)         NOT NULL        PRIMARY KEY,
            first_name         VARCHAR(500)         NULL,
            last_name          VARCHAR(500)         NULL,
            gender             VARCHAR(500)         NULL,
            level              VARCHAR(500)         NULL
    );
""")

song_table_create = ("""
    CREATE TABLE songs(
            song_id           VARCHAR(500)          NOT NULL        PRIMARY KEY,
            title             VARCHAR(500)          NULL,
            artist_id         VARCHAR(500)          NULL,
            year              INTEGER               NULL,
            duration          FLOAT                 NULL
    );
""")

artist_table_create = ("""
    CREATE TABLE artists(
            artist_id         VARCHAR(500)          NOT NULL        PRIMARY KEY,
            name              VARCHAR(500)          NULL,
            location          VARCHAR(500)          NULL,
            latitude          VARCHAR(500)          NULL,
            longitude         VARCHAR(500)          NULL
    );
""")

time_table_create = ("""

    CREATE TABLE time(
            start_time        TIMESTAMP             NOT NULL        PRIMARY KEY,
            hour              SMALLINT              NULL,
            day               SMALLINT              NULL,
            week              SMALLINT              NULL,
            month             SMALLINT              NULL,
            year              SMALLINT              NULL,
            weekday           SMALLINT              NULL
    );
""")


# STAGING TABLES
print("Part 3: Copying Data to Staging Tables")

# SQL statements to copy data into staging tables from S3 buckets
staging_events_copy = ("""
    COPY staging_events FROM {}
    credentials 'aws_iam_role={}'
    format as json {}
    STATUPDATE ON
    region 'us-west-2';
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    copy staging_songs from {}
    credentials 'aws_iam_role={}'
    region      'us-west-2'
    format       as JSON 'auto'
    TRUNCATECOLUMNS 
    BLANKSASNULL 
    EMPTYASNULL
""").format(SONG_DATA, ARN)

# FINAL TABLES
print("Part 4: Inserting Data into Final Tables")

# SQL statements to insert data into final tables
songplay_table_insert = ("""
    INSERT INTO songplays(
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
        DISTINCT TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' AS start_time,
        se.userid,
        se.level,
        ss.song_id,
        ss.artist_id,
        se.sessionid,
        se.location,
        se.useragent
    FROM 
        staging_events  se 
    JOIN 
        staging_songs ss  
    ON 
        ss.artist_name=se.artist 
        AND se.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users(                 
        user_id,
        first_name,
        last_name,
        gender,
        level
    )

    SELECT  
        DISTINCT se.userId,
        se.firstName,
        se.lastName,
        se.gender,
        se.level,
    FROM staging_events AS se
    WHERE se.page = 'NextSong';
""")

song_table_insert = ("""
    INSERT INTO songs(                 
        song_id,
        title,
        artist_id,
        year,
        duration
    )

    SELECT 
        DISTINCT ss.song_id,
        ss.title,
        ss.artist_id,
        ss.year,
        ss.duration,
    FROM staging_songs AS ss;
""")

artist_table_insert = ("""
    INSERT INTO artists(
        artist_id,
        name,
        location,
        latitude,
        longitude
    )
    
    SELECT  
        DISTINCT ss.artist_id,
        ss.artist_name,
        ss.artist_location,
        ss.artist_latitude,
        ss.artist_longitude,
    FROM staging_songs AS ss;
""")

time_table_insert = ("""
    INSERT INTO time(
        start_time,
        hour,
        day,
        week,
        month,
        year,
        weekday
    )

    SELECT  
        DISTINCT TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second',
        EXTRACT(hour FROM start_time),
        EXTRACT(day FROM start_time),
        EXTRACT(week FROM start_time),
        EXTRACT(month FROM start_time),
        EXTRACT(year FROM start_time),
        EXTRACT(week FROM start_time),
    FROM    staging_event se
    WHERE se.page = 'NextSong';
""")

# QUERY LISTS

# Lists of SQL queries for creating, dropping, copying, and inserting data into tables
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
