# Import necessary library
import configparser  # Library for reading configuration files

# Read the configuration from 'dwh.cfg' file
config = configparser.ConfigParser()
config.read('dwh.cfg')
ARN             = config.get('IAM_ROLE', 'ARN')
LOG_DATA        = config.get('S3', 'LOG_DATA')
LOG_JSONPATH    = config.get('S3', 'LOG_JSONPATH')
SONG_DATA       = config.get('S3', 'SONG_DATA')
SONGS_JSONPATH  = config.get('S3', 'SONGS_JSONPATH')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_user_events"
staging_songs_table_drop =  "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop =       "DROP TABLE IF EXISTS songplays"
user_table_drop =           "DROP TABLE IF EXISTS app_users"
song_table_drop =           "DROP TABLE IF EXISTS songs"
artist_table_drop =         "DROP TABLE IF EXISTS artists"
time_table_drop =           "DROP TABLE IF EXISTS record_timestamps"

# CREATE TABLES

staging_songs_table_create = ("""
    CREATE TABLE "staging_songs" (
    num_songs           INTEGER         NULL,
    artist_id           VARCHAR         NOT NULL SORTKEY DISTKEY,
    artist_latitude     DECIMAL(9)      NULL,
    artist_longitude    DECIMAL(9)      NULL,
    artist_location     VARCHAR(500)    NULL,
    artist_name         VARCHAR(500)    NULL,
    song_id             VARCHAR         NOT NULL,
    title               VARCHAR(500)    NULL,
    duration            DECIMAL(9)      NULL,
    year                INTEGER         NULL  
);
""")
                              
staging_events_table_create= ("""
    CREATE TABLE "staging_user_events" (
    event_id            BIGINT IDENTITY(0,1)    NOT NULL,
    artist_name         VARCHAR                 NULL,
    authentication      VARCHAR                 NULL,
    firstname           VARCHAR                 NULL,
    gender              VARCHAR                 NULL,
    item_in_session     INTEGER                 NULL,
    lastname            VARCHAR                 NULL,
    length              FLOAT4                  NULL,
    level               VARCHAR                 NULL,
    location            VARCHAR                 NULL,
    method              VARCHAR                 NULL,
    page                VARCHAR                 NULL,
    registration        FLOAT4                  NULL,
    session_id          INTEGER                 NOT NULL    SORTKEY DISTKEY,
    song_name           VARCHAR                 NULL,
    status              INTEGER                 NULL,
    timestamp           BIGINT                  NOT NULL,
    user_agent          VARCHAR(500)            NULL,
    user_id             INTEGER                 NULL
);
""")

songplay_table_create = ("""
    CREATE TABLE "songplays" (
    songplay_id     INTEGER IDENTITY(0,1)   NOT NULL SORTKEY,
    time_id         TIMESTAMP               NOT NULL            REFERENCES record_timestamps(time_id), 
    user_id         INTEGER                 NOT NULL DISTKEY    REFERENCES app_users(user_id), 
    level           VARCHAR(10)             NOT NULL, 
    song_id         VARCHAR(40)             NOT NULL            REFERENCES songs(song_id), 
    artist_id       VARCHAR(50)             NOT NULL            REFERENCES artists(artist_id), 
    session_id      INTEGER                 NOT NULL, 
    location        VARCHAR(99)             NULL, 
    user_agent      VARCHAR(200)            NULL
    ); 
""")

user_table_create = ("""
    CREATE TABLE "app_users" (
    user_id     INTEGER         NOT NULL SORTKEY    PRIMARY KEY, 
    first_name  VARCHAR(50)     NULL, 
    last_name   VARCHAR(50)     NULL, 
    gender      VARCHAR(1)      NULL, 
    level       VARCHAR(10)     NULL 
    ); 
""")

song_table_create = ("""
    CREATE TABLE "songs" (
    song_id     VARCHAR(25)     NOT NULL SORTKEY    PRIMARY KEY, 
    title       VARCHAR(100)    NOT NULL, 
    artist_id   VARCHAR(25)     NOT NULL            REFERENCES artists(artist_id), 
    duration    FLOAT4          NOT NULL, 
    year        INTEGER         NOT NULL
    );
""")

artist_table_create = ("""
    CREATE TABLE "artists" (
    artist_id   VARCHAR(50)     NOT NULL SORTKEY    PRIMARY KEY, 
    name        VARCHAR(100)    NULL, 
    location    VARCHAR(50)     NULL, 
    latitude    DECIMAL(15)     NULL, 
    longitude   DECIMAL(15)     NULL 
    ); 
""")

time_table_create = ("""
    CREATE TABLE "record_timestamps" (
    time_id     TIMESTAMP   NOT NULL SORTKEY   PRIMARY KEY,
    hour        SMALLINT    NULL, 
    day_of_week SMALLINT    NULL, 
    week        SMALLINT    NULL, 
    month       SMALLINT    NULL, 
    year        SMALLINT    NULL 
    ); 
""")

# STAGING TABLES

staging_events_copy = ("""
    copy            staging_user_events 
    FROM            '{}' 
    CREDENTIALS     'aws_iam_role={}' 
    STATUPDATE      ON
    FORMAT AS JSON  '{}'                   
    REGION          'us-west-2';
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    copy                staging_songs 
    FROM                '{}' 
    CREDENTIALS         'aws_iam_role={}' 
    FORMAT AS JSON      'auto'
    ACCEPTINVCHARS AS   '^'
    STATUPDATE          ON
    REGION              'us-west-2';
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT  INTO songplays (time_id,
                            user_id,
                            level,
                            song_id,
                            artist_id,
                            session_id,
                            location,
                            user_agent)
    SELECT  DISTINCT TIMESTAMP 'epoch' +sue.timestamp/1000 \
            * INTERVAL '1 second'       AS time_id,
            sue.user_id                 AS user_id,
            sue.level                   AS level,
            ss.song_id                  AS song_id,
            ss.artist_id                AS artist_id,
            sue.session_id              AS session_id,
            sue.location                AS location,
            sue.user_agent              AS user_agent
    FROM    staging_user_events         AS sue
    JOIN    staging_songs               AS ss
            ON (sue.artist_name = ss.artist_name)
    WHERE   sue.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO app_users ( user_id,
                            first_name,
                            last_name,
                            gender,
                            level)
    SELECT  DISTINCT sue.user_id        AS user_id,
            sue.firstName               AS first_name,
            sue.lastName                AS last_name,
            sue.gender                  AS gender,
            sue.level                   AS level
    FROM staging_user_events AS sue
    WHERE sue.page = 'NextSong';
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id,
                         name,
                         location,
                         latitude,
                         longitude)
    SELECT  DISTINCT ss.artist_id   AS artist_id,
            ss.artist_name          AS name,
            ss.artist_location      AS location,
            ss.artist_latitude      AS latitude,
            ss.artist_longitude     AS longitude
    FROM staging_songs AS ss;
""")

song_table_insert = ("""
    INSERT INTO songs ( song_id,
                        title,
                        artist_id,
                        duration,
                        year)
    SELECT  DISTINCT ss.song_id AS song_id,
            ss.title            AS title,
            ss.artist_id        AS artist_id,
            ss.duration         AS duration,
            ss.year             AS year
    FROM staging_songs AS ss;
""")

time_table_insert = ("""
    INSERT INTO record_timestamps(  time_id,
                                    hour,
                                    day_of_week,
                                    week,
                                    month,
                                    year)
    SELECT  DISTINCT TIMESTAMP 'epoch' +  sue.timestamp/1000 \
            * INTERVAL '1 second'         AS time_id,
            EXTRACT(hour FROM time_id)    AS hour,
            EXTRACT(day FROM time_id)     AS day,
            EXTRACT(week FROM time_id)    AS week,
            EXTRACT(month FROM time_id)   AS month,
            EXTRACT(year FROM time_id)    AS year
    FROM    staging_user_events           AS sue
    WHERE sue.page = 'NextSong';
""")

# QUERY LISTS

# Dictionary containing SQL statements for creating tables
create_table_queries = {
    'staging events':   staging_events_table_create,
    'staging songs':    staging_songs_table_create,
    'user':             user_table_create,
    'artist':           artist_table_create,
    'time':             time_table_create,
    'song':             song_table_create,
    'songplay':         songplay_table_create
}

# Dictionary containing SQL statements for dropping tables
drop_table_queries = {
    'staging events':   staging_events_table_drop,
    'staging songs':    staging_songs_table_drop,
    'songplay':         songplay_table_drop,
    'user':             user_table_drop,
    'song':             song_table_drop,
    'artist':           artist_table_drop,
    'time':             time_table_drop
}

# Dictionary containing SQL statements for copying data into staging tables
copy_table_queries = {
    'staging events':   staging_events_copy,
    'staging songs':    staging_songs_copy
}

# Dictionary containing SQL statements for inserting data into analytical tables
insert_table_queries = {
    'songplay':     songplay_table_insert,
    'user':         user_table_insert,
    'song':         song_table_insert,
    'artist':       artist_table_insert,
    'time':         time_table_insert
}