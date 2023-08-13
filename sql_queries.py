import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

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
    artist_num_songs    INT,
    artist_id           VARCHAR(25),
    artist_latitude     FLOAT4,
    artist_longitude    FLOAT4,
    artist_location     VARCHAR(35),
    artist_name         VARCHAR(100),
    song_id             VARCHAR(25),
    song_title          VARCHAR(100),
    song_duration       FLOAT4,
    song_year           INT
);
""")
                              
staging_events_table_create= ("""
    CREATE TABLE "staging_user_events" (
    event_id            INT,
    artist_name         VARCHAR(100),
    authentication      VARCHAR(35),
    firstname           VARCHAR(35),
    gender              VARCHAR(1),
    item_in_session     INT,
    lastname            VARCHAR(35),
    length              FLOAT4,
    level               VARCHAR(10),
    location            VARCHAR(55),
    method              VARCHAR(3),
    page                VARCHAR(10),
    registration        FLOAT4,
    session_id          INT,
    song_name           VARCHAR(100),
    status              INT,
    timestamp           INT IDENTITY(0,1),
    user_agent          VARCHAR(250),
    user_id             INT
);
""")

songplay_table_create = ("""
    CREATE TABLE "songplays" (
    songplay_id     INT IDENTITY(0,1)   PRIMARY KEY,
    time_id         NUMERIC(13,0)   REFERENCES record_timestamps(time_id), 
    user_id         INT             REFERENCES app_users(user_id), 
    level           VARCHAR(10), 
    song_id         VARCHAR(25)     REFERENCES songs(song_id), 
    artist_id       VARCHAR(25)     REFERENCES artists(artist_id), 
    session_id      INT, 
    location        VARCHAR(55), 
    user_agent      VARCHAR(250)
    ); 
""")

user_table_create = ("""
    CREATE TABLE "app_users" (
    user_id     INT PRIMARY KEY, 
    first_name  VARCHAR(35), 
    last_name   VARCHAR(35), 
    gender      VARCHAR(1), 
    level       VARCHAR(10) 
    ); 
""")

song_table_create = ("""
    CREATE TABLE "songs" (
    song_id     VARCHAR(25)     PRIMARY KEY, 
    title       VARCHAR(100), 
    artist_id   VARCHAR(25)     REFERENCES artists(artist_id), 
    duration    FLOAT4, 
    year        INT
    );
""")

artist_table_create = ("""
    CREATE TABLE "artists" (
    artist_id   VARCHAR(25)     PRIMARY KEY, 
    name        VARCHAR(100), 
    location    VARCHAR(35), 
    latitude    FLOAT4, 
    longitude   FLOAT4 
    ); 
""")

time_table_create = ("""
    CREATE TABLE "record_timestamps" (
    time_id     INT IDENTITY(0,1)   PRIMARY KEY,
    hour        INT, 
    day_of_week INT, 
    week        INT, 
    month       INT, 
    year        INT 
    ); 
""")

# STAGING TABLES

staging_events_copy = ("""
    copy            staging_user_events 
    FROM            's3://udacity-dend/log_data' 
    credentials     'aws_iam_role={}' 
    compupdate      off 
    region          'us-west-2';
""").format(config["IAM_ROLE"]['ARN'])

staging_songs_copy = ("""
    copy            staging_songs 
    FROM            's3://udacity-dend/song_data' 
    credentials     'aws_iam_role={}' 
    compupdate      off 
    region          'us-west-2';
""").format(config["IAM_ROLE"]['ARN'])

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (time_id, user_id, level, song_id, artist_id, session_id, location, user_agent) 
        VALUES(%s, %s, %s, %s, %s, %s, %s, %s); 
""")

user_table_insert = ("""INSERT INTO app_users (user_id, first_name, last_name, gender, level) 
        VALUES(%s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING; 
""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration) 
        VALUES(%s, %s, %s, %s, %s); 
""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude) 
        VALUES(%s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING; 
""")

time_table_insert = ("""INSERT INTO record_timestamps (hour, day_of_week, week, month, year) 
        VALUES(%s, %s, %s, %s, %s); 
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, artist_table_create, time_table_create, song_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
