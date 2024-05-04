-----------------------------
// DATABASE
-----------------------------
create databases Spot;

-----------------------------
// STORAGE INTEGRATION
-----------------------------
create or replace storage integration s3_int
type = External_stage
storage_provider = s3
Enabled= TRUE
Storage_aws_role_arn = 'arn:aws:iam::905418165206:role/snowflake-s3-connection'
storage_allowed_locations = ('s3://spotify-etl-data/transformed_data/');

DESC integration s3_int;


-----------------------------
// STAGE
-----------------------------
create or replace stage SPOT.PUBLIC.s3_album_stage
url = 's3://spotify-etl-data/transformed_data/album_data/'
storage_integration = s3_int;

create or replace stage SPOT.PUBLIC.s3_artist_stage
url = 's3://spotify-etl-data/transformed_data/artist_data/'
storage_integration = s3_int;


create or replace stage SPOT.PUBLIC.s3_song_stage
url = 's3://spotify-etl-data/transformed_data/songs_data/'
storage_integration = s3_int;


-----------------------------
// FILE FORMAT
-----------------------------
create or replace file format SPOT.PUBLIC.s3_file_format
type = csv
record_delimiter = '\n'
field_delimiter = ','
skip_header = 1
null_if = ('NULL','null')
empty_field_as_null = TRUE
FIELD_OPTIONALLY_ENCLOSED_BY = '0x22';


-----------------------------
// CREATING TABLE
-----------------------------
// ** Album
create or replace table SPOT.PUBLIC.Album(
album_id varchar(50),
name VARCHAR(80),
release_date date,
total_tracs INT,
url VARCHAR(100)
);

// ** Artist
create or replace table SPOT.PUBLIC.Artist(
artist_id VARCHAR(50),
artist_name VARCHAR(50),
external_url VARCHAR(100)
);

// ** Song
create or replace table SPOT.PUBLIC.Song(
song_id VARCHAR(50),
song_name VARCHAR(80),
duration_ms INT,
url VARCHAR(100),
popularity INT,
song_added Date,
album_id VARCHAR(50),
artist_id VARCHAR(50)
);


-----------------------------
// COPY cmd
-----------------------------
// ** Album
copy into SPOT.PUBLIC.ALBUM
from @SPOT.PUBLIC.S3_ALBUM_STAGE
file_format = (format_name = SPOT.PUBLIC.S3_FILE_FORMAT)
ON_ERROR = 'skip_file_3';

// ** Artist
copy into SPOT.PUBLIC.ARTIST
from @SPOT.PUBLIC.S3_ARTIST_STAGE
file_format = (format_name = SPOT.PUBLIC.S3_FILE_FORMAT)
ON_ERROR = 'skip_file_3';

// ** Song
copy into SPOT.PUBLIC.SONG
from @SPOT.PUBLIC.S3_SONG_STAGE
file_format = (format_name = SPOT.PUBLIC.S3_FILE_FORMAT)
ON_ERROR = 'skip_file_3';

select * from SPOT.PUBLIC.ALBUM;
select * from SPOT.PUBLIC.ARTIST;
select * from SPOT.PUBLIC.SONG;

List @SPOT.PUBLIC.S3_ALBUM_STAGE;

-----------------------------
// Schema
-----------------------------
create or replace schema SPOT.Pipes;



-----------------------------
// Creating SnowPipe
-----------------------------
// ** Album
create or replace pipe SPOT.PIPES.s3_pipe
auto_ingest = TRUE
As
copy into SPOT.PUBLIC.ALBUM
from @SPOT.PUBLIC.S3_ALBUM_STAGE
file_format = (format_name = SPOT.PUBLIC.S3_FILE_FORMAT)
ON_ERROR = 'skip_file_3';

DESC pipe SPOT.PIPES.S3_PIPE;

// ** Artist
create or replace pipe SPOT.PIPES.s3_pipe_artist
auto_ingest = TRUE
AS
copy into SPOT.PUBLIC.ARTIST
from @SPOT.PUBLIC.S3_ARTIST_STAGE
file_format = (format_name = SPOT.PUBLIC.S3_FILE_FORMAT)
ON_ERROR = 'skip_file_3';

DESC pipe SPOT.PIPES.s3_pipe_artist;

// ** Song
create or replace pipe SPOT.PIPES.s3_pipe_song
auto_ingest = TRUE
As
copy into SPOT.PUBLIC.SONG
from @SPOT.PUBLIC.S3_SONG_STAGE
file_format = (format_name = SPOT.PUBLIC.S3_FILE_FORMAT)
ON_ERROR = 'skip_file_3';

DESC pipe SPOT.PIPES.S3_PIPE_SONG;

