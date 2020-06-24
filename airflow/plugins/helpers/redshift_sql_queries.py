class RedshiftSqlQueries:

    create_staging_tables = """
        CREATE SCHEMA IF NOT EXISTS STAGING;

        DROP TABLE IF EXISTS staging.metadata;
        CREATE TABLE IF NOT EXISTS staging.metadata (
            id VARCHAR(256),
            submitter VARCHAR(256),
            authors VARCHAR(256),
            title VARCHAR(256),
            comments VARCHAR(256),
            "journal-ref" VARCHAR(256),
            doi VARCHAR(256),
            abstract VARCHAR(4000),
            "report-no" VARCHAR(256),
            categories VARCHAR(256),
            versions VARCHAR(256)
        );

        DROP TABLE IF EXISTS staging.authors;
        CREATE TABLE IF NOT EXISTS staging.authors (
            metadata_id VARCHAR(256),
            author VARCHAR(256)
        );

        DROP TABLE IF EXISTS staging.citations;
        CREATE TABLE IF NOT EXISTS staging.citations (
            metadata_id VARCHAR(256),
            citation VARCHAR(256)
        );

        DROP TABLE IF EXISTS staging.classifications;
        CREATE TABLE IF NOT EXISTS staging.classifications (
            tag_id VARCHAR(256),
            tag_name VARCHAR(256)
        );
    """

    create_main_tables = """
        DROP TABLE IF EXISTS public.articles_fact;
        CREATE TABLE IF NOT EXISTS public.articles_fact (
            article_id INT PRIMARY KEY,
            submitter_name VARCHAR(256),
            title VARCHAR(256),
            comments VARCHAR(256),
            journalref VARCHAR(256),
            doi VARCHAR(256),
            abstract VARCHAR(256),
            reportno VARCHAR(256),
            classifications VARCHAR(256),
            versions VARCHAR(256)
        );

        DROP TABLE IF EXISTS public.versions_dim;
        CREATE TABLE IF NOT EXISTS public.versions_dim (
            id INT PRIMARY KEY,
            article_id INT,
            author VARCHAR(256)
        );

        DROP TABLE IF EXISTS public.authors_dim;
        CREATE TABLE IF NOT EXISTS public.authors_dim (
            id INT PRIMARY KEY,
            article_id INT,
            metadata_id VARCHAR(256),
            author_name VARCHAR(256)
        );

        DROP TABLE IF EXISTS public.classifications_dim;
        CREATE TABLE IF NOT EXISTS public.classifications_dim (
            id INT PRIMARY KEY,
            article_id INT,
            tag_id VARCHAR(256),
            tag_name VARCHAR(256)
        );

        DROP TABLE IF EXISTS public.citations_dim;
        CREATE TABLE IF NOT EXISTS public.citations_dim (
            id INT PRIMARY KEY,
            article_from_id INT,
            article_to_id INT,
            citation_from VARCHAR(256),
            citation_to VARCHAR(256)
        );
    """

    insert_articles_fact = """
        INSERT INTO public.articles_fact
        SELECT DISTINCT
            md5(events.sessionid || events.start_time) songplay_id,
            events.start_time, 
            events.userid, 
            events.level, 
            songs.song_id, 
            songs.artist_id, 
            events.sessionid, 
            events.location, 
            events.useragent
        FROM (
            SELECT 
                TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, 
                *
            FROM staging_events
            WHERE page='NextSong'
        ) events
        LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration;
    """