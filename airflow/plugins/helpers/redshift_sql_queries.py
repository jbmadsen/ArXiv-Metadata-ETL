class RedshiftSqlQueries:

    create_staging_tables = """
        CREATE SCHEMA IF NOT EXISTS STAGING;

        DROP TABLE IF EXISTS staging.metadata;
        CREATE TABLE IF NOT EXISTS staging.metadata (
            metadata_id varchar(256),
            submitter varchar(256),
            authors varchar(256),
            title varchar(256),
            comments varchar(256),
            journalref varchar(256),
            doi varchar(256),
            abstract varchar(4000),
            reportno varchar(256),
            categories varchar(256),
            versions varchar(256)
        );

        DROP TABLE IF EXISTS staging.authors;
        CREATE TABLE IF NOT EXISTS staging.authors (
            metadata_id varchar(256),
            author varchar(256)
        );

        DROP TABLE IF EXISTS staging.citations;
        CREATE TABLE IF NOT EXISTS staging.citations (
            metadata_id varchar(256),
            citation varchar(256)
        );

        DROP TABLE IF EXISTS staging.classifications;
        CREATE TABLE IF NOT EXISTS staging.classifications (
            tag_id varchar(256),
            tag_name varchar(256)
        );
    """

    create_main_tables = """
        DROP TABLE IF EXISTS public.articles_fact;
        CREATE TABLE IF NOT EXISTS public.articles_fact (
            article_id int INT PRIMARY KEY,
            submitter_name varchar(256),
            title varchar(256),
            comments varchar(256),
            journalref varchar(256),
            doi varchar(256),
            abstract varchar(256),
            reportno varchar(256),
            classifications varchar(256),
            versions varchar(256)
        );

        DROP TABLE IF EXISTS public.versions_dim;
        CREATE TABLE IF NOT EXISTS public.versions_dim (
            id INT PRIMARY KEY,
            article_id INT,
            author varchar(256)
        );

        DROP TABLE IF EXISTS public.authors_dim;
        CREATE TABLE IF NOT EXISTS public.authors_dim (
            id INT PRIMARY KEY,
            article_id INT,
            metadata_id varchar(256),
            author_name varchar(256)
        );

        DROP TABLE IF EXISTS public.classifications_dim;
        CREATE TABLE IF NOT EXISTS public.classifications_dim (
            id INT PRIMARY KEY,
            article_id INT,
            tag_id varchar(256),
            tag_name varchar(256)
        );

        DROP TABLE IF EXISTS public.citations_dim;
        CREATE TABLE IF NOT EXISTS public.citations_dim (
            id INT PRIMARY KEY,
            article_from_id INT,
            article_to_id INT,
            citation_from varchar(256),
            citation_to varchar(256)
        );
    """
