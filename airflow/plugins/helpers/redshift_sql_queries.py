class RedshiftSqlQueries:
    create_staging_tables = """
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
