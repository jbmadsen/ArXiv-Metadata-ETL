class RedshiftSqlQueries:
    create_staging_tables = """
        DROP TABLE IF EXISTS public.staging_metadata;
        CREATE TABLE IF NOT EXISTS public.staging_metadata (
            id varchar(256),
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
    """
