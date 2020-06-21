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

        DROP TABLE IF EXISTS public.staging_authors;
        CREATE TABLE IF NOT EXISTS public.staging_authors (
            journal_id varchar(256),
            authors_list varchar(256)
        );

        DROP TABLE IF EXISTS public.staging_citations;
        CREATE TABLE IF NOT EXISTS public.staging_citations (
            citation_id varchar(256),
            citation_list varchar(256)
        );

        DROP TABLE IF EXISTS public.staging_classifications;
        CREATE TABLE IF NOT EXISTS public.staging_classifications (
            tag_id varchar(256),
            tag_name varchar(256)
        );
    """
