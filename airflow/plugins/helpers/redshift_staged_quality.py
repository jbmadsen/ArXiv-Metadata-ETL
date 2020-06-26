class RedshiftStagedValidationQueries:

    MetadataFirstRowsQuery = """
        SELECT * 
        FROM staging.metadata
        LIMIT 200
    """

    AuthorsFirstRowsQuery = """
        SELECT * 
        FROM staging.authors
        LIMIT 200
    """

    CitationsFirstRowsQuery = """
        SELECT * 
        FROM staging.authors
        LIMIT 200
    """

    ClassificationsFirstRowsQuery = """
        SELECT * 
        FROM staging.authors
        LIMIT 200
    """


