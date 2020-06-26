class RedshiftMainValidationQueries:

    ArticleFactFirstRowsQuery = """
        SELECT * 
        FROM public.articles_fact
        LIMIT 200
    """

    VersionsDimFirstRowsQuery = """
        SELECT * 
        FROM public.versions_dim
        LIMIT 200
    """

    AuthorsDimFirstRowsQuery = """
        SELECT * 
        FROM public.authors_dim
        LIMIT 200
    """

    ClassificationsDimFirstRowsQuery = """
        SELECT * 
        FROM public.classifications_dim
        LIMIT 200
    """

    # There are A LOT on unknown citations here, due to the changes between pre-2007 and post-2007
    CitationsDimFirstRowsQuery = """
        SELECT * 
        FROM public.citations_dim
        WHERE article_from_id is not null
            and article_to_id is not null
            and citation_from is not null
            and citation_to is not null
        LIMIT 10
    """

    CanJoinFactAndAllDims = """
        SELECT * 
        FROM public.articles_fact fct
        INNER JOIN public.versions_dim ver 
            ON ver.article_id = fct.article_id
        INNER JOIN public.authors_dim aut
            ON aut.article_id = fct.article_id
        INNER JOIN public.classifications_dim cls
            ON cls.article_id = fct.article_id
        LEFT JOIN public.citations_dim cit
            ON cit.article_from_id = fct.article_id
                OR cit.article_to_id = fct.article_id
        LIMIT 1
    """
    


