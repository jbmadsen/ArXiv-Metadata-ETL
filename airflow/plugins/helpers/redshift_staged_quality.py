class RedshiftStagedQualityChecks:

    CheckMetadataQuery = """
        SELECT id
        FROM staging.metadata
        LIMIT 1
    """

    @staticmethod
    def ValidateMetadataQuery(input):
        if input is not None:
            return True
        return False


