from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import operators
import helpers

# Defining the plugin class
class UdacityPlugin(AirflowPlugin):
    name = "udacity_plugin"
    operators = [
        operators.StageFromS3ToRedshiftOperator,
        operators.RedshiftExecuteSQLOperator,
        operators.LoadRedshiftTableOperator,
        operators.DataQualityOperator
    ]
    helpers = [
        helpers.RedshiftSqlQueries,
        helpers.RedshiftStagedValidationQueries,
        helpers.RedshiftMainValidationQueries,
        helpers.DataValidationChecks,
        helpers.load_authors,
        helpers.load_citations,
    ]
