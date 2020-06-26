import logging
from airflow import AirflowException


class DataValidationChecks:

    @staticmethod
    def ValidateNoEmptyColumnsInResult(input_list):
        
        logging.info(f"Validating no empty columns in result... checking {len(input_list)} elements")
        passed_list = None
        
        try:
            # Act
            for record in input_list:
                if passed_list is None:
                    # Create a list of all false
                    passed_list = [False] * len(record)
                for i in range(len(record)):
                    if not passed_list[i]:
                        passed_list[i] = True if record[i] is not None else False

            # Assert
            return all(item == True for item in passed_list)
        except Exception as e:
            error = f"Error validating: {e}"
            logging.info(error)    
            raise AirflowException(error)

    
    @staticmethod
    def ResultsExists(input_list):
        
        logging.info(f"Validating results exists... checking {len(input_list)} elements")

        if input_list is None or len(input_list) <= 0:
            error = f"Error validating input_list. No Elements in list."
            logging.info(error)    
            raise AirflowException(error)

        if input_list[0] is None or len(input_list[0]) <= 0:
            error = f"Error validating input_list. No Elements in first list item."
            logging.info(error)    
            raise AirflowException(error)

        return True


