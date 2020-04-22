from airflow.plugins_manager import AirflowPlugin

import operators


class InternationalFootballResultPlugin(AirflowPlugin):
    name = "international_football_result"
    operators = [
        operators.InternationalFootballDataSetToDataLake
    ]
