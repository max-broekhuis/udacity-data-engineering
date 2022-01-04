from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#465BF4'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables = [],
                 *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        redshift_conn = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        for table in self.tables:
            records = redshift_conn.get_records(f"select count(*) from {table};")
            if len(records) < 1 or len(records[0]) < 1 or records[0][0] < 1:
                self.log.error(f"Data quality issues detected for {table}.")
                raise ValueError(f"Data quality issues detected for {table}")
            self.log.info(f"Data quality status for {table}: Validated. Number of records: {records[0][0]")