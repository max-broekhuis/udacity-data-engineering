from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#BE45F7'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_query = "",
                 truncate = "",
                 table_name = "",
                 *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.table_name = table_name
        self.truncate = truncate

    def execute(self, context):
        redshift_conn = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        if self.truncate:
            redshift_conn.run(f"TRUNCATE TABLE {self.table_name}")        
        self.log.info(f"Running query to load {self.table_name} into Dimension Table")
        redshift_conn.run(self.sql_query)
        self.log.info(f"Dimension table {self.table_name} loaded.")