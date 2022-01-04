from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class CreateTablesOperator(BaseOperator):
    ui_color = '#33FFBB'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 *args, **kwargs):

        super(CreateTablesOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Executing table creation ")

        lf = open(CreateTablesOperator.create_tables, 'r')
        sql_file = lf.read()
        lf.close()
        
        sql_query = sql_file.split(';')
        
        for command in sql_commands:
            if command.rstrip() != '':
                redshift.run(command)
               
       
    
#        query = open('/home/workspace/aiflow/create_tables.sql', 'r').read()
#        redshift.run(query)
        
       