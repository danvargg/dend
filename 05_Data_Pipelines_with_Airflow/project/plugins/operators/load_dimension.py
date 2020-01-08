# -*- coding: utf-8 -*-
"""
Created on Wed Jan 08 2020
@author: danvargg
"""
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    do_sql = """
        TRUNCATE TABLE {};
        INSERT INTO {}
        {};
        COMMIT;
    """

    @apply_defaults
    def __init__(self, redshift_conn_id='', table='', load_sql_stmt='', *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.load_sql_stmt = load_sql_stmt

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(
            'Loading dimension table {} in Redshift'.format(self.table))

        formatted_sql = LoadDimensionOperator.do_sql.format(
            self.table,
            self.table,
            self.load_sql_stmt)

        redshift.run(formatted_sql)
