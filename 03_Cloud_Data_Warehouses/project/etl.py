# -*- coding: utf-8 -*-
"""
Created on Sun Dec 22 2019
@author: danvargg
"""
import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Loads the data from files in S3 to the stage tables.
    Arguments:
        - cur: cursor variable of the database
        - conn: connection variable of the database
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Inserts the data from stage table to final table.
    Arguments:
        - cur: cursor variable of the database
        - conn: connection variable of the database
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Loads the S3 files into sage tables, loads the final tables from stage tables."""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
        *config['CLUSTER'].values()))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
