# -*- coding: utf-8 -*-
"""
Created on Sun Dec 22 2019
@author: danvargg
"""
import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Executes all drop table queries.
    Arguments:
        - cur: cursor variable of the database
        - conn: connection variable of the database
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Executes all create table queries.
    Arguments:
        - cur: cursor variable of the database
        - conn: connection variable of the database
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Drops and recreates the required tables."""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
        *config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
