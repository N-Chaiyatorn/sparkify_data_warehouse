import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def execute_queries(queries, cur, conn):
    """
    Queries list executor.
    
    ...
    
    Parameters
    ----------
    queries : 
        List of the queries to be executed
    cur : 
        Postgresql connection object's cursor
    conn : 
        Postgresql connection object
    """
    for query in queries:
        cur.execute(query)
        conn.commit()

        
def main():
    # Read configuration file.
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    # Connect to the Redshift cluster.
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    # Drop all staging tables, dimensional tables and fact table.
    execute_queries(drop_table_queries, cur, conn)
    
    # Create all staging tables, dimensional tables and fact table.
    execute_queries(create_table_queries, cur, conn)

    # Close connection to the Redshift cluster after finished.
    conn.close()


if __name__ == "__main__":
    main()