"""
EXAMPLE USAGE
=============

    $ python3 cldemo.py -hn <HOSTNAME> -u <USERNAME> -p <PASSWORD> 
    -n <DATABASENAME>
"""

import argparse
import mysql.connector
from loader import Loader

parser = argparse.ArgumentParser()
parser.add_argument('-hn', '--host_name', type=str, default='localhost',
                    help="MySQL host name of server.")
parser.add_argument('-u', '--username', type=str, default='root',
                    help="MySQL username.")
parser.add_argument('-p', '--password', type=str, default="password",
                    help="MySQL connection password")
parser.add_argument('-n', '--database_name', type=str, default="db",
                    help="MySQL database name")
parser.add_argument('-pk', '--primary_key', type=str,
                    help="Primary key of table to be created")
parser.add_argument('-t', '--table_name', type=str,
                    help="Table name")
parser.add_argument('-dla', '--dla', type=str,
                    help="Name of JSON key for accessing list of individual "\
                        + "JSON records.")
parser.add_argument('-url', '--url', type=str,
                    help="URL of API being accessed, just copy and paste.")

args = parser.parse_args()

mydb = mysql.connector.connect(
    host=args.host_name,
    user=args.username,
    passwd=args.password,
    database=args.database_name
)

my_loader = Loader(db = mydb, primary_key = args.primary_key, 
                   data_list_accessor = args.dla, 
                   table_name=args.table_name, url = args.url)

my_loader.mass_initial_populate()

