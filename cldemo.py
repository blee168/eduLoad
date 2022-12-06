"""
EXAMPLE USAGE
=============

    $ python3 cldemo.py -hn <LOCALHOST> -u root -p <PWD> -n <DATABASE NAME>
    -dla <DATALIST ACCESSOR> -url <URL> -t <TABLE NAME> -pk <PRIMARY KEY>
    -y <YYYY-YYYY> -l <LINKING WORD>

    Argument LINKING WORD assumes prior knowledge of JSON structure in which
    one JSON contains a list of JSONs and a link to a subsequent related list
    of JSONs, similar to a linked list. Needs to be passed in as an argument
    since this is somewhat privileged information.
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
parser.add_argument('-y', '--years', type=str,
                    help="Specify year (YYYY) of range of years (YYYY-YYYY)")
parser.add_argument('-l', '--link_word', type=str,
                    help="Because some returns link to additional lists of \
                          JSON objects")

args = parser.parse_args()

mydb = mysql.connector.connect(
    host=args.host_name,
    user=args.username,
    passwd=args.password,
    database=args.database_name
)

my_loader = Loader(db = mydb, primary_key = args.primary_key,
                   data_list_accessor = args.dla,
                   table_name=args.table_name, url = args.url,
                   year_range = args.years,
                   link_word = args.link_word)

my_loader.mass_populate()
