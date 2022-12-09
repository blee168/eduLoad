import argparse
import datetime
import mysql.connector
import pickle
import re
import sys
import threading
import time
from urllib.request import urlopen
from tqdm import tqdm
from json import loads

def string_typer(s):
    """
    INPUT: string
    OUTPUT: string of either "string", "int", "float" for specifying string,
    integer, or decimal. Defaults to "str".
    """

    digits = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']
    symbols = ['-', '.']

    if (type(s) == int):
        return "int"
    elif (type(s) == float):
        return "float"

    if s is None or len(s) == 0:
        return "string"

    if len(s) == 1:
        if s not in digits:
            return "string"

    if s[0] not in digits and s[0] not in symbols:
        return "string"

    for c in s[1:]:
        if c not in digits and c not in symbols:
            return "string"

    if "." in s:
        return "float"
    else:
        return "int"

def list_typer(values):
    """
    INPUT: list
    OUTPUT: the most restrictive applicable type describing the whole list,
    specified as one of the strings "string", "float", or "int".
    """

    types = []
    for v in values:
        types.append(string_typer(v))

    if "string" in types:
        return "string"
    elif "float" in types:
        return "float"
    else:
        return "int"

class Loader:
    """
    Class extracting educationdata.urban.org data to a local MySQL database.
    """

    def __init__(self, db, primary_key, data_list_accessor,
                table_name, url, year_range = None, api_key = None,
                link_word = None):
        """
        INPUTS:
            db: mysql.connector database object
            api_key: string for accessing weatherstackAPI
            year_range: YYYY-YYYY
                if this string is not none, then a regex routine will try to
                access data stored at the url, according to different years
            primary_key: string for specifying column to be primary key
            data_list_accessor: string for specifying where in the JSON the
                actual list of data JSON objects are
            table_name: string
            link_word assumes prior knowledge of JSON structure in which
            one JSON contains a list of JSONs and a link to a subsequent related list
            of JSONs, similar to a linked list. Needs to be passed in as an argument
            since this is somewhat privileged information.
        """

        if table_name is None:
            sys.exit("I need a table name.")

        if url is None:
            sys.exit("I need the URL of the API we are accessing.")

        self.db = db
        
        # It may be possible api_key is irrelevant, since this often gets
        # baked into the URL itself...
        self.api_key = api_key
        self.table_name = table_name
        self.cursor = db.cursor(buffered = True)
        self.table_exists = False
        self.primary_key = primary_key
        self.original_url = url
        self.url = url
        self.data_list_accessor = data_list_accessor

        self.link_word = link_word

        self.year_range = year_range

        # TODO: I want to somehow divorce the code from a priori knowledge of
        # the structure of the JSON later, for example by passing a hierarchic
        # path (for example, a comma-separated list of words?) to the actual 
        # list of JSON data objects, rather than a single string.

        self.get_data()

        if len(self.data) == 0:
            sys.exit("Dataset appears to be empty.")
        else:
            self.keys = list(self.data[0].keys())
            for i in range(len(self.data)):
                for k in list(self.data[i].keys()):
                    if k not in self.keys:
                        self.keys.append(k)

        self.entry_problems = False
        self.problematic_entries = []

        try:
            self.cursor.execute("SELECT * FROM " + self.table_name + ";")
            self.table_exists = True

        except:
            self.cursor.execute('CREATE TABLE ' + self.table_name + '(' + self.table_header() + ')')
            self.db.commit()


    def get_data(self):
        print("Now fetching data at " + self.url)
        response = urlopen(self.url)
        self.data_chunk = loads(response.read())
        self.data = []
        self.data += self.data_chunk[self.data_list_accessor]
        i = 2
        if self.link_word is not None:
            while self.data_chunk[self.link_word] is not None:
                print("Adding chunk " + str(i))
                i += 1
                # update the URL
                self.url = self.data_chunk[self.link_word]
                # open the new request at the new URL
                response = urlopen(self.url)
                # read the data from the new request
                self.data_chunk = loads(response.read())
                # add new data to old data
                self.data += self.data_chunk[self.data_list_accessor]

    def table_header(self):
        """
        Automates creation of initial table creation string for a MySQL Table.
        INPUT: JSON dataset structured such that dataset[i] is one dict object.
        OUTPUT: String of column names and datatypes for use in a CREATE TABLE
            string for MySQL cursor execution.

        """
        header = ""

        for k in self.keys:
            col_values = [self.data[i][k] for i in range(len(self.data))]
            if list_typer(col_values) == "int":
                header += k + " INT"
                if k == self.primary_key:
                    header += " PRIMARY KEY, "

                else:
                    header += ", "

            elif list_typer(col_values) == "float":
                header += k + " FLOAT (8, 4), "

            else:
                header += k + " VARCHAR(100), "

        return header[:-2]


    def mass_populate(self):
        """
        Calls self.insert() to automatically load data straight from the API.
        """

        if self.year_range is not None:
            year_str_regex = re.search(r'([1][9]\d\d|[2][0]\d\d)', self.url)
            current_year = int(
                self.url[year_str_regex.end() - 4:year_str_regex.end()]
            )

            years = list(range(
                int(self.year_range[0:4]), int(self.year_range[-4:]) + 1
            ))

            years.sort(reverse=True)
            print(years)

            if current_year not in years:
                years.append(current_year)

            print("Years to be added: " + str(years))

            for y in tqdm(years):
                # The iteration is set up to iterate through pages, essentially.
                # This first line brings us back to page 1 without needing to
                # know how the pages themselves are actually designated.
                self.url = self.original_url

                # Now we can change the year without ending up on the last page
                # at the start.
                self.url = self.url.replace(str(current_year), str(y))
                self.get_data()

                print("Now attempting to load " + str(len(self.data)) + \
                    " entries associated with " + str(y) + ".")
                for i in tqdm(range(int(len(self.data)))):
                    self.insert(self.data[i])

        else:
            print("Now attemping to load " + str(len(self.data)) + " entries.")
            for i in tqdm(range(int(len(self.data)))):
                self.insert(self.data[i])
        
        if self.entry_problems:
            self.export_problematic_entries()
            print("There might be problems. Look out for a .pkl file.")



    def insert(self, entry):
        """
        Load a single entry. Checks if table exists.
        If exists, then cols should be derived from something like
            crs.execute("SELECT * FROM " + table_name + " LIMIT 1;")
        INPUT: dict
        """

        if self.table_exists:
            self.cursor.execute("SELECT * FROM " + self.table_name + " LIMIT 1;")
            derived_keys = self.cursor.column_names

            header = "("
            for c in derived_keys:
                header += c + ", "
            header = header[:-2] + ")"

            values = "("

            for c in derived_keys:
                if c not in entry.keys() or entry[c] == None:
                    values += "NULL, "
                else:
                    values += "\"" + str(entry[c]) + "\", "

        else:
            header = "("
            for k in list(entry.keys()):
                header += k + ", "
            header = header[:-2] + ")"

            values = ""

            for v in list(entry.values()):
                if v == None:
                    values += 'NULL, '

                else:
                    v = str(v)
                    if '"' in v:
                        v = v.replace('"', "\\" + '"')
                    values += "\"" + str(v) + "\", "


            values = values[:-2]

        try:
            self.cursor.execute("INSERT IGNORE INTO " + self.table_name \
                + " " + header + "VALUES (" +  values + ");")
            self.db.commit()
        except:
            self.entry_problems = True
            self.problematic_entries.append(entry)

        self.db.commit()
        
    def export_problematic_entries(self):
        if len(self.problematic_entries) != 0:
            f = open("problematic_entries.pkl", "wb")
            pickle.dump(self.problematic_entries, f)
