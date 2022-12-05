import argparse
import datetime
import mysql.connector
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
                table_name, url, api_key = None):
        """
        INPUTS:
            db: mysql.connector database object
            api_key: string for accessing weatherstackAPI
            location: string for determining where to check data. see
                weatherstackAPI for valid locations
        """
        self.db = db
        self.api_key = api_key
        self.table_name = table_name
        self.cursor = db.cursor(buffered = True)
        self.table_exists = False 
        self.primary_key = primary_key
        self.url = url
        self.data_list_accessor = data_list_accessor

        print("Now fetching data at provided url...")

        response = urlopen(self.url)

        # TODO: I want to somehow divorce the code from a priori knowledge of
        # the structure of the JSON later, for example by passing in an index
        # path to the actual list of JSON data objects, rather than a single
        # string.

        self.data = loads(response.read())[self.data_list_accessor]
        if len(self.data) == 0:
            sys.exit("Dataset appears to be empty.")
        else:
            self.keys = list(self.data[0].keys())

        try:
            self.cursor.execute("SELECT * FROM " + self.table_name + ";")
            self.table_exists = True
    
        except:
            self.cursor.execute('CREATE TABLE ' + self.table_name + '(' + self.table_header() + ')')
            self.db.commit()


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
        print("Now loading " + str(len(self.data)) + " entries.")
        for i in tqdm(range(int(len(self.data)))):
            self.insert(self.data[i])
            


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

            col_str = "("
            for c in derived_keys:
                col_str += c + ", "
            col_str = col_str[:-2] + ")"

            in_str = "("

            for c in derived_keys:
                if entry[c] == None:
                    in_str += "NULL, "
                else:
                    in_str += "\"" + str(entry[c]) + "\", "

            self.cursor.execute("INSERT IGNORE INTO " + self.table_name \
                + " " + col_str + "VALUES" + in_str[:-2] + ")")
            self.db.commit()

        else:
            header = "("
            for k in list(entry.keys()):
                header += k + ", "

            header = header[:-2] + ")"

            values = "("
            for v in list(entry.values()):
                if v == None:
                    values += 'NULL, '
                
                else:
                    v = str(v)
                    if '"' in v:
                        v = v.replace('"', "\\" + '"')
                    values += "\"" + str(v) + "\", "
    

            values = values[:-2] + ")"
            
            self.cursor.execute("INSERT IGNORE INTO " + self.table_name + " " + header + " VALUES " + values)
            self.db.commit()