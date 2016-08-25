import psycopg2
import yaml
from tableausdk import *
from tableausdk.Extract import *
from datetime import datetime
import sys

# Data Types Dictionary from python types to Tableau types
data_types= {
    25: 16,     #string
    1042: 16,   #string
    1043: 15,   #char
    23: 7,      #int
    20:7,       #int
    701: 10,    #float
    1700:10,    #float
    16: 11,     #boolean
    1082: 12,   #date
    1114: 13,   #datetime
    1186: 14    #duration
}

# Function: cast data Python: Tableau
def cast_data(data, data_types):
    result = data_types.get(data)
    if result is None:
        print "Data type not detected see data_types dictionary"
        sys.exit()
    return result
#end cast_data

# Function: Insert element in a TDE row
def insert_element(row, pos, type, element):
    if element is None:
        row.setNull(pos)
    else:
        if type == 7:
            row.setInteger(pos,element)
        elif type == 10:
            row.setDouble(pos,element)
        elif type == 11:
            row.setBoolean(pos,element)
        elif type == 15:
            row.setCharString(pos,element)
        elif type == 16:
            row.setString(pos,element)
        elif type == 12:
            date_object = element
            year = int(datetime.strftime(date_object,'%Y'))
            month = int(datetime.strftime(date_object,'%m'))
            day = int(datetime.strftime(date_object,'%d'))
            row.setDate(pos, year, month, day)
        elif type == 13:
            date_object = element
            year = int(datetime.strftime(date_object,'%Y'))
            month = int(datetime.strftime(date_object,'%m'))
            day = int(datetime.strftime(date_object,'%d'))
            hour = int(datetime.strftime(date_object,'%H'))
            min = int(datetime.strftime(date_object,'%M'))
            sec = int(datetime.strftime(date_object,'%S'))
            frac = 0 # fractions of a second aka milliseconds
            row.setDateTime(pos, year, month, day, hour, min, sec, frac)
        else:
            print 'type {0} not found element {1}'.format(type, element)
#end insert_element

#Read arguments
#Command line: $python create_extract.py configuration.yaml query_options.yaml
if len(sys.argv) != 3:
    print "Usage $python create_extract.py configuration.yaml query_options.yaml"
    sys.exit()

# Load configuration: configuration.yml
f = open(sys.argv[1])
dataMap = yaml.load(f)
f.close()

# Load configuration: query_options.yml
f = open(sys.argv[2])
tdeMap = yaml.load(f)
f.close()

try:

    # Create a psycopg2 connection object
    conn = psycopg2.connect(host= dataMap['db_host'], user= dataMap['db_user'], password= dataMap['db_password'], database= dataMap['db_name'])

    # Open a cursor to perform database operations
    cursor = conn.cursor()

    # Executing the query and getting data
    cursor.execute(tdeMap['query'])
    number_rows = cursor.rowcount
    number_columns = len(cursor.description)
    colnames = [desc[0] for desc in cursor.description]
    coltypes = [desc[1] for desc in cursor.description]
    # Usefull when data type is not in data_types dictionary
    #print colnames
    #print coltypes
    if number_rows >= 1 and number_columns > 1:
        result = [item for item in cursor.fetchall()]
    else:
        result = [item[0] for item in cursor.fetchall()]

    # Close server connection
    cursor.close()
    conn.close()

    # Initialize SDK
    ExtractAPI.initialize()

    # Create tde (removes tde with same name)
    try:
        os.remove(tdeMap['tde_name'])
        new_extract = Extract(tdeMap['tde_name'])
    except:
        new_extract = Extract(tdeMap['tde_name'])

    # Create a new table definition
    table_definition = TableDefinition()

    # Insert into TD column names and column types and get Tableau types
    tableau_col_types = []
    for i in range(0,len(colnames)):
        data_type = cast_data(coltypes[i], data_types)
        table_definition.addColumn(str(colnames[i]).decode('utf-8'), data_type)
        tableau_col_types.append(data_type)

    # Add TD 'Extract' to tde
    new_table = new_extract.addTable('Extract', table_definition)


    # Create new row
    new_row = Row(table_definition)

    # Insert data into tde
    for row in result:
        for i in range(0,len(colnames)):
            insert_element(new_row, i, tableau_col_types[i], row[i])
        new_table.insert(new_row)

    # Close the extract in order to save the .tde file and clean up resources
    new_extract.close()

    # Close SDK
    ExtractAPI.cleanup()

except Exception as e:
    print e
