# dowlaod csv file from url
# read csv file
# write csv file to database

import csv
import requests,zipfile, io
import sqlite3

def get_csv(url, filename):
    """
        Download csv file from url and save as filename
        @param url: url of csv file
        @param filename: name of csv file
        @return: None
    """
    # url = 'https://archives.nseindia.com/content/equities/EQUITY_L.csv'
    r = requests.get(url, allow_redirects=True)
    open('equities.csv', 'wb').write(r.content)

def read_data(filename):
    """
        Read csv file and return data
        @param filename: name of csv file
        @return: data
    """
    with open(filename, 'r') as f:
        reader = csv.reader(f)
        data = list(reader)
    return data[1:]

class SQLSchema():

    def __init__(self, db):
        self.db = db
        self.conn = sqlite3.connect(db)
        self.c = self.conn.cursor()

    def create_table(self, table_name, columns):
        """
            Create table
            @param table_name: name of table
            @param columns: list of columns
            @return: None
        """
        self.c.execute('DROP TABLE IF EXISTS ' + table_name)
        self.c.execute('CREATE TABLE ' + table_name + ' (' + ','.join(columns) + ')')

    def insert_data(self, table_name, data):
        """
            Insert data into table
            @param table_name: name of table
            @param data: data to insert
            @return: None
        """
        self.c.executemany('INSERT INTO ' + table_name + ' VALUES (' + ','.join(['?'] * len(data[0])) + ')', data)

    def commit(self):
        """
            Commit changes
            @return: None
        """
        self.conn.commit()

    def close(self):
        """
            Close connection
            @return: None
        """
        self.conn.close()


SQL_FILE = 'nse.db'

TABLE_NAME_EQUITY = 'equities'
COLUMNS_EQUITY = ['SYMBOL', 'NAME OF COMPANY', ' SERIES', ' DATE OF LISTING', ' PAID UP VALUE', ' MARKET LOT', ' ISIN NUMBER', ' FACE VALUE']

# Loop over last 30 days and dowlaod bavcopy file from https://www.nseindia.com/all-reports and write to database

import datetime

TABLE_NAME_BAVCOPY = 'bavcopy'
COLUMNS_BAVCOPY = ['SYMBOL','SERIES','OPEN','HIGH','LOW','CLOSE','LAST','PREVCLOSE','TOTTRDQTY','TOTTRDVAL','TIMESTAMP','TOTALTRADES','ISIN','NA']

def get_bavcopy_url(date):
    """
        Get bavcopy url for date
        @param date: date
        @return: bavcopy url
    """
    url = f'https://archives.nseindia.com/content/historical/EQUITIES/{date.strftime("%Y")}/{date.strftime("%b").upper()}/cm{date.strftime("%d")}{date.strftime("%b").upper()}{date.strftime("%Y")}bhav.csv.zip'
    # print(url)
    return url

def get_bavcopy_file(date):
    """
        Dowlaod bavcopy file for date
        @param date: date
        @return: filename
    """
    url = get_bavcopy_url(date)
    try:
        r = requests.get(url, timeout=5)
        if r.status_code != 200:
            return None
        z = zipfile.ZipFile(io.BytesIO(r.content))
        z.extractall()
        return z.filelist[0].filename
    except:
        return None

def get_bavcopy_data(filename):
    """
        Get bavcopy data for date
        @param date: date
        @return: bavcopy data
    """
    data = []
    try:
        with open(filename, 'r') as f:
            reader = csv.reader(f)
            for row in reader:
                data.append(row)
    except:
        pass
    return data[1:]

def get_bavcopy_data_from_date(date):
    filename = get_bavcopy_file(date)
    print(filename)
    return get_bavcopy_data(filename)

def get_bavcopy_data_from_date_range(start_date, end_date):
    data = []
    for i in range((end_date - start_date).days + 1):
        date = start_date + datetime.timedelta(days=i)
        data += get_bavcopy_data_from_date(date)
    return data

if __name__ == '__main__':
    # download csv file from url
    url = 'https://archives.nseindia.com/content/equities/EQUITY_L.csv'
    filename = 'equities.csv'
    get_csv(url, filename)

    # read csv file
    data = read_data(filename)
    print(data[1])

    # write csv file to database
    sql = SQLSchema(SQL_FILE)
    sql.create_table(TABLE_NAME_EQUITY, COLUMNS_EQUITY)
    sql.insert_data(TABLE_NAME_EQUITY, data[1:])
    sql.commit()
    sql.close()

    # get bavcopy data for last 30 days
    end_date = datetime.datetime.now()
    start_date = end_date - datetime.timedelta(days=30)
    data = get_bavcopy_data_from_date_range(start_date, end_date)
    print(data[1])

    # write bavcopy data to database
    sql = SQLSchema(SQL_FILE)
    sql.create_table(TABLE_NAME_BAVCOPY, COLUMNS_BAVCOPY)
    sql.insert_data(TABLE_NAME_BAVCOPY, data)
    sql.commit()
    sql.close()
