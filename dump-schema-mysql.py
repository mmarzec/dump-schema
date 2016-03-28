#!/usr/bin/env python
"""dump-schema-mysql
Dump database schema to files structure.

Usage:
  dump-schema-mysql.py [options] <database>

Options:
  -u, --user=name        User for login if not current user.
  -p, --password=name    Password to use when connecting to server.
                         If password is not given it's asked from the tty.
  -H, --host=name        Connect to host. [default: localhost]
  -d --dir=DIR           Output directory. [default: ./db_schema]

  -h --help              Show this screen.
  --version              Show version.

"""

import os
import shutil
import pymysql as mdb
from docopt import docopt

def read_one_col(query, col_num=0):
    cursor.execute(query)
    connection.commit()
    rows = cursor.fetchall()
    result = []
    for row in rows:
        result.append(row[col_num])
    return result

def read_one_field(query, col_num=0):
    cursor.execute(query)
    connection.commit()
    result = cursor.fetchone()
    return result[col_num]

def get_item_list(item_type):
    if item_type == 'table':
        query = """SELECT table_name FROM information_schema.tables
        WHERE table_schema=DATABASE()
        AND table_type = "BASE TABLE"
        """
        return read_one_col(query)

    elif item_type == 'view':
        query = """SELECT table_name FROM information_schema.tables
        WHERE table_schema=DATABASE()
        AND table_type = "VIEW"
        """
        return read_one_col(query)

    elif item_type == 'procedure':
        query = """SELECT specific_name FROM information_schema.routines
        WHERE routine_schema = DATABASE()
        AND routine_type = "PROCEDURE"
        """
        return read_one_col(query)

    elif item_type == 'function':
        query = """SELECT specific_name FROM information_schema.routines
        WHERE routine_schema = DATABASE()
        AND routine_type = "FUNCTION"
        """
        return read_one_col(query)

    elif item_type == 'trigger':
        query = """SELECT trigger_name FROM information_schema.triggers
        WHERE trigger_schema = DATABASE()
        """
        return read_one_col(query)

    elif item_type == 'event':
        query = """SELECT event_name FROM information_schema.events
        WHERE event_schema = DATABASE()
        """
        return read_one_col(query)

def get_item_body(name, item_type):
    if item_type == 'table':
        query = 'show create table `'+name+'`'
        return read_one_field(query, 1)

    elif item_type == 'view':
        query = 'show create view `'+name+'`'
        return read_one_field(query, 1)

    elif item_type == 'procedure':
        query = 'show create procedure `'+name+'`'
        return read_one_field(query, 2)

    elif item_type == 'function':
        query = 'show create function `'+name+'`'
        return read_one_field(query, 2)

    elif item_type == 'trigger':
        query = 'show create trigger `'+name+'`'
        return read_one_field(query, 2)

    elif item_type == 'event':
        query = 'show create event `'+name+'`'
        return read_one_field(query, 3)


if __name__ == '__main__':
    arguments = docopt(__doc__, version='dump-schema 0.1.0')

    db_host = arguments['--host']
    db_user = arguments['--user']
    db_pass = arguments['--password']
    db_name = arguments['<database>']
    dump_dir = arguments['--dir']

    connection = mdb.connect(db_host, db_user, db_pass, db_name)
    cursor = connection.cursor()

    # delete old dump
    if os.path.exists(dump_dir):
        shutil.rmtree(dump_dir)

    # create dir
    os.makedirs(dump_dir)


    # dump schema
    def dump_items(item_type):
        items = get_item_list(item_type)
        if items:
            item_type_plural = item_type+'s'
            os.makedirs(dump_dir+'/'+item_type_plural)
            for item in items:
                with open(dump_dir+'/'+item_type_plural+'/'+item+'.sql', 'w') as file_:
                    file_.write(get_item_body(item, item_type))

    dump_items('table')
    dump_items('view')
    dump_items('procedure')
    dump_items('function')
    dump_items('trigger')
    dump_items('event')
