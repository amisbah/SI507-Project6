# Import statements
import psycopg2
import psycopg2.extras
from config import *
import csv
from psycopg2 import sql

# Write code / functions to set up database connection and cursor here.
def get_connection_and_cursor():
    try:
        if db_password != "":
            db_connection = psycopg2.connect("dbname='{0}' user='{1}' password='{2}'".format(db_name, db_user, db_password))
            print("Success connecting to database")
        else:
            db_connection = psycopg2.connect("dbname='{0}' user='{1}'".format(db_name, db_user))
    except:
        print("Unable to connect to the database. Check server and credentials.")
        sys.exit(1) # Stop running program if there's no db connection.

    db_cursor = db_connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    return db_connection, db_cursor

conn, cur = get_connection_and_cursor()


# Write code / functions to create tables with the columns you want and all database setup here.
cur.execute("""CREATE TABLE IF NOT EXISTS Sites(
	id SERIAL PRIMARY KEY,
	name VARCHAR(128) UNIQUE,
	type VARCHAR(128),
	state_id int,
	location VARCHAR(255),
	description text
	)""");

    # -- * ID (SERIAL)
    # -- * Name (VARCHAR up to 128 chars, UNIQUE)
    # -- * Type [e.g. "National Lakeshore" or "National Park"] (VARCHAR up to 128 chars)
    # -- * State_ID (INTEGER - FOREIGN KEY REFERENCING States)
    # -- * Location (VARCHAR up to 255 chars)
    # -- * Description (TEXT)

cur.execute("""CREATE TABLE IF NOT EXISTS States(
	id SERIAL PRIMARY KEY,
	name VARCHAR(40) UNIQUE
	)""");
    #     * ID (SERIAL)
    # * Name (VARCHAR up to 40 chars, UNIQUE)
conn.commit()

# Write code / functions to deal with CSV files and insert data into the database here.

class Site(object):
	def __init__(self, sitelist):
		self.name = sitelist['NAME']
		if sitelist['TYPE'] == "":
			self.type = ""
		else:
			self.type = sitelist['TYPE']
		self.location = sitelist['LOCATION']
		self.description = sitelist['DESCRIPTION']

	def get_site_dict(self):
		return {
			'name': self.name,
			'type': self.type,
			'location': self.location,
			'description': self.description
		}

file_list = []

ark_file = csv.DictReader(open("arkansas.csv",'r'))
file_list.append(ark_file)
# ark_file.close()

cal_file = csv.DictReader(open("california.csv",'r'))
file_list.append(cal_file)
# cal_file.close()

mi_file = csv.DictReader(open("michigan.csv",'r'))
file_list.append(mi_file)
# mi_file.close()

cur.execute("""INSERT INTO states (Name) VALUES ('Arkansas') on conflict do nothing""");
cur.execute("""INSERT INTO states (Name) VALUES ('California') on conflict do nothing""");
cur.execute("""INSERT INTO states (Name) VALUES ('Michigan') on conflict do nothing""");
conn.commit()

# site_list = []
ark_site_objects = []
for line in ark_file:
	site_file = Site(line)
	ark_site_objects.append(site_file)

cal_site_objects = []
for line in cal_file:
	site_file = Site(line)
	cal_site_objects.append(site_file)

mi_site_objects = []
for line in mi_file:
	site_file = Site(line)
	mi_site_objects.append(site_file)	

for site in ark_site_objects:
# for state_num in range(4,7):
		# column_names = file.keys()

		# generate insert into query string
		cur.execute("""INSERT INTO sites (name, type, state_id, location, description) VALUES (%(name)s,%(type)s,4,%(location)s,%(description)s) ON CONFLICT DO NOTHING""", site.get_site_dict())
		    # sql.SQL(', ').join(map(sql.Identifier, column_names)),
		    # sql.SQL(', ').join(map(sql.Placeholder, column_names)),
		    # state_num
		# query_string = query.as_string(conn)
		# cur.execute(query_string)

# conn.commit()

for site in cal_site_objects:
	cur.execute("""INSERT INTO sites (name, type, state_id, location, description) VALUES (%(name)s,%(type)s,5,%(location)s,%(description)s) ON CONFLICT DO NOTHING""", site.get_site_dict())
# conn.commit()

for site in mi_site_objects:
	cur.execute("""INSERT INTO sites (name, type, state_id, location, description) VALUES (%(name)s,%(type)s,6,%(location)s,%(description)s) ON CONFLICT DO NOTHING""", site.get_site_dict())
# conn.commit()
# Make sure to commit your database changes with .commit() on the database connection.



# Write code to be invoked here (e.g. invoking any functions you wrote above)



# Write code to make queries and save data in variables here.
cur.execute("""SELECT location FROM sites""")
all_locations = cur.fetchall()
# for each in all_locations:
# 	all_locations.append(each)
# print (all_locations[0])
# print(len(all_locations))
cur.execute("""SELECT name FROM sites WHERE description LIKE '%beautiful%'  """)
beautiful_sites = cur.fetchall()
# print(len(beautiful_sites))

cur.execute("""SELECT COUNT(type) FROM sites WHERE type = 'National Lakeshore' """)
natl_lakeshores = cur.fetchall()
# print(natl_lakeshores)

cur.execute("""SELECT SI.name FROM sites AS SI INNER JOIN states AS ST ON ST.id = SI.state_id WHERE SI.state_id = '6' """)
michigan_names = cur.fetchall()
# print(len(michigan_names))

cur.execute("""SELECT COUNT(SI.name) FROM sites AS SI INNER JOIN states AS ST ON ST.id = SI.state_id WHERE SI.state_id = '4' """)
total_number_arkansas = cur.fetchall()
print(total_number_arkansas)
# We have not provided any tests, but you could write your own in this file or another file, if you want.
