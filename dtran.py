#!/usr/bin/env python
#-*-coding:utf-8 -*-
import cx_Oracle
import sys
import os
import pymysql
import pymysql.cursors
import datetime
import time
from optparse import OptionParser
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.AL32UTF8'

def get_cli_options():
        parser = OptionParser(usage="usage: python %prog [options]",
                                  description=""".........data transfer.........
						 Only o2o,o2m,m2o,m2m Supported """)
        parser.add_option("--source", "--sourcedb",
                              dest="source_db",
                              help="Usage:  ip:port:user:passwd:owner:table")
        parser.add_option("--dest", "--destdb",
                              dest="dest_db",
                              help="Usage:  ip:port:user:passwd:owner:table")
	parser.add_option("--type", "--type",
			      dest="db_type",
			      default="NULL",
			      help="Usage:  o2o,o2m,m2o,m2m")
	parser.add_option("--from", "--fromsid",
			      dest="from_sid",
			      default="orcl",
			      help="Usage:  Only Source Oracle needed")
	parser.add_option("--to", "--tosid",
			      dest="to_sid",
			      default="orcl",
			      help="Usage:  Only Target Oracle needed")
        (options, args) = parser.parse_args()
        return options

class Producer():
	def __init__(self,v_str,v_type,v_sid):
        	self.host=v_str.strip().split(':')[0]
        	self.port=v_str.strip().split(':')[1]
        	self.user=v_str.strip().split(':')[2]
        	self.passwd=v_str.strip().split(':')[3]
		self.db=v_str.strip().split(':')[4]
		self.table=v_str.strip().split(':')[5]
		self.type=v_type.strip()
		self.sid=v_sid.strip()
	
	def get_type(self):
                type=self.type
                return type

	def get_cursor(self):
		type = self.get_type()
		if type == 'o2o' or type == 'o2m':
			try:
				v_dsn = cx_Oracle.makedsn(self.host, self.port, self.sid)
				conn = cx_Oracle.Connection(user=self.user, password=self.passwd, dsn=v_dsn)
			except Exception , e:
				print e
			return conn
		elif type == 'm2m' or type == 'm2o':
			try:
				conn = pymysql.connect(host=self.host, port=int(self.port), user=self.user, passwd=self.passwd, db=self.db, charset='UTF8',cursorclass = pymysql.cursors.SSCursor)
			except Exception , e:
				print e
			return conn
	def delta(self):
		cursor = self.get_cursor().cursor()
		cursor.execute("delete from  "+self.db+"."+self.table)
		cursor.execute("commit")
		cursor.close()
	
class Consumer():
	def __init__(self,v_str,v_type,v_sid):
                self.host=v_str.strip().split(':')[0]
                self.port=v_str.strip().split(':')[1]
                self.user=v_str.strip().split(':')[2]
                self.passwd=v_str.strip().split(':')[3]
		self.db=v_str.strip().split(':')[4]
		self.table=v_str.strip().split(':')[5]
		self.type=v_type.strip()
		self.sid=v_sid.strip()

	def get_type(self):
		type=self.type
		return type

	def get_cursor(self):
		db_type = self.get_type()
		if db_type == 'o2m' or db_type == 'm2m':
			try:
				conn = pymysql.connect(host=self.host, port=int(self.port), user=self.user, passwd=self.passwd, db=self.db, charset='UTF8')
			except Exception , e:
				print e
			return conn
		elif db_type == 'o2o' or db_type == 'm2o':
			try:
				v_dsn = cx_Oracle.makedsn(self.host, self.port, self.sid)
				conn = cx_Oracle.Connection(user=self.user, password=self.passwd, dsn=v_dsn)
			except Exception , e:
				print e
			return conn
	
	def get_sql(self):
		db_type=self.get_type()
		cursor=self.get_cursor().cursor()
		if db_type == 'o2m' or db_type == 'm2m':
			cursor.execute("select * from "+self.db+"."+self.table+" where 1=0")
			pre_insert_sql="insert into "+self.db+"."+self.table+" ("
			title = ""
			v_col = ""
			for col in range(len(cursor.description)):
                 	       if col != len(cursor.description)-1:
                        	        title += cursor.description[col][0]+","
                               		v_col += "%s"+","
                       	       else:
                                	title += cursor.description[col][0]
                                	v_col += "%s"
                	insert_sql = pre_insert_sql+title+") values("+v_col+")"
                	return insert_sql	
		elif db_type == 'o2o' or db_type == 'm2o':
			cursor.execute("select * from "+self.db+"."+self.table+" where 1=0")
			pre_insert_sql="insert into "+self.db+"."+self.table+" ("
			title = ""
			v_col = ""
			for col in range(len(cursor.description)):
				if col != len(cursor.description)-1:
					title += cursor.description[col][0]+","
					v_col += ":"+str(col+1)+","
				else:
					title += cursor.description[col][0]
					v_col += ":"+str(col+1)
			insert_sql = pre_insert_sql+title+") values("+v_col+")"
			return insert_sql

def migarate():
	batch=10000
	options = get_cli_options()	
	t_producer = Producer(options.source_db,options.db_type,options.from_sid)
	t_consumer = Consumer(options.dest_db,options.db_type,options.to_sid)

	v_sql = t_consumer.get_sql()
	v_type = t_consumer.get_type()
	fromcursor = t_producer.get_cursor().cursor()
	tocursor = t_consumer.get_cursor().cursor()
	fromcursor.execute("select * from "+t_producer.db+"."+t_producer.table)
	num_fields = len(fromcursor.description)
	result = fromcursor.fetchmany(batch)
	print "=====================data transfer begin...====================="
	count = 0
	while result:
		try:
			if v_type == 'o2m' or v_type == 'm2m':
				tocursor.executemany(v_sql,result)
                		tocursor.execute('commit')
			elif v_type == 'o2o' or v_type == 'm2o':
				tocursor.prepare(v_sql)
				tocursor.executemany(None,result)
				tocursor.execute('commit')
			count += len(result)
			print str(count)+" rows commited..."+time.strftime("%Y-%m-%d %X")
		except (KeyboardInterrupt, SystemExit):
                	raise
          	except Exception,e:
               		print Exception,":",e

		result=[]
		result = fromcursor.fetchmany(batch)

	print "=====================data transfer end...====================="
	print
	print "The next Step will Delete the data of source table,Are You Sure?"
	doption = raw_input('Option:(Y/y,N/n)  ')
	if doption.lower() == 'y':
		t_producer.delta()
		sys.exit()
	else:
		print "Delete will not excute..."
		sys.exit()
	

def main():
	migarate()

if __name__ == '__main__':
       main()
