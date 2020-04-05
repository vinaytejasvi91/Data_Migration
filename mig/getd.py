import commands
import re

query = "show databases"


connect_string = 'impala-shell -i lxapp6344.datahub.cloud.telstra.com -d default -k --ssl --ca_cert=/opt/cloudera/security/pki/rootca-intca.cert.pem -B   -q '

result_string = connect_string + "'" +query +"'"
print result_string

status, output = commands.getstatusoutput(result_string)

dblist = list(output.split("\t"))


dblist2 = [x.replace('\n', '') for x in dblist]

del dblist2[-1]
del dblist2[0]
del dblist2[0]

ext_table_start = 'show tables'
ext_table_end = 'Fetched'


counter = 0;

print "The total number of databases: " +str(len(dblist2))

for db in dblist2:
   counter = counter + 1
   tablelistquery = "use " +db +";show tables;"
   result_string = connect_string + "'" +tablelistquery +"'"
   status, output = commands.getstatusoutput(result_string)
   tlist = str((output[output.find(ext_table_start)+len(ext_table_start):output.rfind(ext_table_end)]).strip())
   if len(tlist) > 0:
       tablelist = list(db + "." + x for x in tlist.split('\n'))
   
   table_count = len(tablelist)
   print "The datbase " +db +" has " +str(table_count) +" tables"
   print tablelist
   for table in tablelist:
       if table_count > 0:
           shtable = "show create table " +table
           stats = "show table stats " + table
           rcount = "select count (*) from " + table

	   
           result_string = connect_string + "'" +shtable +"'"
           status, output = commands.getstatusoutput(result_string)

           ext_show_table_start = shtable
           ext_show_table_end = 'Fetched'
           showcreatetable = str((output[output.find(ext_show_table_start)+len(ext_show_table_start):output.rfind(ext_show_table_end)]).strip())
           print "show create table: " +showcreatetable

           ext_show_table_stats_start = stats;
           ext_show_table_ends = 'Fetched'
	   
           result_string = connect_string + "'" +stats +"'"
           status, output = commands.getstatusoutput(result_string)
           showtablestats = str((output[output.find(ext_show_table_stats_start)+len(ext_show_table_stats_start):output.rfind(ext_show_table_ends)]).strip())
           print "show table stats : " +showtablestats

           ext_show_table_count_start = rcount
           ext_show_table_count_end = 'Fetched'
	   
           result_string = connect_string + "'" +rcount +"'"
           status, output = commands.getstatusoutput(result_string)
           showrowcount = str((output[output.find(ext_show_table_count_start)+len(ext_show_table_count_start):output.rfind(ext_show_table_count_end)]).strip())
           print "show row count: " +showrowcount


   if counter == 5:
       break;



