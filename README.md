# YCSB-Monetdb-binding
YCSB interface for monetdb
# Installation
1.Download the YCSB project via the following URL https://github.com/brianfrankcooper/YCSB/archive/0.1.4.zip and unzip it.
2.Include the YCSB-Monetdb-binding within the YCSB-0.1.4 directory
3.Add monetdb to the list of modules in YCSB-0.1.4/pom.xml
4.Add the following lines to the DATABASE section in YCSB-0.1.4/bin/ycsb: "monetdb" : "monetdb.MonetdbClient"
5.compile everything by executing the following command within the YCSB-0.1.4 directory: mvn clean package
