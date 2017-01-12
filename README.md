# mfdb-hafro-etl
ETL scripts for the MFRI MFDB implementation. The scripts on these pages assume you have access to MFRI database and that the packages `mar` (github.com/fishvice/mar) and `mfdb` (github.com/mareframe/mfdb) are installed. 

## Installation instructions
The respective R package sites have detailed installation instructions and examples. However as the `mfdb` package requires access to a postgres database and there are some MFRI specific (or mainly centos) steps needed in the installation for postgres the following steps should be sufficient to install postgres on MFRI machines:
```
$ sudo yum install postgresql-server postgresql-contrib
$ sudo postgresql-setup initdb
$ sudo systemctl enable postgresql
```
 and the follow the instructions on github.com/mareframe/mfdb, specifically:
```
$ su - posgres
$ psql 
postgres=# CREATE USER lentinj
postgres=# CREATE DATABASE mf OWNER lentinj;
```
