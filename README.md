# mfdb-hafro-etl
ETL scripts for the MFRI MFDB implementation. The scripts on these pages assume you have access to MFRI database and that the packages `mar` (github.com/fishvice/mar) and `mfdb` (github.com/mareframe/mfdb) are installed. 

## Installation instructions
The respective R package sites have detailed installation instructions and examples. However as the `mfdb` package requires access to a postgres database and there are some MFRI specific (or mainly centos) steps needed in the installation for postgres the following steps should be sufficient to install postgres on MFRI machines:
```
$ sudo yum install postgresql-server postgresql-contrib postgresql-devel
$ sudo postgresql-setup initdb
$ sudo systemctl enable postgresql
$ sudo systemctl start postgresql
```
 and the follow the instructions on github.com/mareframe/mfdb, specifically:
```
$ su - postgres
$ psql 
postgres=# CREATE USER bthe
postgres=# CREATE DATABASE mf OWNER bthe;
```
## DB setup
The setup for the MFRI MFDB version "should" be as straight forward as simply:
```
source('R/initdb.R')
```
but life is never so simple. The script depend on one users database privileges so your milage may vary. Notably the users needs to be able to setup local temporary tables on `mar`. But there is an easy way out. A backup of the database is stored on `/u2/reikn/Tac/2017/mfdb_dump` (or equivalent path when using laptops) so the user simply needs to restore the DB from this backup like so:
```
cp /u2/reikn/Tac/mfdb_dump/iceland_18012017.tar.gz .
gunzip iceland_18012017.tar.gz
pg_restore --no-owner --role=notandi --clean -d mf iceland_18012017.tar
```
