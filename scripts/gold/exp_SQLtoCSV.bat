@echo off
echo Enter schema:
set /p schema=
echo Enter table name:
set /p table_name=

set BCP_EXPORT_SERVER=AS14_ER2024\SQLEXPRESS
set BCP_EXPORT_DB=DataWareHouse


set BCP_STATEMENT1="DECLARE @colnames VARCHAR(max);SELECT @colnames = COALESCE(@colnames + ',', '') + column_name from %BCP_EXPORT_DB%.INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='%table_name%' AND TABLE_SCHEMA = '%schema%'; select @colnames;"
set BCP_STATEMENT2="SELECT * FROM %schema%.%table_name%"

BCP %BCP_STATEMENT1% queryout delete1.csv -c -T -S "%BCP_EXPORT_SERVER%;Encrypt=no"
BCP %BCP_STATEMENT2% queryout delete2.csv -c -t, -T -S "%BCP_EXPORT_SERVER%;Encrypt=no" -d %BCP_EXPORT_DB%

copy /b delete1.csv+delete2.csv %schema%.%table_name%.csv

del delete1.csv
del delete2.csv