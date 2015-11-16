# DataComparison
This is intended to compare the data in lookup tables between versions of the database on different servers.

Create these two files and put them in the same folder as the solution file:
* TablesToCompare.supersecret
    * list of tables to compare
    * one per line
    * format - SchemaName.TableName
    * ex: dbo.AccountType
    * The first column in the table must be an integer that uniquely identifies the record.
* ConnectionStrings.supersecret
    * list of database pairs to compare
    * one per line
    * format - FriendlyName1,ServerName1,DatabaseName1,FriendlyName2,ServerName2,DatabaseName2
    * ex: Dev,DevServerName,DevDBName,Test,TestServerName,TestDBName

The results will go in files in a folder called Results.
