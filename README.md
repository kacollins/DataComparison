# Data Comparison
This application is intended to compare the data in lookup tables between versions of the database on different servers.
The goal is to schedule a task to run the app daily to catch changes made by users, differences in the results of scripts (be careful with identity insert!), and data changes that need to be rolled.

The comparison works for views too!

Create these two text files and put them in the bin\Debug folder:
* TablesToCompare.supersecret
    * list of tables and views to compare
    * one table or view per line
    * Include the schema name and table/view name, separated by a period (example: dbo.AccountType).
    * If the schema or table/view name needs brackets, include them (example: dbo.[View with spaces in name]).
    * The first column in the table or view must be an integer that uniquely identifies the record.
* DatabasePairs.supersecret
    * list of database pairs to compare
    * one database pair per line
    * Include these parts separated by commas:
        * friendly name of first database (ex: Dev or Test)
        * server name for first database
        * name of first database
        * friendly name of second database
        * server name for second database
        * name of second database
    * example: Dev,DevServerName,DevDBName,Test,TestServerName,TestDBName

The results will go in a folder called Results under bin\Debug, with one file per database pair per day.

When scheduling the task, put the location of the bin\Debug folder in the "Start in (optional)" field.

Optional arguments:
* different filename for TablesToCompare.supersecret (ex: EnumTables.supersecret or MyTables.txt)
* different filename for DatabasePairs.supersecret (ex: MyDatabases.supersecret or AnotherListOfDatabases.txt)
