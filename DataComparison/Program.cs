using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.SqlClient;
using System.Data;
using System.IO;

namespace DataComparison
{
    class Program
    {
        static void Main(string[] args)
        {
            //TODO: add argument for whether this is being run manually vs automated?

            string tableFileName = string.Empty;
            string databaseFileName = string.Empty;

            if (args.Length > (int)InputFile.TablesToCompare)
            {
                tableFileName = args[(int)InputFile.TablesToCompare];

                if (args.Length > (int)InputFile.DatabasePairs)
                {
                    databaseFileName = args[(int)InputFile.DatabasePairs];
                }
            }

            CompareTables(tableFileName, databaseFileName);

            Console.WriteLine("Press enter to exit:");
            Console.Read();
        }

        #region Methods

        private static void CompareTables(string tableFileName, string databaseFileName)
        {
            TableFileResult result = GetTablesToCompare(tableFileName);

            if (result.Tables.Any())
            {
                CompareDatabasePairs(databaseFileName, result.Tables);
            }

            if (result.Errors.Any())
            {
                HandleTopLevelError(AppendLines(result.Errors), false);
            }
            else if (!result.Tables.Any())
            {
                HandleTopLevelError("No tables to compare!");
            }
        }

        private static void CompareDatabasePairs(string databaseFileName, List<Table> tablesToCompare)
        {
            DatabaseFileResult result = GetDatabasePairs(databaseFileName);

            if (result.DatabasePairs.Any())
            {
                foreach (DatabasePair dbPair in result.DatabasePairs)
                {
                    DisplayProgressMessage($"Comparing {dbPair.Database1.FriendlyName} to {dbPair.Database2.FriendlyName}...");
                    CompareDatabasePair(tablesToCompare, dbPair);
                }

                Console.WriteLine("Done!");
            }

            if (result.Errors.Any())
            {
                HandleTopLevelError(AppendLines(result.Errors), false);
            }
            else if (!result.DatabasePairs.Any())
            {
                HandleTopLevelError("No databases to compare!");
            }
        }

        private static void HandleTopLevelError(string errorMessage, bool writeToConsole = true)
        {
            if (writeToConsole)
            {
                Console.WriteLine(errorMessage);
            }

            WriteToFile($"{DateForFileName}_Error", errorMessage, OutputFileExtension.txt);
        }

        private static TableFileResult GetTablesToCompare(string fileName)
        {
            if (string.IsNullOrWhiteSpace(fileName))
            {
                fileName = $"{InputFile.TablesToCompare}.supersecret";
            }

            List<string> lines = GetFileLines(fileName);
            const char separator = '.';

            List<Table> tablesToCompare = lines.Where(line => line.Split(separator).Length == Enum.GetValues(typeof(TablePart)).Length)
                                                .Select(validLine => validLine.Split(separator))
                                                .Select(parts => new Table(parts[(int)TablePart.SchemaName],
                                                                            parts[(int)TablePart.TableName]))
                                                .ToList();

            List<string> errorMessages = lines.Where(line => line.Split(separator).Length != Enum.GetValues(typeof(TablePart)).Length)
                                                .Select(invalidLine => $"Invalid schema/table format: {invalidLine}")
                                                .ToList();

            if (errorMessages.Any())
            {
                Console.WriteLine($"Error: Invalid schema/table format in {InputFile.TablesToCompare} file.");
            }

            TableFileResult result = new TableFileResult(tablesToCompare, errorMessages);

            return result;
        }

        private static DatabaseFileResult GetDatabasePairs(string fileName)
        {
            if (string.IsNullOrWhiteSpace(fileName))
            {
                fileName = $"{InputFile.DatabasePairs}.supersecret";
            }

            List<string> lines = GetFileLines(fileName);
            const char separator = ',';

            List<DatabasePair> databasePairs = lines.Where(line => line.Split(separator).Length == Enum.GetValues(typeof(DatabasePairPart)).Length)
                                                    .Select(validLine => validLine.Split(separator))
                                                    .Select(parts => new DatabasePair(new Database(parts[(int)DatabasePairPart.FriendlyName1],
                                                                                                    parts[(int)DatabasePairPart.ServerName1],
                                                                                                    parts[(int)DatabasePairPart.DatabaseName1]),
                                                                                        new Database(parts[(int)DatabasePairPart.FriendlyName2],
                                                                                                    parts[(int)DatabasePairPart.ServerName2],
                                                                                                    parts[(int)DatabasePairPart.DatabaseName2])))
                                                    .ToList();

            List<string> errorMessages = lines.Where(line => line.Split(separator).Length != Enum.GetValues(typeof(DatabasePairPart)).Length)
                                                .Select(invalidLine => $"Invalid database pair format: {invalidLine}")
                                                .ToList();

            if (errorMessages.Any())
            {
                Console.WriteLine($"Error: Invalid database pair format in {InputFile.DatabasePairs} file.");
            }

            DatabaseFileResult result = new DatabaseFileResult(databasePairs, errorMessages);

            return result;
        }

        private static List<string> GetFileLines(string fileName)
        {
            const char backSlash = '\\';
            DirectoryInfo directoryInfo = new DirectoryInfo($"{CurrentDirectory}{backSlash}{Folder.Inputs}");
            FileInfo file = directoryInfo.GetFiles(fileName).FirstOrDefault();

            if (file == null)
            {
                return new List<string>();
            }
            else
            {
                List<string> lines = File.ReadAllLines(file.FullName)
                                        .Where(line => !string.IsNullOrWhiteSpace(line)
                                                        && !line.StartsWith("--")
                                                        && !line.StartsWith("//")
                                                        && !line.StartsWith("'"))
                                        .ToList();
                return lines;
            }
        }

        private static void CompareDatabasePair(List<Table> tablesToCompare, DatabasePair databasePair)
        {
            List<string> results = new List<string>();

            SqlConnection connection1 = GetDatabaseConnection(databasePair.Database1);
            SqlConnection connection2 = GetDatabaseConnection(databasePair.Database2);

            foreach (Table table in tablesToCompare)
            {
                DisplayProgressMessage($"Comparing {table.SchemaName}.{table.TableName}...");

                List<string> result = CompareTable(table, connection1, connection2,
                                                    databasePair.Database1.FriendlyName,
                                                    databasePair.Database2.FriendlyName);

                results.AddRange(result);
            }

            WriteToFile(results, databasePair.Database1.FriendlyName, databasePair.Database2.FriendlyName);
        }

        private static void WriteToFile(List<string> results, string friendlyName1, string friendlyName2)
        {
            if (results.Any())
            {
                string fileName = $"{DateForFileName}_{friendlyName1}_{friendlyName2}";
                string fileContents = AppendLines(results);

                WriteToFile(fileName, fileContents, OutputFileExtension.sql);
            }
        }

        private static void WriteToFile(string fileName, string fileContents, OutputFileExtension fileExtension)
        {
            const char backSlash = '\\';
            string directory = $"{CurrentDirectory}{backSlash}{Folder.Outputs}";

            if (!Directory.Exists(directory))
            {
                Directory.CreateDirectory(directory);
            }

            string filePath = $"{directory}{backSlash}{fileName}.{fileExtension}";

            if (File.Exists(filePath))
            {
                File.Delete(filePath);
            }

            using (StreamWriter sw = File.CreateText(filePath))
            {
                sw.Write(fileContents);
            }

            Console.WriteLine($"Wrote file to {filePath}");
        }

        private static List<string> CompareTable(Table table, SqlConnection connection1, SqlConnection connection2,
                                                string friendlyName1, string friendlyName2)
        {
            string queryText = $"SELECT * FROM {table.SchemaName}.{table.TableName}";
            List<string> results = new List<string>();
            DataTable DT1 = null;
            DataTable DT2 = null;

            try
            {
                DisplayProgressMessage($"Retrieving data from {table.SchemaName}.{table.TableName} in {friendlyName1}...");
                DT1 = GetDataTable(connection1, queryText);
            }
            catch (Exception ex)
            {
                results.Add($"--Error for {friendlyName1}: {ex.Message}");
            }

            try
            {
                DisplayProgressMessage($"Retrieving data from {table.SchemaName}.{table.TableName} in {friendlyName2}...");
                DT2 = GetDataTable(connection2, queryText);
            }
            catch (Exception ex)
            {
                results.Add($"--Error for {friendlyName2}: {ex.Message}");
            }

            if (DT1 != null && DT2 != null)
            {
                DisplayProgressMessage("Data retrieval successful!");
                results = CompareDataTables(DT1, DT2, table.SchemaName, table.TableName, friendlyName1, friendlyName2, connection1.Database, connection2.Database);
            }

            return results.Where(r => !string.IsNullOrWhiteSpace(r)).ToList();
        }

        private static SqlConnection GetDatabaseConnection(Database db)
        {
            string connString = $"Data Source={db.ServerName};Initial Catalog={db.DatabaseName};Integrated Security=True;MultipleActiveResultSets=True";

            return new SqlConnection(connString);
        }

        private static DataTable GetDataTable(SqlConnection Conn, string SQL)
        {
            SqlDataAdapter SDA = new SqlDataAdapter(SQL, Conn);
            DataTable DT = new DataTable();

            try
            {
                Conn.Open();
                SDA.Fill(DT);
                Conn.Close();
            }
            catch (Exception)
            {
                throw;
            }
            finally
            {
                Conn.Close();
            }

            return DT;
        }

        private static List<string> GetColumnsToIgnore()
        {
            //TODO: Read from file?

            return new List<string>()
            {
                "UserCreated",
                "DateCreated",
                "UserModified",
                "DateModified",
                "SpatialLocation" //Comparison of geography data type from SQL appears to always return false
            };
        }

        private static List<DataColumn> GetColumns(DataTable DT, bool excludeColumnsToIgnore)
        {
            List<DataColumn> columns = DT.Columns.Cast<DataColumn>().ToList();

            if (excludeColumnsToIgnore)
            {
                List<string> columnsToIgnore = GetColumnsToIgnore();

                columns = columns.Where(dc => !columnsToIgnore.Contains(dc.ColumnName)).ToList();
            }

            return columns;
        }

        private static List<DataRow> GetRows(DataTable DT)
        {
            List<DataRow> Rows = DT.Rows.Cast<DataRow>().ToList();

            return Rows.OrderBy(r => r.ItemArray[0]).ToList(); //assuming the first column is the ID
        }

        private static List<string> CompareDataTables(DataTable dt1, DataTable dt2,
                                                string schemaName, string tableName,
                                                string friendlyName1, string friendlyName2,
                                                string dbName1, string dbName2)
        {
            List<DataColumn> dc1 = GetColumns(dt1, true);
            List<DataColumn> dc2 = GetColumns(dt2, true);

            List<DataRow> dr1 = GetRows(dt1);
            List<DataRow> dr2 = GetRows(dt2);

            string idColumnName = dc1.First().ColumnName;

            List<string> validationErrors = GetValidationErrors(schemaName, tableName, idColumnName, friendlyName1, friendlyName2, dr1, dr2);

            if (validationErrors.Any())
            {
                return validationErrors;
            }
            else
            {
                List<string> validationWarnings = GetValidationWarnings(schemaName, tableName, dc1, dc2, friendlyName1, friendlyName2);

                foreach (DataColumn dc in dc1.Where(x => dc2.All(y => x.ColumnName != y.ColumnName)))
                {
                    dt1.Columns.Remove(dc);
                }

                foreach (DataColumn dc in dc2.Where(x => dc1.All(y => x.ColumnName != y.ColumnName)))
                {
                    dt2.Columns.Remove(dc);
                }

                List<DataColumn> dc1All = GetColumns(dt1, false).ToList();
                List<DataColumn> dc2All = GetColumns(dt2, false).ToList();
                List<string> differencesInIDs = GetDifferencesInIDs(schemaName, tableName, dr1, dr2, friendlyName1, friendlyName2, dc1All, dc2All, dbName1, dbName2);

                dc1 = GetColumns(dt1, true).ToList();
                List<string> differencesForSameIDs = GetDifferencesForSameIDs(schemaName, tableName, dc1, dr1, dr2, friendlyName1, friendlyName2, dbName1, dbName2);

                List<string> results = validationWarnings.Union(differencesInIDs).Union(differencesForSameIDs).ToList();

                return results;
            }
        }

        private static List<string> GetDifferencesInIDs(string schema, string table,
                                                        List<DataRow> dataRows1, List<DataRow> dataRows2,
                                                        string friendlyName1, string friendlyName2,
                                                        List<DataColumn> dc1All, List<DataColumn> dc2All,
                                                        string dbName1, string dbName2)
        {
            DisplayProgressMessage($"Checking for different IDs in {schema}.{table}...");

            string idName = dc1All.First().ColumnName;
            List<ScriptForID> results = new List<ScriptForID>();

            string columnList1 = SeparateWithCommas(dc1All.Select(dc => dc.ColumnName));
            string columnList2 = SeparateWithCommas(dc2All.Select(dc => dc.ColumnName));

            List<DataRow> rowsIn1ButNot2 = dataRows1.Except(dataRows2, new DataRowIDComparer()).ToList();
            results.AddRange(rowsIn1ButNot2.Select(d => GetSelectByID(d, schema, table, friendlyName1, friendlyName2, idName)));
            results.AddRange(rowsIn1ButNot2.Select(v => GetInsertScriptByID(v, dbName2, schema, table, friendlyName2, columnList2))); //insert into db2
            results.AddRange(rowsIn1ButNot2.Select(r => GetDeleteScriptByID(r, dbName1, schema, table, friendlyName1, idName))); //delete from db1

            List<DataRow> rowsIn2ButNot1 = dataRows2.Except(dataRows1, new DataRowIDComparer()).ToList();
            results.AddRange(rowsIn2ButNot1.Select(d => GetSelectByID(d, schema, table, friendlyName2, friendlyName1, idName)));
            results.AddRange(rowsIn2ButNot1.Select(v => GetInsertScriptByID(v, dbName1, schema, table, friendlyName1, columnList1))); //insert into db1
            results.AddRange(rowsIn2ButNot1.Select(r => GetDeleteScriptByID(r, dbName2, schema, table, friendlyName2, idName))); //delete from db2

            return results.OrderBy(r => r.ID).Select(r => r.Script).ToList();
        }

        private static List<string> GetDifferencesForSameIDs(string schemaName, string tableName, List<DataColumn> dataColumns,
                                                            List<DataRow> dataRows1, List<DataRow> dataRows2,
                                                            string friendlyName1, string friendlyName2,
                                                            string dbName1, string dbName2)
        {
            DisplayProgressMessage($"Checking for differences in {schemaName}.{tableName}...");

            List<string> results = new List<string>();

            //this assumes that the first column is the int ID column
            //Find the set of rows with IDs that are common between the two sets.
            List<DataRow> RowsWithSameIDs = dataRows1.Intersect(dataRows2, new DataRowIDComparer()).ToList();

            //Find all the rows from the first set that are in the common set
            //Find all the rows from the second set that are in the common set
            //Find all the rows that have IDs in the common set, but have different column values
            List<DataRow> RowsWithSameIDsButDifferentValues = dataRows1
                                                                .Intersect(RowsWithSameIDs, new DataRowIDComparer())
                                                                .Except
                                                                        (
                                                                            dataRows2.Intersect(RowsWithSameIDs, new DataRowIDComparer())
                                                                            , new DataRowComparer(dataColumns)
                                                                        )
                                                                .ToList();

            foreach (DataRow DR in RowsWithSameIDsButDifferentValues)
            {
                results.AddRange(GetColumnsWithDifferences(schemaName, tableName, dataColumns, dataRows1, dataRows2, friendlyName1, friendlyName2, DR, dbName1, dbName2));
            }

            return results;
        }

        private static List<string> GetColumnsWithDifferences(string schema, string table, List<DataColumn> dataColumns,
                                                            List<DataRow> dataRows1, List<DataRow> dataRows2,
                                                            string friendlyName1, string friendlyName2, DataRow DR,
                                                            string dbName1, string dbName2)
        {
            DataRow DR1 = dataRows1.Single(dr1 => int.Parse(dr1.ItemArray[0].ToString()) == int.Parse(DR.ItemArray[0].ToString()));
            DataRow DR2 = dataRows2.Single(dr2 => int.Parse(dr2.ItemArray[0].ToString()) == int.Parse(DR.ItemArray[0].ToString()));

            string idName = dataColumns.First().ColumnName;

            return dataColumns.Where(dc => !DR1[dc.ColumnName].Equals(DR2[dc.ColumnName]))
                                .Select(dataColumn => GetColumnDifference(schema, table, friendlyName1, friendlyName2, dataColumn, DR1, DR2, idName, dbName1, dbName2))
                                .ToList();
        }

        private static string GetColumnDifference(string schema, string table, string friendlyName1, string friendlyName2,
                                                    DataColumn dataColumn, DataRow DR1, DataRow DR2, string idName,
                                                    string dbName1, string dbName2)
        {
            string column = dataColumn.ColumnName;
            int ID = int.Parse(DR1.ItemArray[0].ToString());

            string value1 = $"'{DR1[dataColumn.ColumnName]}'";
            string value2 = $"'{DR2[dataColumn.ColumnName]}'";

            string select = $"SELECT * FROM {schema}.{table} WHERE {idName} = {ID} --{column} = {value1} in {friendlyName1} but {value2} in {friendlyName2}.";
            string update1 = $"--UPDATE {dbName1}.{schema}.{table} SET {column} = {value2} WHERE {idName} = {ID} --update {friendlyName1}.";
            string update2 = $"--UPDATE {dbName2}.{schema}.{table} SET {column} = {value1} WHERE {idName} = {ID} --update {friendlyName2}.";

            return $"{select}{Environment.NewLine}{update1}{Environment.NewLine}{update2}";
        }

        private static List<string> GetValidationErrors(string schema, string table, string idName,
                                                        string friendlyName1, string friendlyName2,
                                                        List<DataRow> dataRows1, List<DataRow> dataRows2)
        {
            DisplayProgressMessage($"Checking {schema}.{table} for validation errors...");

            List<string> results = new List<string>();

            //Make sure ID is an int
            int output;
            results.AddRange(dataRows1.Where(r => !int.TryParse(r.ItemArray[0].ToString(), out output))
                                        .Select(d => $"SELECT * FROM {schema}.{table} WHERE {idName} = '{d.ItemArray[0]}' --ID is not an int in {friendlyName1} (table not compared)"));
            results.AddRange(dataRows2.Where(r => !int.TryParse(r.ItemArray[0].ToString(), out output))
                                        .Select(d => $"SELECT * FROM {schema}.{table} WHERE {idName} = '{d.ItemArray[0]}' --ID is not an int in {friendlyName2} (table not compared)"));

            //Check for duplicate ID values
            results.AddRange(dataRows1.GroupBy(r => r.ItemArray[0]).Where(g => g.Count() > 1)
                                        .Select(d => $"SELECT * FROM {schema}.{table} WHERE {idName} = {d.Key} --Duplicate ID in {friendlyName1} (table not compared)"));
            results.AddRange(dataRows2.GroupBy(r => r.ItemArray[0]).Where(g => g.Count() > 1)
                                        .Select(d => $"SELECT * FROM {schema}.{table} WHERE {idName} = {d.Key} --Duplicate ID in {friendlyName2} (table not compared)"));

            return results;
        }

        private static List<string> GetValidationWarnings(string schema, string table,
                                                        List<DataColumn> dc1, List<DataColumn> dc2,
                                                        string friendlyName1, string friendlyName2)
        {
            DisplayProgressMessage($"Checking {schema}.{table} for validation warnings...");

            List<string> results = new List<string>();

            results.AddRange(dc1.Where(x => dc2.All(y => y.ColumnName != x.ColumnName))
                                .Select(dc => $"SELECT * FROM {schema}.{table} --{dc.ColumnName} column is in {friendlyName1} but not in {friendlyName2}."));

            results.AddRange(dc2.Where(x => dc1.All(y => y.ColumnName != x.ColumnName))
                                .Select(dc => $"SELECT * FROM {schema}.{table} --{dc.ColumnName} column is in {friendlyName2} but not in {friendlyName1}."));

            string dataTypesResult = CheckForDifferentDataTypes(schema, table, dc1, dc2);

            if (string.IsNullOrWhiteSpace(dataTypesResult))
            {
                results.Add(dataTypesResult);
            }

            return results;
        }

        private static string CheckForDifferentDataTypes(string schema, string table,
                                                        List<DataColumn> dc1, List<DataColumn> dc2)
        {
            //TODO: Determine whether different data types even matter

            DisplayProgressMessage($"Checking for different data types in {schema}.{table}...");
            string result = string.Empty;

            List<string> differentDataTypes = dc1.Where(x => dc2.Any(y => y.ColumnName == x.ColumnName && y.DataType != x.DataType))
                                                .Select(x => x.ColumnName)
                                                .ToList();

            if (differentDataTypes.Any())
            {
                string columnsWithDifferentDataTypes = SeparateWithCommas(differentDataTypes);
                string plural = columnsWithDifferentDataTypes.Length == 1 ? "" : "s";
                result = $"{schema}.{table} --Column{plural} with different data type{plural}: {columnsWithDifferentDataTypes}";
            }

            return result;
        }

        private static void DisplayProgressMessage(string Message, bool ClearScreen = false)
        {
            if (ClearScreen)
            {
                Console.Clear();
                Console.WriteLine("Console Cleared");
            }

            Console.WriteLine(Message);
        }

        private static string AppendLines(IEnumerable<string> input)
        {
            return input.Aggregate(new StringBuilder(), (current, next) => current.AppendLine(next)).ToString();
        }

        private static string SeparateWithCommas(IEnumerable<string> input)
        {
            return input.Aggregate((current, next) => $"{current}, {next}");
        }

        private static ScriptForID GetSelectByID(DataRow dr, string schema, string table, string friendlyNameIn, string friendlyNameNotIn, string idName)
        {
            int id = (int)dr.ItemArray[0];
            string select = $"SELECT * FROM {schema}.{table}";

            return new ScriptForID(id, $"{select} WHERE {idName} = {id} --in {friendlyNameIn} but not in {friendlyNameNotIn}.");
        }

        private static ScriptForID GetInsertScriptByID(DataRow dr, string dbName, string schema, string table, string friendlyName, string columnList)
        {
            int id = (int)dr.ItemArray[0];
            //TODO: Handle tables without identity
            string identityOn = $"SET IDENTITY_INSERT {schema}.{table} ON";
            string insertInto = $"INSERT INTO {dbName}.{schema}.{table}({columnList})";
            string identityOff = $"SET IDENTITY_INSERT {schema}.{table} OFF";
            string values = dr.ItemArray.Select(i => i.ToString())
                                .Aggregate((current, next) => $"{current}, '{next}'");

            return new ScriptForID(id, $"--{identityOn} {insertInto} VALUES({values}) {identityOff} --Insert into {friendlyName}");
        }

        private static ScriptForID GetDeleteScriptByID(DataRow dr, string dbName, string schema, string table, string friendlyName, string idName)
        {
            int id = (int)dr.ItemArray[0];

            return new ScriptForID(id, $"--DELETE FROM {dbName}.{schema}.{table} WHERE {idName} = {id} --Delete from {friendlyName}");
        }

        #endregion

        #region Properties

        //In LINQPad: private static string CurrentDirectory => Path.GetDirectoryName(Util.CurrentQueryPath);
        private static string CurrentDirectory => Directory.GetCurrentDirectory();  //bin\Debug

        private static string DateForFileName => DateTime.Today.ToString("yyyyMMdd");

        #endregion

        #region Classes

        private class Database
        {
            public string FriendlyName { get; }
            public string ServerName { get; }
            public string DatabaseName { get; }

            public Database(string friendlyName, string serverName, string databaseName)
            {
                FriendlyName = friendlyName;
                ServerName = serverName;
                DatabaseName = databaseName;
            }
        }

        private class DatabasePair
        {
            public Database Database1 { get; }
            public Database Database2 { get; }

            public DatabasePair(Database database1, Database database2)
            {
                Database1 = database1;
                Database2 = database2;
            }
        }

        private class Table
        {
            public string SchemaName { get; }
            public string TableName { get; }

            public Table(string schemaName, string tableName)
            {
                SchemaName = schemaName;
                TableName = tableName;
            }
        }

        private class TableFileResult
        {
            public List<Table> Tables { get; }
            public List<string> Errors { get; }

            public TableFileResult(List<Table> tables, List<string> errors)
            {
                Tables = tables;
                Errors = errors;
            }
        }

        private class DatabaseFileResult
        {
            public List<DatabasePair> DatabasePairs { get; }
            public List<string> Errors { get; }

            public DatabaseFileResult(List<DatabasePair> databasePairs, List<string> errors)
            {
                DatabasePairs = databasePairs;
                Errors = errors;
            }
        }

        private class ScriptForID
        {
            public int ID { get; }
            public string Script { get; }

            public ScriptForID(int id, string script)
            {
                ID = id;
                Script = script;
            }
        }

        private class DataRowComparer : IEqualityComparer<DataRow>
        {
            private readonly List<DataColumn> dataColumns;

            public DataRowComparer(List<DataColumn> DataColumns)
            {
                dataColumns = DataColumns;
            }

            public bool Equals(DataRow DR1, DataRow DR2)
            {
                return (int)DR1.ItemArray[0] == (int)DR2.ItemArray[0] && dataColumns.All(dc => DR2[dc.ColumnName].Equals(DR1[dc.ColumnName]));
            }

            public int GetHashCode(DataRow DR)
            {
                // Check whether the object is null. 
                if (ReferenceEquals(DR, null))
                {
                    return 0;
                }

                //This is from https://msdn.microsoft.com/en-us/library/bb336390(v=vs.90).aspx
                //I'm not at all sure that it is correct here.
                //It looks like more reading can be done here if this is determined to be stupid:
                //https://msdn.microsoft.com/en-us/library/system.object.gethashcode(v=vs.110).aspx

                return dataColumns.Aggregate(0, (current, dataColumn) => current ^ DR[dataColumn.ColumnName].GetHashCode());
            }
        }

        private class DataRowIDComparer : IEqualityComparer<DataRow>
        {
            public bool Equals(DataRow DR1, DataRow DR2)
            {
                return int.Parse(DR1.ItemArray[0].ToString()) == int.Parse(DR2.ItemArray[0].ToString());
            }

            public int GetHashCode(DataRow DR)
            {
                // Check whether the object is null. 
                return ReferenceEquals(DR, null) ? 0 : DR.ItemArray[0].GetHashCode();
            }
        }

        #endregion

        #region Enums

        private enum DatabasePairPart
        {
            FriendlyName1,
            ServerName1,
            DatabaseName1,
            FriendlyName2,
            ServerName2,
            DatabaseName2
        }

        private enum TablePart
        {
            SchemaName,
            TableName
        }

        private enum InputFile
        {
            TablesToCompare,
            DatabasePairs
        }

        private enum OutputFileExtension
        {
            sql,
            txt
        }

        private enum Folder
        {
            Inputs,
            Outputs
        }

        #endregion

    }
}
