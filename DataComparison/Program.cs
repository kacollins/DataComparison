using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;

namespace DataComparison
{
    class Program
    {
        public static List<string> ColumnsToIgnore { get; private set; }

        static void Main(string[] args)
        {
            string silentModeFlag = args.Length > (int)Argument.SilentModeFlag ? args[(int)Argument.SilentModeFlag] : string.Empty;
            bool silentMode = GetSilentMode(silentModeFlag);

            string tableFileName = args.Length > (int)Argument.TableFileName ? args[(int)Argument.TableFileName] : string.Empty;
            string databaseFileName = args.Length > (int)Argument.DatabaseFileName ? args[(int)Argument.DatabaseFileName] : string.Empty;
            string columnFileName = args.Length > (int)Argument.ColumnFileName ? args[(int)Argument.ColumnFileName] : string.Empty;

            GetColumnsToIgnore(columnFileName);
            CompareTables(tableFileName, databaseFileName);

            if (!silentMode)
            {
                Console.WriteLine("Press enter to exit:");
                Console.Read();
            }
        }

        private static bool GetSilentMode(string silentModeFlag)
        {
            bool silentMode;

            if (!bool.TryParse(silentModeFlag, out silentMode))
            {
                const int silentModeOn = 1;
                silentMode = silentModeFlag == silentModeOn.ToString();
            }

            return silentMode;
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

            List<string> errorMessages = GetFileErrors(lines, separator, Enum.GetValues(typeof(TablePart)).Length, "schema/table format");

            if (errorMessages.Any())
            {
                Console.WriteLine($"Error: Invalid schema/table format in {InputFile.TablesToCompare} file.");
            }

            TableFileResult result = new TableFileResult(tablesToCompare, errorMessages);

            return result;
        }

        private static List<string> GetFileErrors(List<string> fileLines, char separator, int length, string description)
        {
            List<string> errorMessages = fileLines.Where(line => line.Split(separator).Length != length)
                                                    .Select(invalidLine => $"Invalid {description}: {invalidLine}")
                                                    .ToList();

            return errorMessages;
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

            List<string> errorMessages = GetFileErrors(lines, separator, Enum.GetValues(typeof(DatabasePairPart)).Length, "database pair format");

            if (errorMessages.Any())
            {
                Console.WriteLine($"Error: Invalid database pair format in {InputFile.DatabasePairs} file.");
            }

            DatabaseFileResult result = new DatabaseFileResult(databasePairs, errorMessages);

            return result;
        }

        private static List<string> GetFileLines(string fileName)
        {
            List<string> fileLines = new List<string>();

            const char backSlash = '\\';
            DirectoryInfo directoryInfo = new DirectoryInfo($"{CurrentDirectory}{backSlash}{Folder.Inputs}");

            if (directoryInfo.Exists)
            {
                FileInfo file = directoryInfo.GetFiles(fileName).FirstOrDefault();

                if (file == null)
                {
                    Console.WriteLine($"File does not exist: {directoryInfo.FullName}{backSlash}{fileName}");
                }
                else
                {
                    fileLines = File.ReadAllLines(file.FullName)
                                            .Where(line => !string.IsNullOrWhiteSpace(line)
                                                            && !line.StartsWith("--")
                                                            && !line.StartsWith("//")
                                                            && !line.StartsWith("'"))
                                            .ToList();
                }
            }
            else
            {
                Console.WriteLine($"Directory does not exist: {directoryInfo.FullName}");
            }

            return fileLines;
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

            //If table exists in one database but not the other, generate insert scripts for all rows
            if (DT1 == null && DT2 != null)
            {
                DT1 = CreateDataTable(DT2);
            }
            else if (DT1 != null && DT2 == null)
            {
                DT2 = CreateDataTable(DT1);
            }

            if (DT1 != null && DT2 != null)
            {
                DisplayProgressMessage("Data retrieval successful!");
                results = CompareDataTables(DT1, DT2, table.SchemaName, table.TableName, friendlyName1, friendlyName2, connection1.Database, connection2.Database);
            }

            return results.Where(r => !string.IsNullOrWhiteSpace(r)).ToList();
        }

        private static DataTable CreateDataTable(DataTable dtSource)
        {
            DataTable dtDest = new DataTable();

            foreach (DataColumn column in GetColumns(dtSource, true))
            {
                dtDest.Columns.Add(column);
            }

            return dtDest;
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
            }
            finally
            {
                Conn.Close();
            }

            return DT;
        }

        private static void GetColumnsToIgnore(string fileName)
        {
            if (string.IsNullOrWhiteSpace(fileName))
            {
                fileName = $"{InputFile.ColumnsToIgnore}.supersecret";
            }

            List<string> lines = GetFileLines(fileName);

            if (lines.Any())
            {
                ColumnsToIgnore = lines;
            }
            else
            {
                ColumnsToIgnore = new List<string>
                {
                    "UserCreated",
                    "DateCreated",
                    "UserModified",
                    "DateModified",
                    "SpatialLocation" //Comparison of geography data type from SQL appears to always return false
                };
            }
        }

        private static List<DataColumn> GetColumns(DataTable DT, bool excludeColumnsToIgnore)
        {
            List<DataColumn> columns = DT.Columns.Cast<DataColumn>().ToList();

            if (excludeColumnsToIgnore)
            {
                columns = columns.Where(dc => !ColumnsToIgnore.Contains(dc.ColumnName)).ToList();
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
            string idColumnName = dt1.Columns[0].ColumnName;
            List<DataRow> dr1 = GetRows(dt1);
            List<DataRow> dr2 = GetRows(dt2);

            List<string> validationErrors = GetValidationErrors(schemaName, tableName, idColumnName, friendlyName1, friendlyName2, dr1, dr2);

            if (validationErrors.Any())
            {
                return validationErrors;
            }

            List<DataColumn> dc1 = GetColumns(dt1, true);
            List<DataColumn> dc2 = GetColumns(dt2, true);
            List<DataColumn> dc1All = GetColumns(dt1, false);
            List<DataColumn> dc2All = GetColumns(dt2, false);

            List<string> validationWarnings = GetValidationWarnings(schemaName, tableName, dc1, dc2, friendlyName1, friendlyName2);

            foreach (DataColumn dc in dc1.Where(x => dc2.All(y => x.ColumnName != y.ColumnName)))
            {
                dt1.Columns.Remove(dc);
            }

            foreach (DataColumn dc in dc2.Where(x => dc1.All(y => x.ColumnName != y.ColumnName)))
            {
                dt2.Columns.Remove(dc);
            }

            List<string> differencesInIDs = GetDifferencesInIDs(schemaName, tableName, dr1, dr2, friendlyName1, friendlyName2, dc1All, dc2All, dbName1, dbName2);

            dc1 = GetColumns(dt1, true).ToList();
            List<string> differencesForSameIDs = GetDifferencesForSameIDs(schemaName, tableName, dc1, dr1, dr2, friendlyName1, friendlyName2, dbName1, dbName2);

            return validationWarnings.Union(differencesInIDs).Union(differencesForSameIDs).ToList();
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

            List<ScriptForID> resultsForRowsNotIn2 = GetResultsForMissingRows(schema, table, dataRows1, dataRows2, friendlyName1, friendlyName2,
                                                                                dbName1, dbName2, idName, columnList2);
            results.AddRange(resultsForRowsNotIn2);

            List<ScriptForID> resultsForRowsNotIn1 = GetResultsForMissingRows(schema, table, dataRows2, dataRows1, friendlyName2, friendlyName1,
                                                                                dbName2, dbName1, idName, columnList1);
            results.AddRange(resultsForRowsNotIn1);

            return results.OrderBy(r => r.ID).Select(r => r.Script).ToList();
        }

        private static List<ScriptForID> GetResultsForMissingRows(string schema, string table, List<DataRow> dataRowsSource, List<DataRow> dataRowsDest,
                                                                    string friendlyNameSource, string friendlyNameDest,
                                                                    string dbNameSource, string dbNameDest, string idName, string columnListDest)
        {
            List<ScriptForID> results = new List<ScriptForID>();

            List<DataRow> missingRows = dataRowsSource.Except(dataRowsDest, new DataRowIDComparer()).ToList();
            results.AddRange(missingRows.Select(d => GetSelectByID(d, schema, table, friendlyNameSource, friendlyNameDest, idName)));
            results.AddRange(missingRows.Select(v => GetInsertScriptByID(v, dbNameDest, schema, table, friendlyNameDest, columnListDest)));
            results.AddRange(missingRows.Select(r => GetDeleteScriptByID(r, dbNameSource, schema, table, friendlyNameSource, idName)));

            return results;
        }

        private static List<string> GetDifferencesForSameIDs(string schemaName, string tableName, List<DataColumn> dataColumns,
                                                            List<DataRow> dataRows1, List<DataRow> dataRows2,
                                                            string friendlyName1, string friendlyName2,
                                                            string dbName1, string dbName2)
        {
            DisplayProgressMessage($"Checking for differences in {schemaName}.{tableName}...");

            List<string> results = new List<string>();

            //Get rows in dr1 where the ID exists in dr2
            List<DataRow> dr1WithCommonIDs = dataRows1.Intersect(dataRows2, new DataRowIDComparer()).ToList();

            //Get rows in dr1 where the ID exists in dr2 but where the records have different column values
            List<DataRow> dr1WithCommonIDsButDifferentValues = dr1WithCommonIDs
                                                                .Except(dataRows2, new DataRowComparer(dataColumns))
                                                                .ToList();

            List<int> idsWithDifferentValues = dr1WithCommonIDsButDifferentValues.Select(GetID).ToList();

            foreach (int id in idsWithDifferentValues)
            {
                results.AddRange(GetColumnsWithDifferences(schemaName, tableName, dataColumns, dataRows1, dataRows2,
                                                            friendlyName1, friendlyName2, id, dbName1, dbName2));
            }

            return results;
        }

        private static List<string> GetColumnsWithDifferences(string schema, string table, List<DataColumn> dataColumns,
                                                            List<DataRow> dataRows1, List<DataRow> dataRows2,
                                                            string friendlyName1, string friendlyName2, int id,
                                                            string dbName1, string dbName2)
        {
            DataRow DR1 = dataRows1.Single(dr1 => GetID(dr1) == id);
            DataRow DR2 = dataRows2.Single(dr2 => GetID(dr2) == id);

            string idName = dataColumns.First().ColumnName;

            return dataColumns.Where(dc => !DR1[dc.ColumnName].Equals(DR2[dc.ColumnName]))
                                .Select(dataColumn => GetColumnDifference(schema, table, friendlyName1, friendlyName2,
                                                                            dataColumn, DR1, DR2, idName, dbName1, dbName2))
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

            string select = GetSelectForUpdate(schema, table, friendlyName1, friendlyName2, idName, dbName1, dbName2, ID, column, value1, value2);

            string update1 = GetUpdateScript(schema, table, friendlyName1, friendlyName2, idName, dbName1, column, value2, ID);
            string update2 = GetUpdateScript(schema, table, friendlyName2, friendlyName1, idName, dbName2, column, value1, ID);

            return $@"{Environment.NewLine}{select}{
                        Environment.NewLine}{update1}{
                        Environment.NewLine}{update2}";
        }

        private static string GetSelectForUpdate(string schema, string table, string friendlyName1, string friendlyName2,
                                                string idName, string dbName1, string dbName2, int ID,
                                                string column, string value1, string value2)
        {
            string select;
            string select1 = $"SELECT * FROM {dbName1}.{schema}.{table} WHERE {idName} = {ID}";
            string select2 = $"SELECT * FROM {dbName2}.{schema}.{table} WHERE {idName} = {ID}";

            if (select1 == select2)
            {
                select = select1;
            }
            else
            {
                select = $@"{select1} --{friendlyName1}{Environment.NewLine}{
                            select2} --{friendlyName2}";
            }

            string valueComment1 = $"--{column} = {value1} in {friendlyName1}";
            string valueComment2 = $"--{column} = {value2} in {friendlyName2}";

            select = $@"{select}{Environment.NewLine}{
                valueComment1}{Environment.NewLine}{
                valueComment2}";

            return select;
        }

        private static string GetUpdateScript(string schema, string table, string friendlyNameDest, string friendlyNameSource,
                                                string idName, string dbNameDest, string column, string valueSource, int ID)
        {
            string updateComment = $"--Execute this script against {friendlyNameDest} to update it to match {friendlyNameSource}:";

            //TODO: Set UserModified and DateModified if those columns exist in the table

            string update = $@"{updateComment}{Environment.NewLine}/*{
                Environment.NewLine}UPDATE {dbNameDest}.{schema}.{table}{
                Environment.NewLine}SET {column} = {valueSource}{
                Environment.NewLine}WHERE {idName} = {ID}{
                Environment.NewLine}*/";

            return update;
        }

        private static List<string> GetValidationErrors(string schema, string table, string idName,
                                                        string friendlyName1, string friendlyName2,
                                                        List<DataRow> dataRows1, List<DataRow> dataRows2)
        {
            DisplayProgressMessage($"Checking {schema}.{table} for validation errors...");

            List<string> results = new List<string>();

            results.AddRange(GetValidationErrors(schema, table, idName, friendlyName1, dataRows1));
            results.AddRange(GetValidationErrors(schema, table, idName, friendlyName2, dataRows2));

            return results;
        }

        private static List<string> GetValidationErrors(string schema, string table, string idName, string friendlyName, List<DataRow> dataRows)
        {
            List<string> results = new List<string>();

            //Make sure ID is an int
            int output;
            results.AddRange(dataRows.Where(r => !int.TryParse(r.ItemArray[0].ToString(), out output))
                .Select(d => GetSelectForError(schema, table, idName, d.ItemArray[0].ToString(),
                    $"ID is not an int in {friendlyName}")));

            //Check for duplicate ID values
            results.AddRange(dataRows.GroupBy(r => r.ItemArray[0]).Where(g => g.Count() > 1)
                .Select(d => GetSelectForError(schema, table, idName, d.Key.ToString(),
                    $"Duplicate ID in {friendlyName}")));

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

        private static string GetSelectForError(string schema, string table, string idName, string id, string error)
        {
            return $@"SELECT * FROM {schema}.{table} WHERE {idName} = '{id}' --{error} (table not compared)";
        }

        private static int GetID(DataRow dr)
        {
            return int.Parse(dr.ItemArray[0].ToString());
        }

        private static ScriptForID GetSelectByID(DataRow dr, string schema, string table, string friendlyNameIn, string friendlyNameNotIn, string idName)
        {
            int id = GetID(dr);
            string select = $"SELECT * FROM {schema}.{table}";

            return new ScriptForID(id, $"{select} WHERE {idName} = {id} --in {friendlyNameIn} but not in {friendlyNameNotIn}.");
        }

        private static ScriptForID GetInsertScriptByID(DataRow dr, string dbName, string schema, string table, string friendlyName, string columnList)
        {
            int id = GetID(dr);
            //TODO: Handle tables without identity specification? would need to add a flag to the input file
            string identityOn = $"SET IDENTITY_INSERT {schema}.{table} ON";
            string insertInto = $"INSERT INTO {dbName}.{schema}.{table}({columnList})";
            string identityOff = $"SET IDENTITY_INSERT {schema}.{table} OFF";
            string values = dr.ItemArray.Select(i => i.ToString())
                                .Aggregate((current, next) => $"{current}, '{next}'");

            return new ScriptForID(id, $"--{identityOn} {insertInto} VALUES({values}) {identityOff} --Insert into {friendlyName}");
        }

        private static ScriptForID GetDeleteScriptByID(DataRow dr, string dbName, string schema, string table, string friendlyName, string idName)
        {
            int id = GetID(dr);

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
                return dataColumns.All
                                        (
                                            dc => DR2[dc.ColumnName].Equals(DR1[dc.ColumnName])
                                            ||
                                            (DR2[dc.ColumnName].GetType().IsArray && CompareArray((Array)DR1[dc.ColumnName], DR2[dc.ColumnName] as Array))
                                        );
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

                return dataColumns.Aggregate(0, (current, dataColumn) =>
                                                                            !DR[dataColumn.ColumnName].GetType().IsArray
                                                                                ? current ^ DR[dataColumn.ColumnName].GetHashCode()
                                                                                : current ^ ((IStructuralEquatable)DR[dataColumn.ColumnName]).GetHashCode(EqualityComparer<object>.Default));
            }

            private static bool AreElementEqual(object a, object b)
            {
                if (ReferenceEquals(a, b))
                {   // same reference or (null, null) or (DBNull.Value, DBNull.Value)
                    return true;
                }
                if (ReferenceEquals(a, null) || ReferenceEquals(a, DBNull.Value) ||
                    ReferenceEquals(b, null) || ReferenceEquals(b, DBNull.Value))
                {   // (null, non-null) or (null, DBNull.Value) or vice versa
                    return false;
                }
                return a.Equals(b);
            }

            private static bool CompareArray(Array a, Array b)
            {
                if ((null == b) ||
                    (1 != a.Rank) ||
                    (1 != b.Rank) ||
                    (a.Length != b.Length))
                {   // automatically consider array's with Rank>1 not-equal
                    return false;
                }

                int index1 = a.GetLowerBound(0);
                int index2 = b.GetLowerBound(0);
                if (a.GetType() == b.GetType() && (0 == index1) && (0 == index2))
                {
                    switch (Type.GetTypeCode(a.GetType().GetElementType()))
                    {
                        case TypeCode.Byte:
                            return CompareEquatableArray((byte[])a, (byte[])b);
                        case TypeCode.Int16:
                            return CompareEquatableArray((short[])a, (short[])b);
                        case TypeCode.Int32:
                            return CompareEquatableArray((int[])a, (int[])b);
                        case TypeCode.Int64:
                            return CompareEquatableArray((long[])a, (long[])b);
                        case TypeCode.String:
                            return CompareEquatableArray((string[])a, (string[])b);
                    }
                }

                //Compare every element. But don't recurse if we have Array of array.
                int length = index1 + a.Length;
                for (; index1 < length; ++index1, ++index2)
                {
                    if (!AreElementEqual(a.GetValue(index1), b.GetValue(index2)))
                    {
                        return false;
                    }
                }
                return true;
            }

            private static bool CompareEquatableArray<TElem>(TElem[] a, TElem[] b) where TElem : IEquatable<TElem>
            {
                if (ReferenceEquals(a, b))
                {
                    return true;
                }
                if (ReferenceEquals(a, null) ||
                    ReferenceEquals(b, null))
                {
                    return false;
                }
                if (a.Length != b.Length)
                {
                    return false;
                }

                return !a.Where((t, i) => !t.Equals(b[i])).Any();
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
            DatabasePairs,
            ColumnsToIgnore
        }

        private enum Argument
        {
            SilentModeFlag,
            TableFileName,
            DatabaseFileName,
            ColumnFileName
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
