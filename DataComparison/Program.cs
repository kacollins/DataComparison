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
            List<Table> tablesToCompare = GetTablesToCompare();

            if (tablesToCompare.Any())
            {
                List<DatabasePair> databasesToCompare = GetDatabasesToCompare();

                if (databasesToCompare.Any())
                {
                    foreach (DatabasePair databasePair in databasesToCompare)
                    {
                        DisplayProgressMessage($"Comparing {databasePair.Database1.FriendlyName} to {databasePair.Database2.FriendlyName}...");
                        CompareDatabases(tablesToCompare, databasePair);
                    }

                    Console.WriteLine("Done!");
                }
                else
                {
                    Console.WriteLine("No databases to compare!");
                }
            }
            else
            {
                Console.WriteLine("No tables to compare!");
            }

            Console.WriteLine("Press enter to exit:");
            Console.Read();
        }

        #region Methods

        private static List<Table> GetTablesToCompare()
        {
            List<string> lines = GetFileLines("TablesToCompare.supersecret");

            List<Table> tablesToCompare = new List<Table>();

            foreach (string[] parts in lines.Select(line => line.Split('.')))
            {
                if (parts.Length == Enum.GetValues(typeof(TablePart)).Length)
                {
                    tablesToCompare.Add(new Table(parts[(int)TablePart.SchemaName],
                                                parts[(int)TablePart.TableName]));
                }
                else
                {
                    Console.WriteLine("Error: Invalid schema/table name in TablesToCompare file.");
                }
            }

            //Call SP in LINQPad
            //List<Table> tablesToCompare = usp_GetLookups().Tables[0].AsEnumerable()
            //                                        .Select(dr => new Table(dr["SchemaName"].ToString(),
            //                                                                dr["TableName"].ToString()))
            //                                        .ToList();

            return tablesToCompare;
        }

        private static List<DatabasePair> GetDatabasesToCompare()
        {
            List<DatabasePair> databasePairs = new List<DatabasePair>();
            List<string> lines = GetFileLines("DatabasePairs.supersecret");

            foreach (string[] parts in lines.Select(line => line.Split(',')))
            {
                if (parts.Length == Enum.GetValues(typeof(DatabasePairPart)).Length)
                {
                    databasePairs.Add(new DatabasePair(new Database(parts[(int)DatabasePairPart.FriendlyName1],
                                                                    parts[(int)DatabasePairPart.ServerName1],
                                                                    parts[(int)DatabasePairPart.DatabaseName1]),
                                                        new Database(parts[(int)DatabasePairPart.FriendlyName2],
                                                                    parts[(int)DatabasePairPart.ServerName2],
                                                                    parts[(int)DatabasePairPart.DatabaseName2])));
                }
                else
                {
                    Console.WriteLine("Error: Invalid database pair in DatabasePairs file.");
                }
            }

            return databasePairs;
        }

        private static List<string> GetFileLines(string fileName)
        {
            DirectoryInfo directoryInfo = new DirectoryInfo(CurrentDirectory);
            FileInfo file = directoryInfo.GetFiles(fileName).FirstOrDefault();

            if (file == null)
            {
                return new List<string>();
            }
            else
            {
                List<string> lines = File.ReadAllLines(file.FullName)
                                        .Where(x => !string.IsNullOrWhiteSpace(x))
                                        .ToList();
                return lines;
            }
        }

        private static void CompareDatabases(List<Table> tablesToCompare, DatabasePair databasePair)
        {
            List<string> results = new List<string>();

            SqlConnection connection1 = GetDatabaseConnection(databasePair.Database1);
            SqlConnection connection2 = GetDatabaseConnection(databasePair.Database2);

            foreach (Table table in tablesToCompare)
            {
                DisplayProgressMessage($"Comparing {table.SchemaName}.{table.TableName}...");

                string result = CompareTable(table, connection1, connection2,
                                            databasePair.Database1.FriendlyName,
                                            databasePair.Database2.FriendlyName);

                if (!string.IsNullOrWhiteSpace(result))
                {
                    results.Add(result);
                }
            }

            WriteToFile(results, databasePair.Database1.FriendlyName, databasePair.Database2.FriendlyName);
        }

        private static void WriteToFile(List<string> results, string friendlyName1, string friendlyName2)
        {
            if (results.Any())
            {
                string fileName = $"{DateTime.Today.ToString("yyyyMMdd")}_{friendlyName1}_{friendlyName2}";
                string fileContents = results.Aggregate((current, next) => current + Environment.NewLine + next);

                const char backSlash = '\\';
                string directory = $"{CurrentDirectory}{backSlash}Results";

                if (!Directory.Exists(directory))
                {
                    Directory.CreateDirectory(directory);
                }

                string filePath = $"{directory}{backSlash}{fileName}.txt";

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
        }

        private static string CompareTable(Table table, SqlConnection connection1, SqlConnection connection2,
                                            string friendlyName1, string friendlyName2)
        {
            string queryText = $"SELECT * FROM [{table.SchemaName}].[{table.TableName}]";
            string results = string.Empty;
            DataTable DT1 = null;
            DataTable DT2 = null;

            try
            {
                DisplayProgressMessage($"Retrieving data from {table.SchemaName}.{table.TableName} in {friendlyName1}...");
                DT1 = GetDataTable(connection1, queryText);
            }
            catch (Exception ex)
            {
                results = $"Error for {friendlyName1}: {ex.Message}";
            }

            try
            {
                DisplayProgressMessage($"Retrieving data from {table.SchemaName}.{table.TableName} in {friendlyName2}...");
                DT2 = GetDataTable(connection2, queryText);
            }
            catch (Exception ex)
            {
                results = $"Error for {friendlyName2}: {ex.Message}";
            }

            if (DT1 != null && DT2 != null)
            {
                DisplayProgressMessage("Data retrieval sucessfull!");
                results = CompareDataTables(DT1, DT2, table.SchemaName, table.TableName, friendlyName1, friendlyName2).Trim();
            }

            return results;
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
            return new List<string>() { "UserCreated", "DateCreated", "UserModified", "DateModified", "SpatialLocation" }; //Comparison of geography data type from SQL appears to always return false
        }

        private static List<DataColumn> GetColumns(DataTable DT)
        {
            List<string> columnsToIgnore = GetColumnsToIgnore();
            return DT.Columns.Cast<DataColumn>().Where(dc => !columnsToIgnore.Contains(dc.ColumnName)).ToList();
        }

        private static List<DataRow> GetRows(DataTable DT)
        {
            List<DataRow> Rows = DT.Rows.Cast<DataRow>().ToList();

            return Rows.OrderBy(x => x.ItemArray[0]).ToList(); //assuming the first column is the ID
        }

        private static string CompareDataTables(DataTable dt1, DataTable dt2,
                                                string schemaName, string tableName,
                                                string friendlyName1, string friendlyName2)
        {
            List<DataColumn> dc1 = GetColumns(dt1);
            List<DataColumn> dc2 = GetColumns(dt2);

            List<DataRow> dr1 = GetRows(dt1);
            List<DataRow> dr2 = GetRows(dt2);

            string validationErrors = GetValidationErrors(schemaName, tableName, dc1, dc2, friendlyName1, friendlyName2);

            foreach (DataColumn dc in dc1.Where(x => dc2.All(y => x.ColumnName != y.ColumnName)))
            {
                dt1.Columns.Remove(dc);
            }

            foreach (DataColumn dc in dc2.Where(x => dc1.All(y => x.ColumnName != y.ColumnName)))
            {
                dt2.Columns.Remove(dc);
            }

            string differencesInIDs = GetDifferencesInIDs(schemaName, tableName, dr1, dr2, friendlyName1, friendlyName2);

            dc1 = GetColumns(dt1).ToList();
            string differencesForSameIDs = GetDifferencesForSameIDs(schemaName, tableName, dc1, dr1, dr2, friendlyName1, friendlyName2);

            return validationErrors + differencesForSameIDs + differencesInIDs;
        }

        private static string GetDifferencesInIDs(string schemaName, string tableName,
                                                    List<DataRow> dataRows1, List<DataRow> dataRows2,
                                                    string friendlyName1, string friendlyName2)
        {
            DisplayProgressMessage($"Checking for different IDs in {schemaName}.{tableName}...");
            StringBuilder results = new StringBuilder();

            List<DataRow> differentIDs = new List<DataRow>();

            differentIDs = dataRows1.Except(dataRows2, new DataRowIDComparer()).ToList();

            if (differentIDs.Any())
            {
                foreach (DataRow Diff in differentIDs)
                {
                    results.AppendLine($"{schemaName}.{tableName}: ID = {(int)Diff.ItemArray[0]} is in {friendlyName1} but not in {friendlyName2}.");
                }
            }

            differentIDs = dataRows2.Except(dataRows1, new DataRowIDComparer()).ToList();

            if (differentIDs.Any())
            {
                foreach (DataRow Diff in differentIDs)
                {
                    results.AppendLine($"{schemaName}.{tableName}: ID = {(int)Diff.ItemArray[0]} is in {friendlyName2} but not in {friendlyName1}.");
                }
            }

            return results.ToString();
        }

        private class DataRowIDComparer : IEqualityComparer<DataRow>
        {
            public bool Equals(DataRow DR1, DataRow DR2)
            {
                return (int)DR1.ItemArray[0] == (int)DR2.ItemArray[0];
            }

            public int GetHashCode(DataRow DR)
            {
                // Check whether the object is null. 
                if (Object.ReferenceEquals(DR, null))
                {
                    return 0;
                }

                return DR.ItemArray[0].GetHashCode();
            }
        }

        private static string GetDifferencesForSameIDs(string schemaName, string tableName, List<DataColumn> dataColumns,
                                                        List<DataRow> dataRows1, List<DataRow> dataRows2,
                                                        string friendlyName1, string friendlyName2)
        {
            DisplayProgressMessage($"Checking for differences in {schemaName}.{tableName}...");

            StringBuilder results = new StringBuilder();
            const char quote = '"';

            //this assumes that the first column is the int ID column
            //Find the set of rows with IDs that are common between the two sets.
            List<DataRow> RowsWithTheSameIDs = dataRows1.Intersect(dataRows2, new DataRowIDComparer()).ToList();

            //Find all the rows from the first set that are in the common set
            //Find all the rows from the second set that are in the common set
            //Find all the rows that have IDs in the common set, but have different column values
            List<DataRow> DifferentRows = dataRows1
                                            .Intersect(RowsWithTheSameIDs, new DataRowIDComparer())
                                                .Except
                                                        (
                                                            dataRows2.Intersect(RowsWithTheSameIDs, new DataRowIDComparer())
                                                            , new DataRowComparer(dataColumns)
                                                        ).ToList();

            foreach (DataRow DR in DifferentRows)
            {
                DataRow DR1 = dataRows1.Where(dr1 => (int)dr1.ItemArray[0] == (int)DR.ItemArray[0]).Single();
                DataRow DR2 = dataRows2.Where(dr2 => (int)dr2.ItemArray[0] == (int)DR.ItemArray[0]).Single();

                foreach (DataColumn dataColumn in dataColumns)
                {
                    if (!DR1[dataColumn.ColumnName].Equals(DR2[dataColumn.ColumnName]))
                    {
                        string column = dataColumn.ColumnName;
                        int ID = (int)DR1.ItemArray[0];
                        object value1 = DR1[dataColumn.ColumnName];
                        object value2 = DR2[dataColumn.ColumnName];

                        string result = $"{schemaName}.{tableName}: {column} for ID = {ID} is {quote}{value1}{quote} in {friendlyName1} but {quote}{value2}{quote} in {friendlyName2}.";
                        results.AppendLine(result);
                    }
                }
            }

            return results.ToString();
        }

        private class DataRowComparer : IEqualityComparer<DataRow>
        {
            private List<DataColumn> dataColumns;

            public DataRowComparer(List<DataColumn> DataColumns)
            {
                dataColumns = DataColumns;
            }

            public bool Equals(DataRow DR1, DataRow DR2)
            {
                if ((int)DR1.ItemArray[0] == (int)DR2.ItemArray[0])
                {
                    foreach (DataColumn dataColumn in dataColumns)
                    {
                        if (!DR2[dataColumn.ColumnName].Equals(DR1[dataColumn.ColumnName]))
                        {
                            return false;
                        }
                    }

                    return true;
                }
                else
                {
                    return false;
                }
            }

            public int GetHashCode(DataRow DR)
            {
                // Check whether the object is null. 
                if (Object.ReferenceEquals(DR, null))
                {
                    return 0;
                }

                int hash = 0;
                foreach (DataColumn dataColumn in dataColumns)
                {
                    //This is from https://msdn.microsoft.com/en-us/library/bb336390(v=vs.90).aspx
                    //I'm not at all sure that it is correct here.
                    //It looks like more reading can be done here if this is determined to be stupid:
                    //https://msdn.microsoft.com/en-us/library/system.object.gethashcode(v=vs.110).aspx
                    hash = hash ^ DR[dataColumn.ColumnName].GetHashCode();
                }

                return hash;
            }
        }

        private static string GetValidationErrors(string schemaName, string tableName,
                                                    List<DataColumn> dc1, List<DataColumn> dc2,
                                                    string friendlyName1, string friendlyName2)
        {
            DisplayProgressMessage($"Validating {schemaName}.{tableName}...");
            StringBuilder results = new StringBuilder();

            //TODO: Make sure ID value is an int

            //TODO: check for duplicate ID values

            foreach (DataColumn dc in dc1.Where(x => dc2.All(y => y.ColumnName != x.ColumnName)))
            {
                results.AppendLine($"{schemaName}.{tableName}: {dc.ColumnName} column is in {friendlyName1} but not in {friendlyName2}.");
            }

            foreach (DataColumn dc in dc2.Where(x => dc1.All(y => y.ColumnName != x.ColumnName)))
            {
                results.AppendLine($"{schemaName}.{tableName}: {dc.ColumnName} column is in {friendlyName2} but not in {friendlyName1}.");
            }

            string dataTypesResult = CheckForDifferentDataTypes(schemaName, tableName, dc1, dc2);

            if (string.IsNullOrWhiteSpace(dataTypesResult))
            {
                results.AppendLine(dataTypesResult);
            }

            return results.ToString();
        }

        private static string CheckForDifferentDataTypes(string schemaName, string tableName, List<DataColumn> dc1, List<DataColumn> dc2)
        {
            DisplayProgressMessage($"Checking for different data types in {schemaName}.{tableName}...");
            string result = string.Empty;

            List<string> differentDataTypes = dc1.Where(x => dc2.Any(y => y.ColumnName == x.ColumnName && y.DataType != x.DataType))
                                                .Select(x => x.ColumnName)
                                                .ToList();

            if (differentDataTypes.Any())
            {
                string columnsWithDifferentDataTypes = differentDataTypes.Aggregate((current, next) => current + ", " + next);
                string plural = columnsWithDifferentDataTypes.Length == 1 ? "" : "s";
                result = $"{schemaName}.{tableName} not compared: Column{plural} with different data type{plural}: {columnsWithDifferentDataTypes}";
            }

            return result;
        }

        //In LINQPad: private static string CurrentDirectory => Path.GetDirectoryName(Util.CurrentQueryPath);
        private static string CurrentDirectory => Directory.GetParent(Directory.GetParent(Directory.GetCurrentDirectory()).FullName).FullName;

        private static void DisplayProgressMessage(string Message, bool ClearScreen = false)
        {
            if (ClearScreen)
            {
                Console.Clear();
            }
            Console.WriteLine(Message);
        }

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

        #endregion

    }
}
