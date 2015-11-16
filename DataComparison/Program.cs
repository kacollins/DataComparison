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
            List<string> lines = GetFileLines("ConnectionStrings.supersecret");

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
                    Console.WriteLine("Error: Invalid database pair in ConnectionStrings file.");
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
                DT1 = GetDataTable(connection1, queryText);
            }
            catch (Exception ex)
            {
                results = $"Error for {friendlyName1}: {ex.Message}";
            }

            try
            {
                DT2 = GetDataTable(connection2, queryText);
            }
            catch (Exception ex)
            {
                results = $"Error for {friendlyName2}: {ex.Message}";
            }

            if (DT1 != null && DT2 != null)
            {
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
            return new List<string>() { "UserCreated", "DateCreated", "UserModified", "DateModified" };
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
            StringBuilder results = new StringBuilder();

            foreach (DataRow dataRow1 in dataRows1.Where(dr1 => dataRows2.All(dr2 => (int)dr1.ItemArray[0] != (int)dr2.ItemArray[0])))
            {
                results.AppendLine($"{schemaName}.{tableName}: ID = {(int)dataRow1.ItemArray[0]} is in {friendlyName1} but not in {friendlyName2}.");
            }

            foreach (DataRow datarow2 in dataRows2.Where(dr2 => dataRows1.All(dr1 => (int)dr2.ItemArray[0] != (int)dr1.ItemArray[0])))
            {
                results.AppendLine($"{schemaName}.{tableName}: ID = {(int)datarow2.ItemArray[0]} is in {friendlyName2} but not in {friendlyName1}.");
            }

            return results.ToString();
        }

        private static string GetDifferencesForSameIDs(string schemaName, string tableName, List<DataColumn> dataColumns,
                                                        List<DataRow> dataRows1, List<DataRow> dataRows2,
                                                        string friendlyName1, string friendlyName2)
        {
            StringBuilder results = new StringBuilder();
            const char quote = '"';

            //this assumes that the first column is the int ID column
            foreach (DataRow dataRow1 in dataRows1.Where(dr1 => dataRows2.Any(dr2 => (int)dr1.ItemArray[0] == (int)dr2.ItemArray[0])))
            {
                foreach (DataColumn dataColumn in dataColumns.Where(dc => !dataRows2.Any(dr2 => (int)dr2.ItemArray[0] == (int)dataRow1.ItemArray[0]
                                                                                             && dr2[dc.ColumnName].Equals(dataRow1[dc.ColumnName])
                                                                                        )))
                {
                    string column = dataColumn.ColumnName;
                    int ID = (int)dataRow1.ItemArray[0];
                    object value1 = dataRow1[dataColumn.ColumnName];
                    object value2 = dataRows2.First(x => (int)x.ItemArray[0] == (int)dataRow1.ItemArray[0])[dataColumn.ColumnName];

                    string result = $"{schemaName}.{tableName}: {column} for ID = {ID} is {quote}{value1}{quote} in {friendlyName1} but {quote}{value2}{quote} in {friendlyName2}.";
                    results.AppendLine(result);
                }
            }

            return results.ToString();
        }

        private static string GetValidationErrors(string schemaName, string tableName,
                                                    List<DataColumn> dc1, List<DataColumn> dc2,
                                                    string friendlyName1, string friendlyName2)
        {
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
