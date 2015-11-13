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
            List<DatabasePair> databasesToCompare = GetDatabasesToCompare();

            foreach (DatabasePair databasePair in databasesToCompare)
            {
                CompareDatabases(tablesToCompare, databasePair);
            }

            Console.WriteLine("Done! Press enter to exit:");
            Console.Read();
        }

        #region Methods

        private static List<Table> GetTablesToCompare()
        {
            List<string> lines = GetFileLines("TablesToCompare.supersecret");

            List<Table> tablesToCompare = new List<Table>();

            foreach (string[] lineParts in lines.Select(line => line.Split('.')))
            {
                if (lineParts.Length == Enum.GetValues(typeof(TablePart)).Length)
                {
                    tablesToCompare.Add(new Table(lineParts[(int)TablePart.SchemaName],
                                                lineParts[(int)TablePart.TableName]));
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

            foreach (string[] lineParts in lines.Select(line => line.Split(',')))
            {
                if (lineParts.Length == Enum.GetValues(typeof(DatabasePairPart)).Length)
                {
                    databasePairs.Add(new DatabasePair(new Database(lineParts[(int)DatabasePairPart.FriendlyName1]
                                                                    , lineParts[(int)DatabasePairPart.ServerName1]
                                                                    , lineParts[(int)DatabasePairPart.DatabaseName1])
                                                    , new Database(lineParts[(int)DatabasePairPart.FriendlyName2]
                                                                    , lineParts[(int)DatabasePairPart.ServerName2]
                                                                    , lineParts[(int)DatabasePairPart.DatabaseName2])));
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
                List<string> lines = File.ReadAllLines(file.FullName).Where(x => !string.IsNullOrWhiteSpace(x)).ToList();
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

                if (!string.IsNullOrEmpty(result))
                {
                    results.Add(result);
                }
            }

            if (results.Any())
            {
                string filename = $"{DateTime.Today.ToString("yyyyMMdd")}_{databasePair.Database1.FriendlyName}_{databasePair.Database2.FriendlyName}";
                WriteToFile(filename, results.Aggregate((current, next) => current + Environment.NewLine + next));
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
                results = CompareDatatables(DT1, DT2, table.SchemaName, table.TableName, friendlyName1, friendlyName2).Trim();
            }

            return results;
        }

        private static void WriteToFile(string fileName, string fileContents)
        {
            const char backSlash = '\\';
            string filePath = $"{CurrentDirectory}{backSlash}Results{backSlash}{fileName}.txt";

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
            catch (Exception ex)
            {
                throw ex;
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

        private static string CompareDatatables(DataTable DT1, DataTable DT2,
                                                string schemaName, string tableName,
                                                string friendlyName1, string friendlyName2)
        {
            List<DataColumn> DT1Columns = GetColumns(DT1).ToList();
            List<DataColumn> DT2Columns = GetColumns(DT2).ToList();

            List<DataRow> DT1Rows = GetRows(DT1).ToList();
            List<DataRow> DT2Rows = GetRows(DT2).ToList();

            string validationErrors = GetValidationErrors(schemaName, tableName, DT1Columns, DT2Columns, friendlyName1, friendlyName2);

            if (string.IsNullOrWhiteSpace(validationErrors))
            {
                StringBuilder results = new StringBuilder();

                results.Append(GetDifferencesInIDs(schemaName, tableName, DT1Rows, DT2Rows, friendlyName1, friendlyName2));
                results.Append(GetDifferencesForSameIDs(schemaName, tableName, DT1Columns, DT1Rows, DT2Rows, friendlyName1, friendlyName2));

                return results.ToString();
            }
            else
            {
                return validationErrors;
            }
        }

        private static string GetDifferencesInIDs(string schemaName, string tableName,
                                                    List<DataRow> DT1Rows, List<DataRow> DT2Rows,
                                                    string friendlyName1, string friendlyName2)
        {
            StringBuilder results = new StringBuilder();

            foreach (var RowInDT1 in DT1Rows.Where(x => DT2Rows.All(y => (int)x.ItemArray[0] != (int)y.ItemArray[0])))
            {
                results.AppendLine($"{schemaName}.{tableName}: ID = {(int)RowInDT1.ItemArray[0]} is in {friendlyName1} but not in {friendlyName2}.");
            }

            foreach (var RowInDT2 in DT2Rows.Where(x => DT1Rows.All(y => (int)x.ItemArray[0] != (int)y.ItemArray[0])))
            {
                results.AppendLine($"{schemaName}.{tableName}: ID = {(int)RowInDT2.ItemArray[0]} is in {friendlyName2} but not in {friendlyName1}.");
            }

            return results.ToString();
        }

        private static string GetDifferencesForSameIDs(string schemaName, string tableName, List<DataColumn> DT1Columns,
                                                        List<DataRow> DT1Rows, List<DataRow> DT2Rows,
                                                        string friendlyName1, string friendlyName2)
        {
            StringBuilder results = new StringBuilder();
            const char quote = '\"';

            //this assumes that the first column is the int ID column
            foreach (var RowInDT1 in DT1Rows.Where(x => DT2Rows.Any(y => (int)x.ItemArray[0] == (int)y.ItemArray[0])))
            {
                foreach (var Col in DT1Columns.Where(Col => !DT2Rows.Any
                    (x => (int)x.ItemArray[0] == (int)RowInDT1.ItemArray[0]
                          && x.ItemArray[Col.Ordinal].Equals(RowInDT1.ItemArray[Col.Ordinal])
                    )))
                {
                    string column = Col.ColumnName;
                    int ID = (int)RowInDT1.ItemArray[0];
                    object value1 = RowInDT1.ItemArray[Col.Ordinal];
                    object value2 = DT2Rows.First(x => (int)x.ItemArray[0] == (int)RowInDT1.ItemArray[0]).ItemArray[Col.Ordinal];

                    results.AppendLine($"{schemaName}.{tableName}: {column} for ID = {ID} is {quote}{value1}{quote} in {friendlyName1} but {quote}{value2}{quote} in {friendlyName2}");
                }
            }

            return results.ToString();
        }

        private static string GetValidationErrors(string schemaName, string tableName,
                                                    List<DataColumn> DT1Columns, List<DataColumn> DT2Columns,
                                                    string friendlyName1, string friendlyName2)
        {
            StringBuilder results = new StringBuilder();

            //TODO: Make sure primary key is an int

            foreach (DataColumn dc in DT1Columns.Where(x => DT2Columns.All(y => y.ColumnName != x.ColumnName)))
            {
                results.AppendLine($"{schemaName}.{tableName} not compared: {dc.ColumnName} column is in {friendlyName1} but not in {friendlyName2}.");
            }

            foreach (DataColumn dc in DT2Columns.Where(x => DT1Columns.All(y => y.ColumnName != x.ColumnName)))
            {
                results.AppendLine($"{schemaName}.{tableName} not compared: {dc.ColumnName} column is in {friendlyName2} but not in {friendlyName1}!");
            }

            List<string> differentOrdinals = DT1Columns.Where(x => DT2Columns.Any(y => y.ColumnName == x.ColumnName && y.Ordinal != x.Ordinal))
                                                        .Select(x => x.ColumnName)
                                                        .ToList();

            if (differentOrdinals.Any())
            {
                string columnsWithDifferentOrdinals = differentOrdinals.Aggregate((current, next) => current + ", " + next);
                results.AppendLine($"{schemaName}.{tableName} not compared: Column(s) with different ordinals: {columnsWithDifferentOrdinals}");
            }

            List<string> differentDataTypes = DT1Columns.Where(x => DT2Columns.Any(y => y.ColumnName == x.ColumnName && y.DataType != x.DataType))
                                                        .Select(x => x.ColumnName)
                                                        .ToList();

            if (differentDataTypes.Any())
            {
                string columnsWithDifferentDataTypes = differentDataTypes.Aggregate((current, next) => current + ", " + next);
                results.AppendLine($"{schemaName}.{tableName} not compared: Column(s) with different data types: {columnsWithDifferentDataTypes}");
            }

            return results.ToString();
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
