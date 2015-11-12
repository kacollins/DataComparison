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
        const char backSlash = '\\';

        static void Main(string[] args)
        {
            Console.WriteLine("Press enter to compare database table contents:");
            Console.Read();

            IEnumerable<Table> tablesToCompare = GetTablesToCompare();

            foreach (Table table in tablesToCompare)
            {
                string queryText = $"SELECT * FROM [{table.SchemaName}].[{table.TableName}]";
                DataTable DT1 = GetDataTable(GetDatabase1Connection(), queryText);
                DataTable DT2 = GetDataTable(GetDatabase2Connection(), queryText);
                string results = CompareDatatables(DT1, DT2, table.SchemaName, table.TableName);
            
                Console.WriteLine(results);
                WriteToFile(results);

                Console.WriteLine("Press enter to continue:");
                Console.Read();
            }

            Console.WriteLine("Press enter to exit:");
            Console.Read();
        }

        private static void WriteToFile(string results)
        {
            string filePath = $"{Directory.GetCurrentDirectory()}{backSlash}results.txt";

            if (File.Exists(filePath))
            {
                File.Delete(filePath);
            }

            using (StreamWriter sw = File.CreateText(filePath))
            {
                sw.Write(results);
            }
        }

        private static IEnumerable<Table> GetTablesToCompare()
        {
            //TODO: Call SP to get list of tables to compare
            Table testingTable = new Table() { SchemaName = "dbo", TableName = "DatabaseLog" };
            return new List<Table>() { testingTable };
        }

        private static SqlConnection GetDatabase1Connection()
        {
            //TODO: Read from a file
            return new SqlConnection("Data Source=.\\SQLEXPRESS;Initial Catalog=AdventureWorks2012;Integrated Security=True;MultipleActiveResultSets=True");
        }

        private static SqlConnection GetDatabase2Connection()
        {
            //TODO: Read from a file
            return new SqlConnection("Data Source=.\\SQLEXPRESS;Initial Catalog=AdventureWorks2;Integrated Security=True;MultipleActiveResultSets=True");
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
                Console.WriteLine(ex.Message);
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
            //TODO: Read from a file
            return new List<string>() { "UserCreated", "DateCreated", "UserModified", "DateModified" };
        }

        private static IEnumerable<DataColumn> GetColumns(DataTable DT)
        {
            List<string> columnsToIgnore = GetColumnsToIgnore();
            return DT.Columns.Cast<DataColumn>().Where(dc => !columnsToIgnore.Contains(dc.ColumnName)).ToList();
        }

        private static IEnumerable<DataRow> GetRows(DataTable DT)
        {
            List<DataRow> Rows = DT.Rows.Cast<DataRow>().ToList();

            return Rows.OrderBy(x => x.ItemArray[0]); //assuming the first column is the ID
        }

        private static string CompareDatatables(DataTable DT1, DataTable DT2, string schemaName, string tableName)
        {
            List<DataColumn> DT1Columns = GetColumns(DT1).ToList();
            List<DataColumn> DT2Columns = GetColumns(DT2).ToList();

            List<DataRow> DT1Rows = GetRows(DT1).ToList();
            List<DataRow> DT2Rows = GetRows(DT2).ToList();

            string validationErrors = ValidateColumns(schemaName, tableName, DT1Columns, DT2Columns);

            if (validationErrors.Length == 0)
            {
                StringBuilder results = new StringBuilder();

                results.AppendLine(GetDifferencesInIDs(schemaName, tableName, DT1Rows, DT2Rows));
                results.AppendLine(GetDifferencesForSameIDs(schemaName, tableName, DT1Columns, DT1Rows, DT2Rows));

                return results.ToString();
            }
            else
            {
                return validationErrors;
            }
        }

        private static string GetDifferencesForSameIDs(string schemaName, string tableName, List<DataColumn> DT1Columns,
                                                        List<DataRow> DT1Rows, List<DataRow> DT2Rows)
        {
            StringBuilder results = new StringBuilder();

            //this assumes that the first column is the int ID column
            foreach (var RowInDT1 in DT1Rows.Where(x => DT2Rows.Any(y => (int)x.ItemArray[0] == (int)y.ItemArray[0])))
            {
                foreach (var Col in DT1Columns.Where(Col => !DT2Rows.Any
                    (x => (int)x.ItemArray[0] == (int)RowInDT1.ItemArray[0]
                          && x.ItemArray[Col.Ordinal].Equals(RowInDT1.ItemArray[Col.Ordinal])
                    )))
                {
                    string columnName = Col.ColumnName;
                    int ID = (int)RowInDT1.ItemArray[0];
                    object value1 = RowInDT1.ItemArray[Col.Ordinal];
                    object value2 =
                        DT2Rows.First(x => (int)x.ItemArray[0] == (int)RowInDT1.ItemArray[0]).ItemArray[Col.Ordinal];

                    results.AppendLine(
                        $"{schemaName}.{tableName} - Column {columnName} for ID {ID} is different: {value1} vs {value2}");
                }
            }

            return results.ToString();
        }

        private static string GetDifferencesInIDs(string schemaName, string tableName, List<DataRow> DT1Rows, List<DataRow> DT2Rows)
        {
            StringBuilder results = new StringBuilder();

            foreach (var RowInDT1 in DT1Rows.Where(x => DT2Rows.All(y => (int)x.ItemArray[0] != (int)y.ItemArray[0])))
            {
                results.AppendLine(
                    $"{schemaName}.{tableName} - ID {(int)RowInDT1.ItemArray[0]} is in database 1 but not in database 2.");
            }

            foreach (var RowInDT2 in DT2Rows.Where(x => DT1Rows.All(y => (int)x.ItemArray[0] != (int)y.ItemArray[0])))
            {
                results.AppendLine(
                    $"{schemaName}.{tableName} - ID {(int)RowInDT2.ItemArray[0]} is in database 1 but not in database 2.");
            }

            return results.ToString();
        }

        private static string ValidateColumns(string schemaName, string tableName, List<DataColumn> DT1Columns, List<DataColumn> DT2Columns)
        {
            StringBuilder SB = new StringBuilder();

            //TODO: Make sure primary key is an int

            foreach (DataColumn dc in DT1Columns.Where(x => DT2Columns.All(y => y.ColumnName != x.ColumnName)))
            {
                SB.AppendLine($"{schemaName}.{tableName} - {dc.ColumnName} column in database 1 but not in database 2!");
            }

            foreach (DataColumn dc in DT2Columns.Where(x => DT1Columns.All(y => y.ColumnName != x.ColumnName)))
            {
                SB.AppendLine($"{schemaName}.{tableName} - {dc.ColumnName} column in database 2 but not in database 1!");
            }

            if (DT1Columns.Any(x => DT2Columns.Any(y => y.ColumnName == x.ColumnName && y.Ordinal != x.Ordinal))
                || DT2Columns.Any(x => DT1Columns.Any(y => y.ColumnName == x.ColumnName && y.Ordinal != x.Ordinal)))
            {
                //TODO: List columns
                SB.AppendLine($"{schemaName}.{tableName} - Column(s) in different places!");
            }

            if (DT1Columns.Any(x => DT2Columns.Any(y => y.ColumnName == x.ColumnName && y.DataType != x.DataType))
                || DT2Columns.Any(x => DT1Columns.Any(y => y.ColumnName == x.ColumnName && y.DataType != x.DataType)))
            {
                //TODO: List columns
                SB.AppendLine($"{schemaName}.{tableName} - Column(s) with different data types!");
            }

            return SB.ToString();
        }
    }
}
