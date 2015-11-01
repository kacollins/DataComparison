using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Data;

namespace DataComparison
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Press any key to retrieve database table contents:");

            Console.ReadKey();

            Console.WriteLine("\nGetting SQL Connection Object for Database1...");

            Console.WriteLine(GetTableContents(GetDatabase1Connection(), GetSQLQueryText()));
            Console.WriteLine(GetTableContents(GetDatabase2Connection(), GetSQLQueryText()));

            Console.WriteLine(CompareDatatables(GetDataTable(GetDatabase1Connection(), GetSQLQueryText()), GetDataTable(GetDatabase2Connection(), GetSQLQueryText())));

            Console.WriteLine("Press any key to continue:");

            Console.ReadKey() ;
        }

        private static SqlConnection GetDatabase1Connection()
        {
            return new SqlConnection("Server=(localdb)\\MSSQLLocalDB;Integrated Security=false;AttachDbFilename=C:\\Git Repos\\TestingDatabase\\Databases\\Database1.mdf;User Id=Adam;Password=123456");
        }

        private static SqlConnection GetDatabase2Connection()
        {
            return new SqlConnection("Server=(localdb)\\MSSQLLocalDB;Integrated Security=false;AttachDbFilename=C:\\Git Repos\\TestingDatabase\\Databases\\Database2.mdf;User Id=Adam;Password=123456");
        }

        private static string GetSQLQueryText()
        {
            return "SELECT * FROM ALookup";
        }

        private static string GetTableContents(SqlConnection Conn, string SQL)
        {
            DataTable DT = GetDataTable(Conn, SQL);

            if (DT != null)
            {
                StringBuilder SB = new StringBuilder();

                IEnumerable<DataColumn> Columns = GetColumns(DT);

                foreach (var Col in Columns)
                {
                    SB.Append(Col.ColumnName + "\t");
                }

                SB.AppendLine();

                foreach (DataRow Row in DT.Rows)
                {
                    foreach (var Col in Columns)
                    {
                        SB.Append(Row.ItemArray[Col.Ordinal] + "\t");
                    }

                    SB.AppendLine();
                }

                return SB.ToString();
            }
            else
            {
                return string.Empty;
            }
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

        private static IEnumerable<DataColumn> GetColumns(DataTable DT)
        {
            List<DataColumn> Columns = new List<DataColumn>();

            foreach (DataColumn Col in DT.Columns)
            {
                Columns.Add(Col);
            }

            return Columns;
        }

        private static IEnumerable<DataRow> GetRows(DataTable DT)
        {
            List<DataRow> Rows = new List<DataRow>();

            foreach (DataRow Row in DT.Rows)
            {
                Rows.Add(Row);
            }

            return Rows.OrderBy(x => x.ItemArray[0]); //assuming the first column is the ID
        }

        private static string CompareDatatables(DataTable DT1, DataTable DT2)
        {
            IEnumerable<DataColumn> DT1Columns = GetColumns(DT1);
            IEnumerable<DataColumn> DT2Columns = GetColumns(DT2);

            IEnumerable<DataRow> DT1Rows = GetRows(DT1);
            IEnumerable<DataRow> DT2Rows = GetRows(DT2);

            StringBuilder SB = new StringBuilder();

            if (DT1Columns.Count() != DT2Columns.Count())
            {
                SB.AppendLine("These tables have a different number of columns!");
            }

            if
                (
                    DT1Columns.Any(x => !DT2Columns.Any(y => y.ColumnName == x.ColumnName))
                    ||
                    DT2Columns.Any(x => !DT1Columns.Any(y => y.ColumnName == x.ColumnName))
                )
            {
                SB.AppendLine("These tables have some differently named columns!");
            }

            if
                (
                    DT1Columns.Any(x => DT2Columns.Any(y => y.ColumnName == x.ColumnName && y.Ordinal != x.Ordinal))
                    ||
                    DT2Columns.Any(x => DT1Columns.Any(y => y.ColumnName == x.ColumnName && y.Ordinal != x.Ordinal))
                )
            {
                SB.AppendLine("These tables have some of the same columns but in different places!");
            }

            if
                (
                    DT1Columns.Any(x => DT2Columns.Any(y => y.ColumnName == x.ColumnName && y.DataType != x.DataType))
                    ||
                    DT2Columns.Any(x => DT1Columns.Any(y => y.ColumnName == x.ColumnName && y.DataType != x.DataType))
                )
            {
                SB.AppendLine("These tables have some of the same columns but with different data types!");
            }

            if (DT1Rows.Count() != DT2Rows.Count())
            {
                SB.AppendLine("These tables have a different number of rows!");
            }

            //this assumes that the first column is the int ID column
            foreach (var RowInDT1 in DT1Rows.Where(x => DT2Rows.Any(y => (int)x.ItemArray[0] == (int)y.ItemArray[0])))
            {
                foreach (var Col in DT1Columns)
                {
                    if
                        (!DT2Rows.Any
                                    (x =>
                                            (int)x.ItemArray[0] == (int)RowInDT1.ItemArray[0]
                                            &&
                                            x.ItemArray[Col.Ordinal].Equals(RowInDT1.ItemArray[Col.Ordinal])
                                    )
                        )
                    {

                        SB.AppendLine
                                        (string.Format
                                                        ("Column {0} for ID {1} is different: {2} vs {3}"
                                                            ,Col.ColumnName
                                                            , (int)RowInDT1.ItemArray[0]
                                                            ,RowInDT1.ItemArray[Col.Ordinal]
                                                            ,DT2Rows.First(x => (int)x.ItemArray[0] == (int)RowInDT1.ItemArray[0]).ItemArray[Col.Ordinal]
                                                        )
                                        );
                    }
                }
            }

            return SB.ToString();
        }
    }
}