using System;
using System.Data.SqlClient;
using System.Text;

namespace CSharpDemo
{
    class AzureSQLDatabaseDemo
    {
        static void MainMethod()
        {
            try
            {
                SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder();

                // your_server.database.windows.net
                builder.DataSource = @"tcp:csharpmvcwebapidatabaseserver.database.windows.net";
                // your_username
                builder.UserID = @"jianjlv";
                // your_password
                builder.Password = "6076361Abb";
                // your_database
                builder.InitialCatalog = "CSharpMVCWebAPIDatabase";

                using (SqlConnection connection = new SqlConnection(builder.ConnectionString))
                {
                    Console.WriteLine("\nQuery data example:");
                    Console.WriteLine("=========================================\n");

                    connection.Open();
                    StringBuilder sb = new StringBuilder();
                    //sb.Append("SELECT TOP 20 pc.Name as CategoryName, p.name as ProductName ");
                    //sb.Append("FROM [SalesLT].[ProductCategory] pc ");
                    //sb.Append("JOIN [SalesLT].[Product] p ");
                    //sb.Append("ON pc.productcategoryid = p.productcategoryid;");
                    sb.Append(@"select * from dbo.user_info");
                    String sql = sb.ToString();

                    using (SqlCommand command = new SqlCommand(sql, connection))
                    {
                        using (SqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                Console.WriteLine("{0} {1}", reader.GetString(0), reader.GetString(1));
                            }
                        }
                    }
                }
            }
            catch (SqlException e)
            {
                Console.WriteLine(e.ToString());
            }
            Console.WriteLine("\nDone. Press enter.");
            Console.ReadKey();
        }
    }
}
