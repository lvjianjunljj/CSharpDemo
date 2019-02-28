using System;
using System.Data.SqlClient;
using System.Text;

namespace CSharpDemo.Azure
{
    class AzureSQLDatabase
    {
        public static void MainMethod()
        {

            try
            {
                SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder();

                // your_server.database.windows.net
                builder.DataSource = $"tcp:{Constant.SQL_SERVER_NAME}.database.windows.net";
                // your_username
                builder.UserID = $"{Constant.Instance.SQLAccountUserId}@{Constant.SQL_SERVER_NAME}";
                // your_password
                builder.Password = Constant.Instance.SQLAccountPassword;
                // your_database
                builder.InitialCatalog = Constant.SQL_DATABASE_NAME;

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
                                Console.WriteLine("{0} {1}", reader.GetString(1), reader.GetString(2));
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
        }
    }
}
