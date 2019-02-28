using SqlSugar;
using System;
using System.Collections.Generic;

namespace CSharpDemo
{
    class SqlSugarDemo
    {
        public static void MainMethod()
        {
            SqlSugarClient db = new SqlSugarClient(new ConnectionConfig()
            {
                ConnectionString = $"Server=tcp:{Constant.SQL_SERVER_NAME}.database.windows.net;Database={Constant.SQL_DATABASE_NAME};User ID ={Constant.Instance.SQLAccountUserId}@{Constant.SQL_SERVER_NAME};Password={Constant.Instance.SQLAccountPassword}; ",
                DbType = DbType.SqlServer, //必填
                IsAutoCloseConnection = true
            });


            List<UserEntity> list = db.Queryable<UserEntity>().ToList();//查询所有（使用SqlSugarClient查询所有到LIST）
            foreach (UserEntity user in list)
            {
                Console.WriteLine("{0}\t{1}\t{2}", user.Id, user.Username, user.Password);
            }
        }

    }
    [SugarTable("User_Info")]
    class UserEntity
    {
        [SugarColumn(ColumnName = "userid")]
        public int Id { get; set; }
        [SugarColumn(ColumnName = "username")]
        public string Username { get; set; }
        [SugarColumn(ColumnName = "userpassword")]
        public string Password { get; set; }

    }
}
