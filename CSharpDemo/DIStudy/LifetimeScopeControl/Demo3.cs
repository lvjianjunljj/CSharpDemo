using Autofac;
using System;
using System.Collections.Generic;

namespace CSharpDemo.DIStudy.LifetimeScopeControl
{
    class Demo3
    {
        public static void Run()
        {
            ContainerBuilder cb = new ContainerBuilder();
            cb.RegisterType<AccountBll>().As<IAccountBll>();
            cb.RegisterType<AccountDal>().As<IAccountDal>();
            cb.RegisterType<ConsoleLogger>().As<ILogger>().InstancePerLifetimeScope();
            IContainer container = cb.Build();

            //using (ILifetimeScope beginLifetimeScope = container.BeginLifetimeScope())
            //{

            //    IAccountBll accountBll = beginLifetimeScope.Resolve<IAccountBll>();
            //    accountBll.Transfer("yueluo", "newbe", 333);
            //    accountBll.Transfer("yueluo", "newbe", 333);
            //}

            /*
             * IContainer inherits ILifetimeScope, and function BeginLifetimeScope is to begin a new nested scope.
             * Component instances created via the new scope will  be disposed along with it.
             * So I think this is a better demo.
             * 
             * Here have three situations:
             * 1. Single Instance in every lifetime
             * The ILoggers that AccountBll and AccountDal depend on in every lifetime are the same, 
             * but in different lifetime are different
             *  cb.RegisterType<ConsoleLogger>().As<ILogger>().InstancePerLifetimeScope();
             * 2. Single Instance in entire project
             * The ILoggers that AccountBll and AccountDal depend on in every lifetime are the same,
             * and in different lifetime are the also the same
             *  cb.RegisterType<ConsoleLogger>().As<ILogger>().SingleInstance();
             * 3. Different instance in every step
             * The ILoggers that AccountBll and AccountDal depend on in every lifetime are different,
             * and in different lifetime are also different.
             *  cb.RegisterType<ConsoleLogger>().As<ILogger>();
             * We can have a try for every situation. The outputs are different:
             * Maybe the "输出日志" is the same, but the logger hashcode in every function is different.
             */

            using (ILifetimeScope beginLifetimeScope = container.BeginLifetimeScope())
            {
                IAccountBll accountBll = beginLifetimeScope.Resolve<IAccountBll>();
                accountBll.Transfer("yueluo", "newbe", 333);
            }

            using (ILifetimeScope beginLifetimeScope = container.BeginLifetimeScope())
            {
                IAccountBll accountBll = beginLifetimeScope.Resolve<IAccountBll>();
                accountBll.Transfer("yueluo", "newbe", 333);
            }
        }

        public interface ILogger
        {
            // Unable to define a public variable in interface
            // so we need to define a function in this interface
            void DefineScope(string scopeTag);
            void Log(string message);
        }

        public class ConsoleLogger : ILogger
        {
            public string _currenctScopeTag;

            public void DefineScope(string scopeTag)
            {
                _currenctScopeTag = scopeTag;
            }

            public void Log(string message)
            {
                Console.WriteLine(string.IsNullOrEmpty(_currenctScopeTag)
                    ? $"输出日志：{message}"
                    : $"输出日志：{message}[scope:{_currenctScopeTag}]");
            }
        }

        public interface IAccountBll
        {
            /// <summary>
            /// 转账
            /// </summary>
            /// <param name="fromAccountId">来源账号Id</param>
            /// <param name="toAccountId">目标账号Id</param>
            /// <param name="amount">转账数额</param>
            void Transfer(string fromAccountId, string toAccountId, decimal amount);
        }

        public class AccountBll : IAccountBll
        {
            private readonly ILogger _logger;
            private readonly IAccountDal _accountDal;

            public AccountBll(
                ILogger logger,
                IAccountDal accountDal)
            {
                _logger = logger;
                _accountDal = accountDal;
            }

            public void Transfer(string fromAccountId, string toAccountId, decimal amount)
            {
                Console.WriteLine("Transfer function logger hashcode: " + _logger.GetHashCode());
                _logger.DefineScope(Guid.NewGuid().ToString());
                decimal fromAmount = _accountDal.GetBalance(fromAccountId);
                decimal toAmount = _accountDal.GetBalance(toAccountId);
                fromAmount -= amount;
                toAmount += amount;
                _accountDal.UpdateBalance(fromAccountId, fromAmount);
                _accountDal.UpdateBalance(toAccountId, toAmount);
            }
        }

        public interface IAccountDal
        {
            /// <summary>
            /// 获取账户的余额
            /// </summary>
            /// <param name="id"></param>
            /// <returns></returns>
            decimal GetBalance(string id);

            /// <summary>
            /// 更新账户的余额
            /// </summary>
            /// <param name="id"></param>
            /// <param name="balance"></param>
            void UpdateBalance(string id, decimal balance);
        }

        public class AccountDal : IAccountDal
        {
            private readonly ILogger _logger;

            public AccountDal(
                ILogger logger)
            {
                _logger = logger;
            }

            private readonly Dictionary<string, decimal> _accounts = new Dictionary<string, decimal>
            {
                {"newbe",1000},
                {"yueluo",666},
            };

            public decimal GetBalance(string id)
            {
                return _accounts.TryGetValue(id, out decimal balance) ? balance : 0;
            }

            public void UpdateBalance(string id, decimal balance)
            {
                Console.WriteLine("UpdateBalance function logger hashcode: " + _logger.GetHashCode());
                _logger.Log($"更新了 {id} 的余额为 {balance}");
                _accounts[id] = balance;
            }
        }
    }
}
