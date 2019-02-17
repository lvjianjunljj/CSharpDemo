using Autofac;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSharpDemo.DIStudy.FactoryPattern
{
    class Demo2
    {
        public static void Run()
        {
            Console.WriteLine($"开始运行{nameof(Demo2)}");
            ContainerBuilder cb = new ContainerBuilder();
            cb.RegisterType<UserDal>().As<IUserDal>();
            cb.RegisterType<UserBll>().As<IUserBll>();
            // 使用预编译命令，使得 Release 和 Debug 版本注册的对象不同，从而实现调用的短信API不同
#if DEBUG
            cb.RegisterType<ConsoleSmsSender>().As<ISmsSender>();
#else
            cb.RegisterType<HttpApiSmsSender>().As<ISmsSender>();
#endif
            IContainer container = cb.Build();

            IUserBll userBll = container.Resolve<IUserBll>();
            bool login = userBll.Login("12345678901", "qwerty");
            Console.WriteLine(login);

            login = userBll.Login("10987654321", "ytrewq");
            Console.WriteLine(login);
            Console.WriteLine($"结束运行{nameof(Demo2)}");
        }

        public class UserBll : IUserBll
        {
            private readonly IUserDal _userDal;
            private readonly ISmsSender _smsSender;

            public UserBll(
                IUserDal userDal,
                ISmsSender smsSender)
            {
                _userDal = userDal;
                _smsSender = smsSender;
            }

            public bool Login(string phone, string password)
            {
                bool re = _userDal.Exists(phone, password);
                if (re)
                {
                    _smsSender.Send(phone, "您已成功登录系统");
                }

                return re;
            }
        }
        /// <summary>
        /// 调试·短信API
        /// </summary>
        public class ConsoleSmsSender : ISmsSender
        {
            public void Send(string phone, string message)
            {
                Console.WriteLine($"调试：已给{phone}发送消息：{message}");
            }
        }

        /// <summary>
        /// 真·短信API
        /// </summary>
        public class HttpApiSmsSender : ISmsSender
        {
            public void Send(string phone, string message)
            {
                Console.WriteLine($"上线：已调用API给{phone}发送消息：{message}");
            }
        }
    }
}
