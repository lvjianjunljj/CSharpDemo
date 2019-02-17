using Autofac;
using System;

namespace CSharpDemo.DIStudy.FactoryPattern
{
    class Demo3
    {
        public static void Run()
        {
            Console.WriteLine($"开始运行{nameof(Demo3)}");
            ContainerBuilder cb = new ContainerBuilder();
            cb.RegisterType<UserDal>().As<IUserDal>();
            cb.RegisterType<UserBll>().As<IUserBll>();
            cb.RegisterType<SmsSenderFactory>().As<ISmsSenderFactory>();
            cb.RegisterType<ConfigProvider>().As<IConfigProvider>();

            // The default value for enum in register is the first enum. Here is Console
            //cb.RegisterType<SmsConfig>().As<SmsConfig>();

            // This kind of writing is better than reading directly through the configuration in the class. 
            // More flexible...

            /* 
             * Note: 
             * I think in this case, ContainerBuilder cannot register an enum instance. 
             * So we can only register the SmsConfig instance.
             */
            cb.RegisterInstance(new SmsConfig
            {
                SmsSenderType = SmsSenderType.Console
            }).As<SmsConfig>();
            IContainer container = cb.Build();

            IUserBll userBll = container.Resolve<IUserBll>();
            bool login = userBll.Login("12345678901", "qwerty");
            Console.WriteLine(login);

            login = userBll.Login("10987654321", "ytrewq");
            Console.WriteLine(login);
            Console.WriteLine($"结束运行{nameof(Demo3)}");
        }

        public class UserBll : IUserBll
        {
            private readonly IUserDal _userDal;
            // In this case, I think using ISmsSender is better than using ISmsSenderFactory.
            private readonly ISmsSenderFactory _smsSenderFactory;

            public UserBll(
                IUserDal userDal,
                ISmsSenderFactory smsSenderFactory)
            {
                _userDal = userDal;
                _smsSenderFactory = smsSenderFactory;
            }

            public bool Login(string phone, string password)
            {
                bool re = _userDal.Exists(phone, password);
                if (re)
                {
                    ISmsSender smsSender = _smsSenderFactory.Create();
                    smsSender.Send(phone, "您已成功登录系统");
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
                Console.WriteLine($"已给{phone}发送消息：{message}");
            }
        }

        /// <summary>
        /// 真·短信API
        /// </summary>
        public class HttpApiSmsSender : ISmsSender
        {
            public void Send(string phone, string message)
            {
                Console.WriteLine($"已调用API给{phone}发送消息：{message}");
            }
        }

        public enum SmsSenderType
        {
            /// <summary>
            /// 控制台发送短信
            /// </summary>
            Console,

            /// <summary>
            /// 通过HttpApi进行发送短信
            /// </summary>
            HttpAPi
        }

        public interface ISmsSenderFactory
        {
            ISmsSender Create();
        }

        public class SmsSenderFactory : ISmsSenderFactory
        {
            private readonly IConfigProvider _configProvider;

            public SmsSenderFactory(
                IConfigProvider configProvider)
            {
                _configProvider = configProvider;
            }

            public ISmsSender Create()
            {
                // 短信发送者创建，从配置管理中读取当前的发送方式，并创建实例
                SmsConfig smsConfig = _configProvider.GetSmsConfig();
                Console.WriteLine(smsConfig.SmsSenderType);
                // Not good enough: Not decoupled from the implementation class of SmsSender
                switch (smsConfig.SmsSenderType)
                {
                    case SmsSenderType.Console:
                        return new ConsoleSmsSender();
                    case SmsSenderType.HttpAPi:
                        return new HttpApiSmsSender();
                    default:
                        return new HttpApiSmsSender();
                }
            }
        }

        public class SmsConfig
        {
            public SmsSenderType SmsSenderType { get; set; }
        }

        public interface IConfigProvider
        {
            SmsConfig GetSmsConfig();
        }

        public class ConfigProvider : IConfigProvider
        {
            private readonly SmsConfig _smsConfig;
            public ConfigProvider(SmsConfig smsConfig)
            {
                _smsConfig = smsConfig;
            }

            public SmsConfig GetSmsConfig()
            {
                return _smsConfig;
            }
        }
        //public class ConfigProvider : IConfigProvider
        //{
        //    private readonly SmsConfig _smsConfig = new SmsConfig
        //    {
        //        SmsSenderType = SmsSenderType.Console
        //    };

        //    public SmsConfig GetSmsConfig()
        //    {
        //        // 此处直接使用了写死的短信发送配置，实际项目中往往是通过配置读取的方式，实现该配置的加载。
        //        return _smsConfig;
        //    }
        //}
    }
}
