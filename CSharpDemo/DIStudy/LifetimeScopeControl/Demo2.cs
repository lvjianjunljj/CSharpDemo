using Autofac;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CSharpDemo.DIStudy.LifetimeScopeControl
{
    public static class Demo2
    {
        public static void Run()
        {
            ContainerBuilder cb = new ContainerBuilder();
            // Function SingleInstance achieve the Single Mode
            cb.RegisterType<StaticClockByOneTime>()
                .As<IClock>()
                .SingleInstance();
            IContainer container = cb.Build();
            IClock clock = container.Resolve<IClock>();
            Console.WriteLine($"第一次获取时间：{clock.Now}");
            Thread.Sleep(1000);
            clock = container.Resolve<IClock>();
            Console.WriteLine($"第二次获取时间：{clock.Now}");
            Thread.Sleep(1000);
            clock = container.Resolve<IClock>();
            Console.WriteLine($"第三次获取时间：{clock.Now}");
        }

        public interface IClock
        {
            /// <summary>
            /// 获取当前系统时间
            /// </summary>
            DateTime Now { get; }
        }

        public class StaticClockByOneTime : IClock
        {
            private DateTime _firstTime = DateTime.MinValue;
            public DateTime Now
            {
                get
                {
                    if (_firstTime == DateTime.MinValue)
                    {
                        _firstTime = DateTime.Now;
                    }

                    return _firstTime;
                }
            }
        }
    }
}
