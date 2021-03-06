﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSharpDemo.DIStudy.DISampleDemo
{
    // I do not think this is a good demo, it cannot show the reason of using DI.
    class InjectionTest
    {
    }
    /// <summary>
    /// 在构造函数中注入
    /// </summary>
    class ConstructorInjectionClient
    {
        private ITimeProvider timeProvider;

        public ConstructorInjectionClient(ITimeProvider timeProvider)
        {
            this.timeProvider = timeProvider;
        }
    }

    class ConstructorInjectionTest
    {
        public static void Test()
        {
            ITimeProvider timeProvider = (new Assembler()).Create("SystemTimeProvider");
            ConstructorInjectionClient client = new ConstructorInjectionClient(timeProvider);   // 在构造函数中注入
        }
    }

    /// <summary>
    /// 通过Setter实现注入
    /// </summary>
    class SetterInjectionClient
    {
        private ITimeProvider timeProvider;

        public ITimeProvider TimeProvider
        {
            get { return this.timeProvider; }   // getter本身和Setter方式实现注入没有关系
            set { this.timeProvider = value; }
        }
    }

    class SetterInjectionTest
    {
        public static void Test()
        {
            ITimeProvider timeProvider = (new Assembler()).Create("SystemTimeProvider");
            SetterInjectionClient client = new SetterInjectionClient();
            client.TimeProvider = timeProvider; // 通过Setter实现注入
        }
    }

    /// <summary>
    /// 定义需要注入ITimeProvider的类型
    /// </summary>
    interface IObjectWithTimeProvider
    {
        ITimeProvider TimeProvider { get; set; }
    }

    /// <summary>
    /// 通过接口方式注入
    /// </summary>
    class InterfacerInjectionClient : IObjectWithTimeProvider
    {
        private ITimeProvider timeProvider;

        /// <summary>
        /// IObjectWithTimeProvider Members
        /// </summary>
        public ITimeProvider TimeProvider
        {
            get { return this.timeProvider; }
            set { this.timeProvider = value; }
        }
    }

    class InterfacerInjectionTest
    {
        public static void Test()
        {
            ITimeProvider timeProvider = (new Assembler()).Create("SystemTimeProvider");
            IObjectWithTimeProvider objectWithTimeProvider = new InterfacerInjectionClient();
            objectWithTimeProvider.TimeProvider = timeProvider; // 通过接口方式注入
        }
    }

    //可以通过Attribute将附加的内容注入到对象上。

    [AttributeUsage(AttributeTargets.Class, AllowMultiple = true)]
    sealed class DecoratorAttribute : Attribute
    {

        public readonly object Injector;
        private Type type;

        public DecoratorAttribute(Type type)
        {
            if (type == null) throw new ArgumentNullException("type");
            this.type = type;
            Injector = (new Assembler()).Create("SystemTimeProvider");
        }

        public Type Type { get { return this.type; } }
    }

    /// <summary>
    /// 用户帮助客户类型和客户程序获取其Attribute定义中需要的抽象类型实例
    /// </summary>
    static class AttributeHelper
    {
        public static T Injector<T>(object target)
            where T : class
        {
            if (target == null) throw new ArgumentNullException("target");
            Type targetType = target.GetType();
            object[] attributes = targetType.GetCustomAttributes(typeof(DecoratorAttribute), false);
            if ((attributes == null) || (attributes.Length <= 0)) return null;
            foreach (DecoratorAttribute attribute in (DecoratorAttribute[])attributes)
                if (attribute.Type == typeof(T))
                    return (T)attribute.Injector;
            return null;
        }
    }

    [Decorator(typeof(ITimeProvider))]
    class AttributerInjectionClient
    {
        public string GetTime()
        {
            // 与其他方式注入不同的是，这里使用的ITimeProvider来自自己的Attribute
            ITimeProvider provider = AttributeHelper.Injector<ITimeProvider>(this);
            return provider.CurrentDate.ToString();
        }
    }

    class AttributerInjectionTest
    {
        public static void Test()
        {
            AttributerInjectionClient client = new AttributerInjectionClient();
            string time = client.GetTime();
        }
    }
}
