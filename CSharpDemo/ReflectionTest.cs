using System;
using System.Reflection;

namespace CSharpDemo
{
    public enum GenderEnum
    {
        Boy = 0,
        Girl = 1,
        Dog = 2
    }

    class WorkerModel
    {

        /// <summary>
        /// 字段 Fields
        /// </summary>
        public string name = "Hathway";
        private int id = 32;
        protected bool isAdmin = true;
        public GenderEnum gender = GenderEnum.Girl;


        /// <summary>
        /// 属性 Attributes 
        /// </summary>
        public string Name { get; set; }
        public GenderEnum Gender { get; set; }
        private int Id { get; set; }
        protected bool IsAdmin { get; set; }

        /// <summary>
        /// 方法 Function
        /// </summary>
        public void Android()
        {
        }
        protected void iOS()
        {
        }
        private void WindowPhone()
        {
        }


        /// <summary>
        /// 构造方法 Constructor
        /// </summary>
        public WorkerModel()
        {
        }

        public WorkerModel(string name, int id, GenderEnum gender, bool isAdmin)
        {
        }


    }
    class ReflectionTest
    {
        static void MainMethod(string[] args)
        {
            WorkerModel wm = new WorkerModel("name", 1, GenderEnum.Girl, true);
            wm.Name = "Name";
            wm.Gender = GenderEnum.Dog;
            GetPropertyInfo(wm);

            Console.ReadKey();
        }

        // 字段信息
        static void GetFieldValue(Object obj)
        {
            //得到对象的类型
            Type type = obj.GetType();
            //得到字段的值,只能得到public类型的字典的值
            FieldInfo[] fieldInfos = type.GetFields();
            foreach (FieldInfo f in fieldInfos)
            {
                //字段名称
                string fieldName = f.Name;
                //字段类型
                string fieldType = f.FieldType.ToString();
                //字段的值
                string fieldValue = f.GetValue(obj).ToString();

                Console.WriteLine("fieldName------>" + fieldName);
                Console.WriteLine("fieldType------>" + fieldType);
                Console.WriteLine("fieldValue------>" + fieldValue);

                Console.WriteLine("-------------------------------------------------------------------");

            }
        }

        // 属性信息
        static void GetPropertyInfo(Object obj)
        {
            Type type = obj.GetType();
            PropertyInfo[] propertyInfo = type.GetProperties();

            foreach (PropertyInfo p in propertyInfo)
            {

                string propertyName = p.Name;
                string propertyValue = p.GetValue(obj).ToString();
                Console.WriteLine("propertyName------>" + propertyName);
                Console.WriteLine("propertyValue----->" + propertyValue);
                Console.WriteLine("-------------------------------------------------------------------");

            }
        }
        // 方法信息
        static void GetMethodInfo(Object obj)
        {
            Type type = obj.GetType();
            //获取所有public修饰的方法
            MethodInfo[] methodInfo = type.GetMethods();

            foreach (var m in methodInfo)
            {
                string methodName = m.Name;

                Console.WriteLine("methodName------>" + methodName);

                Console.WriteLine("-------------------------------------------------------------------");
            }
        }
        // 成员信息
        static void GetMemberInfo(Object obj)
        {
            Type type = obj.GetType();
            MemberInfo[] memberInfo = type.GetMembers();

            foreach (var m in memberInfo)
            {
                string memberName = m.Name;

                Console.WriteLine("memberName------>" + memberName);

                Console.WriteLine("-------------------------------------------------------------------");
            }
        }
        // 构造方法信息
        static void GetConstructorInfo(Object obj)
        {
            Type type = obj.GetType();
            //获取所有public修饰的构造方法
            ConstructorInfo[] constructorInfo = type.GetConstructors();
            foreach (var c in constructorInfo)
            {
                string constructorName = c.Name;
                ParameterInfo[] constructorParams = c.GetParameters();
                Console.WriteLine("constructorName------>" + constructorName);
                foreach (var p in constructorParams)
                {
                    Console.WriteLine("Params------ p.Name-->" + p.Name);
                    Console.WriteLine("Params------ p.Type--->" + p.ParameterType);
                }
                Console.WriteLine("-------------------------------------------------------------------");

            }
        }
    }
}
