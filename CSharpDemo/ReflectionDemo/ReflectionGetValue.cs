using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Reflection;

namespace CSharpDemo.ReflectionDemo
{
    class ReflectionGetValue
    {
        public static void MainMethod()
        {
            //WorkerModelTest();
            GetValueToStringTest();
        }
        private static void WorkerModelTest()
        {
            WorkerModel wm = new WorkerModel("name", 1, GenderEnum.Girl, true);
            wm.Name = "Name";
            wm.Gender = GenderEnum.Dog;
            wm.StringList = new List<string>();
            wm.StringList.Add("1234");
            GetPropertyInfo(wm);
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

        // ReflectionTest r = new ReflectionTest();
        // r.Test();
        public void Test()
        {
            Func<object, string> fuc = new Func<object, string>(FuncBook3);

            // The first binding flag is to call public function and the next two is to call private function.
            MethodInfo method = this.GetType().GetMethod("FuncBook3", BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance);
            if (method != null)
            {
                // new Program() is for this.
                fuc = (Func<object, string>)Delegate.CreateDelegate(typeof(Func<object, string>), this, method);
            }
            else
            {
                Console.WriteLine("method is null");
            }
            string res = fuc.Invoke(123);
            Console.WriteLine(res);
        }

        public string FuncBook(object o1, object o2)
        {
            return o1 + "送书来了1" + o2;
        }
        private string FuncBook2(object o1)
        {
            return o1 + "送书来了2";
        }
        public string FuncBook3(object o1)
        {
            return o1 + "送书来了3";
        }

        enum GenderEnum
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
            public IList<string> StringList { get; set; }

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

        static void GetValueToStringTest()
        {
            ReflectionTest test = new ReflectionTest();
            test.Set = new HashSet<string> { "1", "2" };
            test.Dict = new Dictionary<string, string>
            {
                ["1"] = "1",
                ["2"] = "2",
                ["3"] = null,
            };
            Console.WriteLine(test.Dict.ContainsKey("3"));
            test.Array = new string[] { "1", "2" };
            test.Char = '1';
            test.Boolean = true;
            test.Enum = ReflectionEnum.One;
            var properties = test.GetType().GetProperties(BindingFlags.Instance | BindingFlags.Public | BindingFlags.GetProperty);

            Console.WriteLine(JsonConvert.SerializeObject(test));
            Console.WriteLine("---------------");

            //object property = setProperty.GetValue(test);

            foreach (var property in properties)
            {
                var type = property?.GetValue(test)?.GetType();
                string propertyStr = ConvertPropertyValueToString(property?.GetValue(test));

                Console.WriteLine(type);
                Console.WriteLine(property?.GetValue(test)?.ToString());
                Console.WriteLine(propertyStr);
                Console.WriteLine("-------------------");
            }
        }

        // This is an important function when we use reflection to get the property value 
        // to avoid getting type tostring like: System.Collections.Generic.HashSet`1[System.String]
        private static string ConvertPropertyValueToString(object propertyValueObject)
        {
            if (propertyValueObject == null)
            {
                return string.Empty;
            }

            if (propertyValueObject is string || propertyValueObject is char || propertyValueObject is Enum)
            {
                return propertyValueObject.ToString();
            }

            return JsonConvert.SerializeObject(propertyValueObject);
        }

        class ReflectionTest
        {
            public HashSet<string> Set { get; set; }
            public Dictionary<string, string> Dict { get; set; }
            public string[] Array { get; set; }
            public string Str { get; set; }
            public char Char { get; set; }
            public bool Boolean { get; set; }
            public float Float { get; set; }
            public int Int { get; set; }
            public ReflectionEnum Enum { get; set; }
        }

        enum ReflectionEnum
        {
            One = 1,
            Two = 2,
            Threee = 3
        }
    }

}
