using System;
using System.Collections.Generic;
using Autofac;

namespace CSharpDemo.DIStudy.ThreeLayers
{
    class Demo3
    {
        // As for Autofac, I think we just can use constructor injection not setter injection.
        // And I think Attribute injection is so complex.
        public static void Run()
        {
            Console.WriteLine($"开始运行{nameof(Demo3)}");
            // 使用 StudentDal1
            ContainerBuilder cb = new ContainerBuilder();
            cb.RegisterType<StudentBll>().As<IStudentBll>();
            cb.RegisterType<StudentDal1>().As<IStudentDal>();
            IContainer container = cb.Build();

            IStudentBll studentBll = container.Resolve<IStudentBll>();
            IStudentDal studentDal = container.Resolve<IStudentDal>();
            IEnumerable<Student> students = studentBll.GetStudents();
            foreach (Student student in students)
            {
                Console.WriteLine(student);
            }
            // 使用 StudentDal2
            cb = new ContainerBuilder();
            cb.RegisterType<StudentBll>().As<IStudentBll>();
            cb.RegisterType<StudentDal2>().As<IStudentDal>();
            container = cb.Build();

            studentBll = container.Resolve<IStudentBll>();
            students = studentBll.GetStudents();
            foreach (Student student in students)
            {
                Console.WriteLine(student);
            }
            Console.WriteLine($"结束运行{nameof(Demo3)}");
        }

        public interface IStudentBll
        {
            IEnumerable<Student> GetStudents();
        }

        public class StudentBll : IStudentBll
        {
            private readonly IStudentDal _studentDal;

            /**
             * 通过构造函数传入一个 IStudentDal 这种方式称为“构造函数注入”
             * 使用构造函数注入的方式，使得不依赖于特定的 IStudentDal 实现。
             * 只要 IStudentDal 接口的定义不修改，该类就不需要修改，实现了DAL与BLL的解耦
             */
            public StudentBll(
                IStudentDal studentDal)
            {
                _studentDal = studentDal;
            }

            public IEnumerable<Student> GetStudents()
            {
                IEnumerable<Student> re = _studentDal.GetStudents();
                return re;
            }
        }

        public interface IStudentDal
        {
            IEnumerable<Student> GetStudents();
        }

        public class StudentDal1 : IStudentDal
        {
            private readonly IList<Student> _studentList = new List<Student>
            {
                new Student
                {
                    Id = "12",
                    Name = "Traceless",
                }
            };

            public IEnumerable<Student> GetStudents()
            {
                return _studentList;
            }
        }

        public class StudentDal2 : IStudentDal
        {
            private readonly IList<Student> _studentList = new List<Student>
            {
                new Student
                {
                    Id = "11",
                    Name = "月落"
                }
            };

            public IEnumerable<Student> GetStudents()
            {
                return _studentList;
            }
        }

        public class Student
        {
            public string Id { get; set; }
            public string Name { get; set; }

            public override string ToString()
            {
                return $"{nameof(Id)}: {Id}, {nameof(Name)}: {Name}";
            }
        }
    }
}
