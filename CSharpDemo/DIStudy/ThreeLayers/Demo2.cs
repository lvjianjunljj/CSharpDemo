using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSharpDemo.DIStudy.ThreeLayers
{
    // I think DI is just turning dependency on class into dependency on interface,
    // Then we can use different implement class of this interface for this class in the main process.
    // Note: we cannot define static members on an interface in C#. An interface is a contract for instances
    class Demo2
    {
        public static void Run()
        {
            Console.WriteLine($"开始运行{nameof(Demo2)}");
            // 使用 StudentDal1
            IStudentBll studentBll = new StudentBll(new StudentDal1());
            IEnumerable<Student> students = studentBll.GetStudents();
            foreach (Student student in students)
            {
                Console.WriteLine(student);
            }
            // 使用 StudentDal2
            studentBll = new StudentBll(new StudentDal2());
            students = studentBll.GetStudents();
            foreach (Student student in students)
            {
                Console.WriteLine(student);
            }
            Console.WriteLine($"结束运行{nameof(Demo2)}");
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
