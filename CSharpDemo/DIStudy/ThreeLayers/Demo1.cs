using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSharpDemo.DIStudy.ThreeLayers
{
    class Demo1
    {
        public static void Run()
        {
            Console.WriteLine($"开始运行{nameof(Demo1)}");
            StudentBll studentBll = new StudentBll();
            IEnumerable<Student> students = studentBll.GetStudents();
            foreach (Student student in students)
            {
                Console.WriteLine(student);
            }
            Console.WriteLine($"结束运行{nameof(Demo1)}");
        }

        public class StudentBll
        {
            public IEnumerable<Student> GetStudents()
            {
                StudentDal studentDal = new StudentDal();
                IEnumerable<Student> re = studentDal.GetStudents();
                return re;
            }
        }

        public class StudentDal
        {
            private readonly IList<Student> _studentList = new List<Student>
            {
                new Student
                {
                    Id = "11",
                    Name = "月落"
                },
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
