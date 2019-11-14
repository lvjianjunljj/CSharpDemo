using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSharpDemo
{
    class DateTimeDemo
    {
        public static void MainMethod()
        {
            Console.WriteLine(GetHourStartTime(DateTime.Now));
            Console.WriteLine(GetDayStartTime(DateTime.Now));
            Console.WriteLine(GetWeekStartTime(DateTime.Now));
            Console.WriteLine(GetMonthStartTime(DateTime.Now));
            Console.WriteLine(GetYearStartTime(DateTime.Now));
        }

        public static void PrintDateTime()
        {
            Console.WriteLine(DateTime.UtcNow.ToString("o"));
            Console.WriteLine(DateTime.UtcNow.ToString("r"));
            Console.WriteLine(DateTime.UtcNow.ToString("u"));
        }

        // Get the start time of the hour/day/week/month/year of the specified date
        public static DateTime GetHourStartTime(DateTime dateTime)
        {
            return new DateTime(dateTime.Year, dateTime.Month, dateTime.Day, dateTime.Hour, 0, 0);
        }

        public static DateTime GetDayStartTime(DateTime dateTime)
        {
            return dateTime.Date;
        }
        public static DateTime GetWeekStartTime(DateTime dateTime)
        {
            return dateTime.Date.AddDays(1 - (int)(DateTime.Now.DayOfWeek) + 1);
        }
        public static DateTime GetMonthStartTime(DateTime dateTime)
        {
            return new DateTime(dateTime.Year, dateTime.Month, 1);
        }
        public static DateTime GetYearStartTime(DateTime dateTime)
        {
            return new DateTime(dateTime.Year, 1, 1);
        }
    }
}
