namespace CommonLib.IDEAs
{
    using System;
    using System.Collections.Generic;
    using System.Text.RegularExpressions;

    /// <summary>
    /// Class StreamSetUtils.
    /// </summary>
    public static class StreamSetUtils
    {
        /// <summary>
        /// Generates the streamset paths.
        /// </summary>
        /// <param name="streamPath">The stream path.</param>
        /// <param name="startDate">The start date.</param>
        /// <param name="endDate">The end date.</param>
        /// <param name="startSerialNum">The start serial number.</param>
        /// <param name="endSerialNum">The end serial number.</param>
        /// <param name="grain">The grain.</param>
        /// <returns>List&lt;string&gt;.</returns>
        /// <exception cref="System.ArgumentNullException">
        /// startDate
        /// or
        /// endDate
        /// or
        /// startSerialNum or endSerialNum is null
        /// </exception>
        public static List<string> GenerateStreamsetPaths(string streamPath, DateTime? startDate, DateTime? endDate, int? startSerialNum, int? endSerialNum, Grain grain)
        {
            if (startDate == null)
            {
                throw new ArgumentNullException(nameof(startDate));
            }

            if (endDate == null)
            {
                throw new ArgumentNullException(nameof(endDate));
            }

            bool requireSerial = streamPath.Contains("%n");
            if (requireSerial && (startSerialNum == null || endSerialNum == null))
            {
                throw new ArgumentNullException("startSerialNum or endSerialNum is null");
            }

            var streams = new List<string>();
            var dates = GenerateDates((DateTime)startDate, (DateTime)endDate, grain);
            foreach (var date in dates)
            {
                var year = date.Year.ToString();
                var month = date.Month.ToString("00");
                var day = date.Day.ToString("00");
                var hour = date.Hour;
                var stream = CreateStreamPath(streamPath, year, month, day, hour);
                if (requireSerial)
                {
                    for (int currentNum = (int)startSerialNum; currentNum <= (int)endSerialNum; currentNum++)
                    {
                        var path = stream.Replace("%n", currentNum.ToString());
                        streams.Add(path);
                    }
                }
                else
                {
                    streams.Add(stream);
                }
            }

            return streams;
        }

        private static string CreateStreamPath(string streamPath, string year, string month, string day, int hour)
        {
            string pathWithYear = streamPath.Replace("%Y", year);
            string pathWithYearMonth = pathWithYear.Replace("%m", month);
            string pathWithYearMonthDay = pathWithYearMonth.Replace("%d", day);
            string pathWithYearMonthDayHour = InsertHourlyValues(pathWithYearMonthDay, hour);
            return pathWithYearMonthDayHour;
        }

        private static string InsertHourlyValues(string pathWithYearMonthDay, int hour)
        {
            // Handle both versions of hourly string replacements
            // HH refers to 24 hour clock, hh to 12 hour clock. See https://docs.microsoft.com/en-us/dotnet/standard/base-types/custom-date-and-time-format-strings for more details
            return pathWithYearMonthDay.Replace("%h", hour.ToString("00")).Replace("{0:HH}", hour.ToString("00")).Replace("{0:hh}", (hour % 12).ToString("00"));
        }

        /// <summary>
        /// Generates the dates.
        /// </summary>
        /// <param name="startDate">The start date.</param>
        /// <param name="endDate">The end date.</param>
        /// <param name="grain">The grain.</param>
        /// <returns>List&lt;DateTime&gt;.</returns>
        private static List<DateTime> GenerateDates(DateTime startDate, DateTime endDate, Grain grain)
        {
            var currentDate = startDate;
            var dates = new List<DateTime>();
            while (currentDate <= endDate)
            {
                dates.Add(currentDate);
                currentDate = DateUtils.GetNextDate(currentDate, grain);
            }

            return dates;
        }

        /// <summary>
        /// Determines whether the path represents a streamset.
        /// </summary>
        /// <param name="streamPath">The stream path.</param>
        /// <returns><c>true</c> if [is test run over stream set] [the specified dataset]; otherwise, <c>false</c>.</returns>
        public static bool IsPathAStreamSet(string streamPath)
        {
            var streamSetRegex = @"[-_/]?%[Ymdhn]";
            return Regex.IsMatch(streamPath, streamSetRegex);
        }
    }
}
