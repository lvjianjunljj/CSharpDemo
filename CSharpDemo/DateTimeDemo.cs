namespace CSharpDemo
{
    using Newtonsoft.Json;
    using System;

    // OneNote link: https://microsoftapc-my.sharepoint.com/personal/jianjlv_microsoft_com/_layouts/OneNote.aspx?id=%2Fpersonal%2Fjianjlv_microsoft_com%2FDocuments%2FJianjun%20%40%20Microsoft&wd=target%28Develop%20Lang.one%7C5B97F099-2C06-420B-BE08-9DDC5BC0F237%2FC%23%E6%97%A5%E6%9C%9F%E6%A0%BC%E5%BC%8F%E8%BD%AC%E6%8D%A2%7C22A592C4-E04E-43B1-A9EB-71515D67ACDD%2F%29
    class DateTimeDemo
    {
        public static void MainMethod()
        {
            PrintDateTime();
            //GetStartTimeDemo();
            //CompareWithJsonConvertSerializeObject();
            //TimeZoneDemo();
        }

        public static void TimeZoneDemo()
        {
            Console.WriteLine("DateTime.UtcNow.ToString(\"o\") demo");
            string timeStringO = DateTime.UtcNow.ToString("o");
            Console.WriteLine($"UtcNow.ToString(\"o\"): {timeStringO}");
            // Note: 
            // When we parse the time string contains TimeZoneInfo(generate from ToString("o")), 
            // it will return the dateTime based on the local timeZone.
            // The ToString result of DateTime.UtcNow ends with "Z"
            DateTime timeParseO = DateTime.Parse(timeStringO);
            Console.WriteLine($"DateTime.Parse().ToString(\"o\"): {timeParseO:o}");
            Console.WriteLine();

            Console.WriteLine("DateTime.Now.ToString(\"o\") demo");
            timeStringO = DateTime.Now.ToString("o");
            // The ToString result of DateTime.Now ends with "+08:00"
            Console.WriteLine($"Now.ToString(\"o\"): {timeStringO}");
            timeParseO = DateTime.Parse(timeStringO);
            Console.WriteLine($"DateTime.Parse().ToString(\"o\"): {timeParseO:o}");
            Console.WriteLine();

            Console.WriteLine("DateTime.UtcNo.ToString(\"s\") demo");
            string timeStringS = DateTime.UtcNow.ToString("s");
            Console.WriteLine($"UtcNow.ToString(\"s\"): {timeStringS}");
            // DateTime.UtcNow.ToString("s") does not contains TimeZoneInfo, 
            // so parse functio can return the same value with the source DateTime but without timeZone info
            // Actually it has been different from the source dateTime if we use ToUniversalTime() function to convert it to UniversalTime(By default with local TimeZone if not contains TimeZone info)
            DateTime timeParseS = DateTime.Parse(timeStringS);
            Console.WriteLine($"DateTime.Parse().ToString(\"o\"): {timeParseS:o}");
            Console.WriteLine($"DateTime.Parse().ToUniversalTime().ToString(\"o\"): {timeParseS.ToUniversalTime():o}");
            Console.WriteLine();

            // We can use function TimeZoneInfo.ConvertTimeToUtc or ToUniversalTime() to convert the DateTime with local TimeZone info or without TimeZone info to UniversalTime()
            Console.WriteLine("ToUniversalTime() demo");
            Console.WriteLine($"UtcNow.ToString(\"s\"): {timeStringS}");
            Console.WriteLine($"DateTime.Parse().ToString(\"o\"): {timeParseS:o}");
            Console.WriteLine($"DateTime.Parse().ToUniversalTime().ToString(\"o\"): {timeParseS.ToUniversalTime():o}");
            Console.WriteLine($"TimeZoneInfo.ConvertTimeToUtc(DateTime.Parse()).ToString(\"o\"): {TimeZoneInfo.ConvertTimeToUtc(timeParseS):o}");
            Console.WriteLine();

            Console.WriteLine("DateTime.UtcNo.ToString(\"s\") append 'Z' demo");
            // You can just append a letter 'Z' to set the TimeZoneInfo Zulu Time Zone for the date string without TimeZoneInfo
            // And this operation won't change the TimeZoneInfo for the date string with TimeZoneInfo
            string timeStringSEndZ = timeStringS + "Z";
            DateTime timeParseSEndZ = DateTime.Parse(timeStringSEndZ).ToUniversalTime();
            Console.WriteLine($"UtcNow.ToString(\"s\") + \"Z\": {timeStringSEndZ}");
            Console.WriteLine($"DateTime.Parse().ToUniversalTime().ToString(\"o\"): {timeParseSEndZ:o}");


            string timeStringSEndTimeZoneZ = timeStringS + "+08:00Z";
            DateTime timeParseSEndTimeZoneZ = DateTime.Parse(timeStringSEndZ).ToUniversalTime();
            Console.WriteLine($"UtcNow.ToString(\"s\") + \"Z\": {timeStringSEndTimeZoneZ}");
            Console.WriteLine($"DateTime.Parse().ToUniversalTime().ToString(\"o\"): {timeParseSEndTimeZoneZ:o}");
            Console.WriteLine();

            string timeStringWithHourInfo = DateTime.UtcNow.ToString("yyyy-MM-dd") + "Z";
            timeStringWithHourInfo = "2018-10-24 22:04:42Z";
            DateTime timeParseWithHourInfo = DateTime.Parse(timeStringWithHourInfo).ToUniversalTime();
            Console.WriteLine($"UtcNow.ToString(\"s\") + \"Z\": {timeStringWithHourInfo}");
            Console.WriteLine($"DateTime.Parse().ToUniversalTime().ToString(\"o\"): {timeParseWithHourInfo:o}");

            /* Note:
             * This kind of dateText "2018-10-24 22:04:42" cannot append time zone info.
             * Just this kind of dateText "2018-10-24T22:04:42" can append time zone info.
             * Just like this: 
             * "2018-10-24T22:04:42Z"
             * "2018-10-24T22:04:42+8:00Z"
             */
            try
            {
                Console.WriteLine(DateTime.Parse("08/18/2018 07:22:16-07:00Z").ToUniversalTime());
            }
            catch (FormatException)
            {
                Console.WriteLine("FormatException");
                Console.WriteLine(DateTime.Parse("2018-10-11T00:00:00Z").ToUniversalTime());
            }

            Console.WriteLine();
            Console.WriteLine(DateTime.Parse("2018-10-11T00:00:00Z").ToUniversalTime().ToString());
            Console.WriteLine(DateTime.Parse("2018-10-11T00:00:00Z").ToString());
        }

        public static void CompareWithJsonConvertSerializeObject()
        {
            DateTime dateNow = DateTime.UtcNow;

            Console.WriteLine("Datetime without Millisecond print schema");
            var timeStampWithoutMillisecond = new DateTime(dateNow.Year, dateNow.Month, dateNow.Day, dateNow.Hour, dateNow.Minute, dateNow.Second);
            // The DateTime string from JsonConvert.SerializeObject is the same as ToString("s") when the date does not contain Millisecond
            Console.WriteLine(JsonConvert.SerializeObject(new DateTestClass() { TimeStamp = timeStampWithoutMillisecond }));
            Console.WriteLine(timeStampWithoutMillisecond.ToString("o"));
            Console.WriteLine(timeStampWithoutMillisecond.ToString("s"));


            Console.WriteLine("Datetime with Millisecond print schema");
            var timeStampWithMillisecond = dateNow;
            // The DateTime string from JsonConvert.SerializeObject is the same as ToString("o") when the date contains Millisecond
            Console.WriteLine(JsonConvert.SerializeObject(new DateTestClass() { TimeStamp = timeStampWithMillisecond }));
            Console.WriteLine(timeStampWithMillisecond.ToString("o"));
            Console.WriteLine(timeStampWithMillisecond.ToString("s"));
        }

        public static void GetStartTimeDemo()
        {
            Console.WriteLine(GetHourStartTime(DateTime.Now));
            Console.WriteLine(GetDayStartTime(DateTime.Now));
            Console.WriteLine(GetWeekStartTime(DateTime.Now));
            Console.WriteLine(GetMonthStartTime(DateTime.Now));
            Console.WriteLine(GetYearStartTime(DateTime.Now));
        }

        public static void PrintDateTime()
        {

            // 12/30/2020 3:26:51 PM
            Console.WriteLine(DateTime.UtcNow.ToString());
            // 2020-12-30T15:26:51.0974633Z
            Console.WriteLine(DateTime.UtcNow.ToString("o"));
            // Wed, 30 Dec 2020 15:26:51 GMT
            Console.WriteLine(DateTime.UtcNow.ToString("r"));
            // 2020 - 12-30 15:26:51Z
            Console.WriteLine(DateTime.UtcNow.ToString("u"));
            // 2020 - 12-30T15:26:51
            Console.WriteLine(DateTime.UtcNow.ToString("s"));
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
        private class DateTestClass
        {
            [JsonProperty(PropertyName = "timeStamp")]
            public DateTime TimeStamp { get; set; }
        }
    }
}
