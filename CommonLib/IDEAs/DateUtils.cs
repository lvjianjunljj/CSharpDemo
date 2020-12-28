namespace CommonLib.IDEAs
{
    using System;

    /// <summary>
    /// Class DateUtils.
    /// </summary>
    public static class DateUtils
    {
        /// <summary>
        /// Gets the previous date.
        /// </summary>
        /// <param name="date">The date.</param>
        /// <param name="type">The type.</param>
        /// <returns>DateTime.</returns>
        /// <exception cref="NotSupportedException">GetMetricValues does not support the comparison type {type}</exception>
        public static DateTime GetPreviousDate(DateTime date, ComparisonType type)
        {
            DateTime previousDate;
            switch (type)
            {
                case ComparisonType.VarianceToTarget:
                    previousDate = date;
                    break;
                case ComparisonType.DayOverDay:
                    previousDate = GetPreviousDate(date, Grain.Daily);
                    break;
                case ComparisonType.WeekOverWeek:
                    previousDate = GetPreviousDate(date, Grain.Weekly);
                    break;
                case ComparisonType.MonthOverMonth:
                    previousDate = GetPreviousDate(date, Grain.Monthly);
                    break;
                case ComparisonType.EndOfMonthOverEndOfMonth:
                    previousDate = GetPreviousDate(date, Grain.EndOfMonth);
                    break;
                case ComparisonType.YearOverYear:
                case ComparisonType.YearOverYearOfMonthOverMonth:
                    previousDate = GetPreviousDate(date, Grain.Yearly);
                    break;
                default:
                    throw new NotSupportedException($"GetPreviousDate does not support the comparison type {type}.");
            }

            return previousDate;
        }

        /// <summary>
        /// Gets the previous date for a given date and grain.
        /// </summary>
        /// <param name="date">The original date.</param>
        /// <param name="grain">The grain for the previous date.</param>
        /// <returns>The previous date.</returns>
        public static DateTime GetPreviousDate(DateTime date, Grain grain)
        {
            DateTime previousDate;
            switch (grain)
            {
                case Grain.Hourly:
                    previousDate = date.AddHours(-1);
                    break;
                case Grain.Daily:
                    previousDate = date.AddDays(-1);
                    break;
                case Grain.Weekly:
                    previousDate = date.AddDays(-7);
                    break;
                case Grain.Monthly:
                    // DateTime.AddMonths(-1) will return the closest valid date of the previous month
                    // E.g.: DateTime('2018-03-31').AddMonths(-1) will return DateTime('2018-02-28')
                    previousDate = date.AddMonths(-1);
                    break;
                case Grain.EndOfMonth:
                    // given timeMinusSLATime, the end date will be the end of month of the previous month
                    var prevDate = date.AddMonths(-1);
                    previousDate = new DateTime(
                        prevDate.Year,
                        prevDate.Month,
                        DateTime.DaysInMonth(prevDate.Year, prevDate.Month),
                        0,
                        0,
                        0,
                        DateTimeKind.Utc);
                    break;
                case Grain.Yearly:
                    previousDate = date.AddYears(-1);
                    break;
                default:
                    throw new NotSupportedException($"GetPreviousDate does not support the grain {grain}.");
            }

            return previousDate;
        }

        /// <summary>
        /// Gets the timespan to add depending on the grain.
        /// </summary>
        /// <param name="date">The date.</param>
        /// <param name="grain">The grain.</param>
        /// <returns>DateTime.</returns>
        /// <exception cref="NotImplementedException">The Grain:{grain}</exception>
        public static DateTime GetNextDate(DateTime date, Grain grain)
        {
            // Depending on the grain of the dataset, iterate through the date range at different rates
            switch (grain)
            {
                case Grain.Hourly:
                    return date.AddHours(1);
                case Grain.Daily:
                    return date.AddDays(1);
                case Grain.Weekly:
                    // We know that the start date will already be on a Sunday,so adding 7 days will also be Sunday
                    return date.AddDays(7);
                case Grain.Monthly:
                    // In the monthly case, we add one month each time but the timespan added depends on the starting month. 
                    // Therefore we cannot just compute the timespan for all months. 
                    return date.AddMonths(1);
                case Grain.EndOfMonth:
                    // GetStartDate will always go for the first end of month given this.StartDate, dataset.StartDate, and rolling window
                    // So GetNextDate only needs to go for the end of next month
                    var nextDate = date.AddMonths(1);
                    return new DateTime(
                        nextDate.Year,
                        nextDate.Month,
                        DateTime.DaysInMonth(nextDate.Year, nextDate.Month),
                        0,
                        0,
                        0,
                        DateTimeKind.Utc);
                default:
                    throw new NotImplementedException(
                        $"The Grain:{grain} is not supported.");
            }
        }

        /// <summary>
        /// Gets the earlist date that should be available if we are in SLA.
        /// </summary>
        /// <param name="slaString">The SLA specified.</param>
        /// <param name="grain">The grain to use.</param>
        /// <returns>The latest data within SLA.</returns>
        public static DateTime GetSLADate(string slaString, Grain grain)
        {
            int sla = 0;
            if (!int.TryParse(slaString, out sla))
            {
                throw new ArgumentException($"GetSLADate encountered non-integer SLA string {slaString}.");
            }

            DateTime slaDate;
            switch (grain)
            {
                case Grain.Hourly:
                    slaDate = DateTime.UtcNow - TimeSpan.FromHours(sla);
                    break;
                case Grain.Daily:
                    slaDate = DateTime.UtcNow - TimeSpan.FromDays(sla);
                    break;
                case Grain.Weekly:
                    slaDate = DateTime.UtcNow - TimeSpan.FromDays(sla * 7);
                    break;
                case Grain.Monthly:
                    slaDate = DateTime.UtcNow - TimeSpan.FromDays(sla * 30);
                    break;
                case Grain.EndOfMonth:
                    // Not really sure what the right value would be here.
                    slaDate = DateTime.UtcNow - TimeSpan.FromDays(sla * 30);
                    break;
                default:
                    throw new NotSupportedException($"GetSLADate does not support the grain {grain}.");
            }

            return slaDate;
        }

        /// <summary>
        /// Round the date according to the grain, the returned DateTime is in UTC timezone
        /// </summary>
        /// <param name="date">The date</param>
        /// <param name="grain">The grain</param>
        /// <returns>Rounded DateTime in UTC timezone</returns>
        public static DateTime RoundDate(DateTime date, Grain grain)
        {
            DateTime roundedDate = date;

            switch (grain)
            {
                case Grain.Hourly:
                    roundedDate = new DateTime(
                        date.Year,
                        date.Month,
                        date.Day,
                        date.Hour,
                        0,
                        0,
                        DateTimeKind.Utc);
                    break;
                case Grain.Daily:
                    roundedDate = new DateTime(
                        date.Year,
                        date.Month,
                        date.Day,
                        0,
                        0,
                        0,
                        DateTimeKind.Utc);
                    break;
                case Grain.Weekly:
                    var week = date.Date.AddDays(-(int)date.DayOfWeek);
                    roundedDate = new DateTime(
                        week.Year,
                        week.Month,
                        week.Day,
                        0,
                        0,
                        0,
                        DateTimeKind.Utc);
                    break;
                case Grain.Monthly:
                    roundedDate = new DateTime(
                        date.Year,
                        date.Month,
                        1,
                        0,
                        0,
                        0,
                        DateTimeKind.Utc);
                    break;
                case Grain.EndOfMonth:
                    roundedDate = new DateTime(
                        date.Year,
                        date.Month,
                        DateTime.DaysInMonth(date.Year, date.Month),
                        0,
                        0,
                        0,
                        DateTimeKind.Utc);
                    break;
            }

            return roundedDate;
        }
    }
}
