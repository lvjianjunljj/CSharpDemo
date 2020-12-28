namespace CommonLib.IDEAs
{
    /// <summary>
    /// Enum Grain
    /// </summary>
    public enum Grain
    {
        /// <summary>
        /// Non-Value
        /// </summary>
        None = 0,

        /// <summary>
        /// Hourly Grain - Once per hour
        /// </summary>
        Hourly,

        /// <summary>
        /// Daily Grain - Once per day
        /// </summary>
        Daily,

        /// <summary>
        /// Weeekly Grain - Once per week
        /// </summary>
        Weekly,

        /// <summary>
        /// Monthly Grain - Once per month (always on the first of every month)
        /// </summary>
        Monthly,

        /// <summary>
        /// Monthly Grain - Once per month (always test at the end of every month)
        /// </summary>
        EndOfMonth,

        /// <summary>
        /// Yearly Grain - Once per year
        /// </summary>
        Yearly
    }

    /// <summary>
    /// Enum ComparisonType
    /// </summary>
    public enum ComparisonType
    {
        /// <summary>
        /// The none
        /// </summary>
        None = 0,

        /// <summary>
        /// Comparison only considers a single day, used to compare against expected values
        /// </summary>
        SingleDay,

        /// <summary>
        /// Comparison of day over adjacent day
        /// </summary>
        DayOverDay,

        /// <summary>
        /// Comparison of week over adjacent week
        /// </summary>
        WeekOverWeek,

        /// <summary>
        /// Comparison of month over adjacent month
        /// </summary>
        MonthOverMonth,

        /// <summary>
        /// Comparison of year over adjacent year
        /// </summary>
        YearOverYear,

        /// <summary>
        /// Comparison of year over year of month compared to adjacent month
        /// </summary>
        YearOverYearOfMonthOverMonth,

        /// <summary>
        /// The variance to target percentage (VTT)
        /// </summary>
        VarianceToTarget,

        /// <summary>
        /// Comparison of end of month over adjacent end of month
        /// </summary>
        EndOfMonthOverEndOfMonth,
    }
}
