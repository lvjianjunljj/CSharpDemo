namespace CSharpDemo.RetrierDir
{
    using System;
    using System.Threading;

    /// <summary>
    /// Class Retrier.
    /// </summary>
    public static class Retrier
    {
        /// <summary>
        /// The default maximum tries
        /// </summary>
        private const int DefaultMaxTries = 10;

        /// <summary>
        /// Retries the specified action.
        /// </summary>
        /// <param name="action">The action.</param>
        /// <param name="maxTries">The maximum tries.</param>
        public static void Retry(Action action, int maxTries = DefaultMaxTries)
        {
            Func<int> func = () => { action(); return 0; };
            Retrier.Retry<int, Exception>(func, maxTries);
        }

        /// <summary>
        /// Retries the specified function.
        /// </summary>
        /// <typeparam name="TResult">The type of the t result.</typeparam>
        /// <param name="func">The function.</param>
        /// <param name="maxTries">The maximum tries.</param>
        /// <returns>TResult.</returns>
        public static TResult Retry<TResult>(Func<TResult> func, int maxTries = DefaultMaxTries)
        {
            return Retrier.Retry<TResult, Exception>(func, maxTries);
        }

        /// <summary>
        /// Retries the specified function.
        /// </summary>
        /// <typeparam name="TResult">The type of the t result.</typeparam>
        /// <typeparam name="TException">The type of the t exception.</typeparam>
        /// <param name="func">The function.</param>
        /// <param name="maxTries">The maximum tries.</param>
        /// <returns>T.</returns>
        /// <exception cref="InvalidOperationException">Unreachable code</exception>
        public static TResult Retry<TResult, TException>(
            Func<TResult> func,
            int maxTries = DefaultMaxTries)
            where TException : Exception
        {
            int maxWaitSeconds = 1;
            Random random = new Random();
            for (int tryIndex = 0; tryIndex < maxTries; tryIndex++)
            {
                try
                {
                    var returnValue = func();
                    return returnValue;
                }
                catch (TException e)
                {
                    // If we have retries left
                    if (tryIndex < maxTries - 1)
                    {
                        Console.WriteLine($"{e.ToString()}");
                        var waitSeconds = random.Next(maxWaitSeconds);
                        Console.WriteLine($"Waiting {waitSeconds} seconds before retrying...");
                        Thread.Sleep(1000 * waitSeconds);
                        // Double the max wait next time (exponential backoff pattern). 
                        // https://en.wikipedia.org/wiki/Exponential_backoff
                        maxWaitSeconds *= 2;
                    }
                    else
                    {
                        throw;
                    }
                }
            }

            throw new InvalidOperationException("Unreachable code");
        }
    }
}
