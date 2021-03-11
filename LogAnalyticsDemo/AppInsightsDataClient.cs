
//namespace LogAnalyticsDemo
//{
//    using Microsoft.Rest;
//    using Microsoft.Azure.ApplicationInsights;
//    using Microsoft.Azure.ApplicationInsights.Models;

//    using System.Threading.Tasks;
//    using System;
//    using System.Threading;

//    /// <summary>
//    /// The app insights data client class.
//    /// </summary>
//    public class AppInsightsDataClient
//    {
//        /// <summary>
//        /// The application insights data client
//        /// </summary>
//        private IApplicationInsightsDataClient applicationInsightsDataClient;

//        /// <summary>
//        /// Initializes a new instance of the <see cref="AppInsightsDataClient"/> class.
//        /// </summary>
//        /// <param name="credentials">
//        /// The credentials.
//        /// </param>
//        /// <param name="appId">
//        /// The app id.
//        /// </param>
//        public AppInsightsDataClient(ServiceClientCredentials credentials, string appId)
//        {
//            this.applicationInsightsDataClient = new ApplicationInsightsDataClient(credentials)
//            {
//                AppId = appId
//            };
//        }

//        /// <summary>
//        /// The query async.
//        /// </summary>
//        /// <param name="query">
//        /// The query.
//        /// </param>
//        /// <param name="timespan">
//        /// The timespan.
//        /// </param>
//        /// <param name="cancellationToken">
//        /// The cancellation token.
//        /// </param>
//        /// <returns>
//        /// The <see cref="QueryResults"/>.
//        /// </returns>
//        public async Task<QueryResults> QueryAsync(
//            string query,
//            TimeSpan? timespan = null,
//            CancellationToken cancellationToken = default)
//        {
//            return await this.applicationInsightsDataClient.QueryAsync(query, timespan, cancellationToken: cancellationToken).ConfigureAwait(false);
//        }

//        /// <summary>
//        /// The get trace events async.
//        /// </summary>
//        /// <param name="timespan">
//        /// The timespan.
//        /// </param>
//        /// <param name="filter">
//        /// The filter.
//        /// </param>
//        /// <param name="search">
//        /// The search.
//        /// </param>
//        /// <param name="orderby">
//        /// The orderby.
//        /// </param>
//        /// <param name="select">
//        /// The select.
//        /// </param>
//        /// <param name="skip">
//        /// The skip.
//        /// </param>
//        /// <param name="top">
//        /// The top.
//        /// </param>
//        /// <param name="format">
//        /// The format.
//        /// </param>
//        /// <param name="count">
//        /// The count.
//        /// </param>
//        /// <param name="apply">
//        /// The apply.
//        /// </param>
//        /// <param name="cancellationToken">
//        /// The cancellation token.
//        /// </param>
//        /// <returns>
//        /// The <see cref="Task"/>.
//        /// </returns>
//        public async Task<EventsResults<EventsTraceResult>> GetTraceEventsAsync(
//            TimeSpan? timespan = null,
//            string filter = null,
//            string search = null,
//            string orderby = null,
//            string select = null,
//            int? skip = null,
//            int? top = null,
//            string format = null,
//            bool? count = null,
//            string apply = null,
//            CancellationToken cancellationToken = default)
//        {
//            return await this.applicationInsightsDataClient.GetTraceEventsAsync(timespan, filter, search, orderby, select, skip, top, format, count, apply, cancellationToken).ConfigureAwait(false);
//        }
//    }
//}
