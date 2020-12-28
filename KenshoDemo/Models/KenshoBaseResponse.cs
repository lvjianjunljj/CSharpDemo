﻿namespace KenshoDemo.Models
{
    using System.Net;
    public class KenshoBaseResponse
    {
        /// <summary>
        /// This gets or sets the status code returned.
        /// </summary>
        public HttpStatusCode StatusCode { get; set; }

        /// <summary>
        /// This will contain the returned content or any error message generated by the request.
        /// </summary>
        public string ContentOrErrorMessage { get; set; }
    }
}