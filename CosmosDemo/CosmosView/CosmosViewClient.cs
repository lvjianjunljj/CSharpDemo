namespace CosmosDemo.CosmosView
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Text;
    using VcClient;

    class CosmosViewClient
    {
        /// <summary>
        /// This is the string that cosmos uses to format its exceptions, we can use it to get at the real compilation error
        /// </summary>
        private static string CosmosErrorWrapString = "**************";

        /// <summary>
        /// Checks view availability 
        /// </summary>
        /// <param name="testRun">The testRun used to generate the view calling script</param>
        /// <param name="output">An output parameter, used to capture the exception message</param>
        /// <returns>True, the compilation is a success; false, the compilation failed</returns>
        public static bool CheckViewAvailability(string scriptToCompile, out string output)
        {
            var certificate = CertificateGenerator.GetCertificateByThumbprint();
            VC.Setup(null, certificate);

            var scriptFilePath = $"./TheScript_{Guid.NewGuid()}.script";
            var isSuccessful = true;
            string exceptionMessage = "Succesful Compilation";
            try
            {
                File.WriteAllText(scriptFilePath, scriptToCompile);
                try
                {
                    ScopeClient.Scope.Compile(scriptFilePath);
                }
                // This exception catches the following conditions:
                // The View is not at the location specified.
                // The data behind the view is not available.
                // The View itself has error handling, and we have run into a view defined issue.
                // The view itself has an error within it during compilation
                catch (ScopeClient.CompilationErrorException e)
                {
                    var trimmedCompilationError = GetCosmosCompileErrorMessage(e.Message);

                    exceptionMessage = trimmedCompilationError;

                }
            }
            finally
            {
                if (File.Exists(scriptFilePath))
                {
                    File.Delete(scriptFilePath);
                }
            }

            output = exceptionMessage;
            return isSuccessful;
        }

        /// <summary>
        /// Gets the top level cosmos compilation exception message from the entire exception.
        /// </summary>
        /// <param name="cosmosCompilationExceptionMessage">The entire cosmos compilation exception</param>
        /// <returns>The substring representing the actual compilation error that a user can act on</returns>
        private static string GetCosmosCompileErrorMessage(string cosmosCompilationExceptionMessage)
        {
            var wrapperIndeces = cosmosCompilationExceptionMessage.LastIndexOf(CosmosErrorWrapString, StringComparison.InvariantCultureIgnoreCase);

            return cosmosCompilationExceptionMessage.Substring(0, wrapperIndeces);
        }

        /// <summary>
        /// Build the script for the Cosmos View availability test
        /// </summary>
        /// <param name="viewPath">The path of the View</param>
        /// <param name="viewParameters">The parameters of this View</param>
        /// <returns>The script for the Cosmos View availability test</returns>
        public static string BuildScriptForViewAvailabilityTest(string viewPath, List<CosmosViewParameter> viewParameters)
        {
            var stringBuilder = new StringBuilder();
            stringBuilder.Append("ViewSamples = ");
            stringBuilder.Append("VIEW \"" + viewPath + "\"");
            if (viewParameters == null || viewParameters.Count == 0)
            {
                stringBuilder.Append(";");
            }
            else
            {
                stringBuilder.Append("PARAMS");
                stringBuilder.Append("(");

                var parameterPart = new List<string>();
                foreach (var parameter in viewParameters)
                {
                    if (parameter.Value.StartsWith("@@"))
                    {
                        // For external parameter, in the availability test, we only support TestDate in DateTime type
                        if (parameter.Type == Type.GetType("System.DateTime"))
                        {
                            parameterPart.Add($"{parameter.Name} = DateTime.Parse(@@TestDate@@)");
                        }
                        else
                        {
                            throw new InvalidOperationException($"The type of test date parameter can only be System.DateTime, while for {viewPath}, it is {parameter.Type}: {parameter.Value}");
                        }
                    }
                    else
                    {
                        // https://microsoft.sharepoint.com/teams/Cosmos/Wiki/Scope%20Data%20Types.aspx
                        if (parameter.Type == Type.GetType("System.String"))
                        {
                            parameterPart.Add($"{parameter.Name} = \"{parameter.Value}\"");
                        }
                        else if (parameter.Type == Type.GetType("System.DateTime"))
                        {
                            if (DateTime.TryParse(parameter.Value, out DateTime temp))
                            {
                                parameterPart.Add($"{parameter.Name} = DateTime.Parse(\"{parameter.Value}\")");
                            }
                            else
                            {
                                throw new InvalidOperationException($"Mark {parameter.Name} as System.DateTime, while its default value {parameter.Value} cannot be converted to System.DateTime");
                            }
                        }
                        else
                        {
                            parameterPart.Add($"{parameter.Name} = {parameter.Value}");
                        }
                    }
                }

                stringBuilder.Append(string.Join(", ", parameterPart));
                stringBuilder.Append(");");
            }

            stringBuilder.Append("OUTPUT ViewSamples TO \"/my/output.tsv\" USING DefaultTextOutputter();");

            return stringBuilder.ToString();
        }
    }
}
