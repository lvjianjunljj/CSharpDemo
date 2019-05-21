using Kusto.Data.Exceptions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureKustoDemo
{
    class KustoServiceExceptionDemo
    {
        public static void MainMethod()
        {
            try
            {
                throw new KustoServiceException("Kusto Service Exception String!!!", new Exception());
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }
    }
}
