using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSharpDemo.DISampleDemo
{
    class SystemTimeProvider : ITimeProvider
    {
        public DateTime CurrentDate { get { return DateTime.Now; } }
    }
    class UtcNowTimeProvider : ITimeProvider
    {
        public DateTime CurrentDate { get { return DateTime.UtcNow; } }
    }
}
