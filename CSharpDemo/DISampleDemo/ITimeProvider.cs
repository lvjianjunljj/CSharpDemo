using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSharpDemo.DISampleDemo
{
    interface ITimeProvider
    {
        DateTime CurrentDate { get; }
    }
}
