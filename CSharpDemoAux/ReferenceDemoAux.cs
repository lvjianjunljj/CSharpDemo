using CSharpDemoReferenceAux;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSharpDemoAux
{
    //ReferenceDemoAux referenceDemoAux = new ReferenceDemoAux();
    //referenceDemoAux.TestReferenceError();
    public class ReferenceDemoAux
    {
        public void TestReferenceError()
        {
            ReferenceAuxClass referenceAuxClass = this.GetReferenceAuxClass();
            Console.WriteLine(referenceAuxClass.AuxString);
        }
        public ReferenceAuxClass GetReferenceAuxClass()
        {
            Console.WriteLine(12341234);
            ReferenceAuxClass referenceAuxClass = new ReferenceAuxClass();
            referenceAuxClass.AuxString = "AuxString";
            return referenceAuxClass;
        }
    }
}
