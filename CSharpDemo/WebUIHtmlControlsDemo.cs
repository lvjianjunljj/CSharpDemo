using System;
using System.IO;
using System.Text;
using System.Web.UI;
using System.Web.UI.HtmlControls;

namespace CSharpDemo
{
    class WebUIHtmlControlsDemo
    {
        static void MainMethod(string[] args)
        {
            HtmlGenericControl htmlControl = new HtmlGenericControl("html");
            HtmlForm hf = new HtmlForm();
            StringBuilder sb = new StringBuilder();
            StringWriter sw = new StringWriter(sb);
            HtmlTextWriter htw = new HtmlTextWriter(sw);
            htmlControl.RenderControl(htw);
            Console.WriteLine(sw.ToString());
            Console.WriteLine(sb);


            string contents = null;
            using (StringWriter swriter = new StringWriter())
            {
                HtmlTextWriter writer = new HtmlTextWriter(swriter);
                htmlControl.RenderControl(writer);
                contents = swriter.ToString();
            }
            Console.WriteLine(contents);


            Control ht = new HtmlTable();
            HtmlTableRow htr = new HtmlTableRow();
            HtmlTableCell cell1 = new HtmlTableCell();
            HtmlTableCell cell2 = new HtmlTableCell();
            cell1.Controls.Add(new LiteralControl("1"));
            cell2.Controls.Add(new LiteralControl("2"));
            htr.Controls.Add(cell1);
            htr.Controls.Add(cell2);
            ht.Controls.Add(htr);
            using (StringWriter swriter = new StringWriter())
            {
                HtmlTextWriter writer = new HtmlTextWriter(swriter);
                ht.RenderControl(writer);
                contents = swriter.ToString();
            }
            Console.WriteLine(contents);
            Console.ReadKey();
        }
    }
}
