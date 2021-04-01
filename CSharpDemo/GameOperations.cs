namespace CSharpDemo
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    class GameOperations
    {
        public static void MainMethod()
        {
            ThreeCountryDemo();
        }

        private static void ThreeCountryDemo()
        {
            Node a = new Node(1, "Zhang", "Qun");
            Node b = new Node(2, "Cai", "Qun");
            Node c = new Node(3, "Zhen", "Wei");
            Node d = new Node(4, "Sun", "Wu");
            Node e = new Node(5, "Zhuge", "Shu");
            Node f = new Node(6, "Sima", "Wei");
            Node g = new Node(7, "Huang", "Shu");
            Node h = new Node(8, "Lu", "Wu");
            a.Helpers.Add(b);
            a.Helpers.Add(g);
            b.Helpers.Add(c);
            b.Helpers.Add(h);
            c.Helpers.Add(a);
            c.Helpers.Add(g);
            d.Helpers.Add(a);
            d.Helpers.Add(c);
            e.Helpers.Add(b);
            e.Helpers.Add(d);
            f.Helpers.Add(d);
            f.Helpers.Add(e);
            g.Helpers.Add(f);
            g.Helpers.Add(h);
            h.Helpers.Add(e);
            h.Helpers.Add(f);

            var result = GetAllLoops(new HashSet<Node>(), e, null, null);
            foreach (var loop in result)
            {
                if (loop.Contains(c))
                    Console.WriteLine(string.Join("->", loop.Select(n => n.Name)));
            }
        }

        private static IList<IList<Node>> GetAllLoops(Node node)
        {
            IList<IList<Node>> result = new List<IList<Node>>();

            return result;
        }

        private static IList<IList<Node>> GetAllLoops(HashSet<Node> set, Node start, Node cur, IList<IList<Node>> loops)
        {
            if (loops == null)
            {
                loops = new List<IList<Node>>();
            }

            if (set.Contains(cur))
            {
                return loops;
            }

            if (cur == null)
            {
                cur = start;
                loops.Add(new List<Node>());
            }

            List<IList<Node>> result = new List<IList<Node>>();
            foreach (var help in cur.Helpers)
            {
                HashSet<Node> nextSet = new HashSet<Node>(set);
                nextSet.Add(cur);
                IList<IList<Node>> nextLoops = new List<IList<Node>>();
                foreach (var loop in loops)
                {
                    var newLoop = new List<Node>(loop);
                    newLoop.Add(cur);
                    nextLoops.Add(newLoop);
                }

                result.AddRange(GetAllLoops(nextSet, start, help, nextLoops));
            }

            return result;
        }

        class Node
        {
            public Node(int id, string name, string country)
            {
                this.Id = id;
                this.Name = name;
                this.Country = country;
                this.Helpers = new List<Node>();
            }
            public int Id { get; set; }
            public string Name { get; set; }
            public string Country { get; set; }
            public IList<Node> Helpers { get; set; }
        }
    }
}
