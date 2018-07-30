
using System;
using System.Text;
using C = System.Console;
using System.Runtime.InteropServices;
using System.Collections;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

using Zhb.Utils;
using Zhb.ExtensionMethods;
using PGProtocol3;
using PGNet;

namespace pgproxy
{
    class Program
    {
        static void Main(string[] args)
        {
            //RunProxy();
            TestQuery().Wait();
        }
        static async Task TestParse()
        {
            PgConnection cnn = new PgConnection(await "127.0.0.1:5432".ConnectAsync());
            await cnn.LoginAsync();
            //cnn.PrintInfo();
            QueryResult res = await cnn.Query2Async("copy (select * from t1) to stdout");//"insert into t2 values($1,$2)", new[] { "aaa", "100" }, new[] { "bbb", "200" });
            foreach (var r in res)
                Console.WriteLine(r);
            cnn.Close();
        }
        static async Task TestQuery()
        {
            PgConnection cnn = new PgConnection(await "127.0.0.1:5432".ConnectAsync());
            await cnn.LoginAsync();
            while (true)
            {
                Console.Write("#> ");
                string sql = Console.ReadLine();
                sql = sql.Trim();
                if (sql == "exit")
                    break;
                try
                {
                    var res = await cnn.QueryAsync(sql);
                    Console.WriteLine(res.CmdTag);
                    if (res.RowDesc != null)
                    {
                        Console.WriteLine(res.ColNames.ToString2());
                        foreach (var row in res)
                            Console.WriteLine(row);
                    }
                }
                catch (PgErrorException ex)
                {
                    Console.WriteLine("PgErrorException: {0}", ex.Message);
                }
            }
            cnn.Close();
        }
        static void RunProxy()
        {
            SimpleProxy sp = new SimpleProxy("127.0.0.1:8888".Listen(), "127.0.0.1:5432");
            sp.Start();
        }
    }
}
