
using System;
using System.Text;
using C = System.Console;
using System.Runtime.InteropServices;
using System.Collections;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
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
            SimpleProxy sp = new SimpleProxy("127.0.0.1:8888".Listen(), "127.0.0.1:5432");
            sp.Start();
        }
    }
}
