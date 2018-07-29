// 
// pgnet : 连接类
// 
using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using System.Linq;
using PGProtocol3;
using Zhb.Utils;
using Zhb.ExtensionMethods;

namespace PGNet
{
    public abstract class PgConnectionBase : ISocketable, IDisposable
    {
        protected Socket sock;
        protected MyBuffer readBuf;
        private bool ownSock;
        public bool LogMsg { get; set; } = true;
        public abstract bool IsFe { get; }

        public PgConnectionBase(Socket s, int bufSize = 1024 * 4, bool own = true)
        {
            sock = s;
            readBuf = new MyBuffer(bufSize);
            ownSock = own;
        }
        public Socket GetSocket() => this.sock;
        public void Dispose()
        {
            sock.Shutdown(SocketShutdown.Both);
            sock.Close();
        }
        // ReadXXX不能用于读取startup message；WriteXXX可以用于写startup message。
        public async Task<Msg> ReadMsgAsync(int milliSecondsTimeout = -1)
        {
            Msg msg = readBuf.GetMsg(IsFe);
            if (msg != null)
                return msg;
            readBuf.Append(await sock.RcvAsync2(1024 * 4, milliSecondsTimeout));
            return readBuf.GetMsg(IsFe);
        }
        public async Task<Msg[]> ReadMsgsAsync(int max = -1, int milliSecondsTimeout = -1)
        {
            //readBuf.Append(await sock.RcvAsync2(1024 * 4, 0));
            Msg[] msgs = readBuf.GetMsgs(max, IsFe);
            if (msgs.Length > 0)
                return msgs;
            readBuf.Append(await sock.RcvAsync2(1024 * 4, milliSecondsTimeout));
            return readBuf.GetMsgs(max, IsFe);
        }
        public async Task WriteMsgAsync(Msg msg)
        {
            PrintWriteMsgs(msg);
            await sock.SndAsync(msg.ToBytes());
        }
        public async Task WriteMsgsAsync(params Msg[] msgs)
        {
            if (msgs.Length <= 0)
                return;
            PrintWriteMsgs(msgs);
            await sock.SndAsync(msgs.Select(m => m.ToBytes()).ToArray());
        }
        private void PrintWriteMsgs(params Msg[] msgs)
        {
            if (!LogMsg)
                return;
            string prefixStr = IsFe ? "BE" : "FE";
            foreach (Msg msg in msgs)
                Console.WriteLine("{0}: {1}", prefixStr, msg);
        }
    }

    public class PgFeConnection : PgConnectionBase
    {
        public override bool IsFe => true;
        public PgFeConnection(Socket s, int bufSize = 1024 * 4, bool own = true) : base(s, bufSize, own)
        { }
        // 读取第一个消息，如果第1个是SSLRequest，那么会有第2个startup msg。
        public async Task<Msg> ReadStartMsgAsync(int milliSecondsTimeout = -1)
        {
            Msg msg = readBuf.GetStartupMsg();
            if (msg != null)
                return msg;
            readBuf.Append(await sock.RcvAsync2(1024 * 4, milliSecondsTimeout));
            return readBuf.GetStartupMsg();
        }
        public async Task WriteNoSslAsync()
        {
            await sock.SndAsync("N".Ascii());
        }
    }

    public class PgBeConnection : PgConnectionBase
    {
        public override bool IsFe => false;
        public PgBeConnection(Socket s, int bufSize = 1024 * 4, bool own = true) : base(s, bufSize, own)
        { }
    }

    public class SimpleProxy
    {
        private Socket listenSock;
        private string beAddr;
        public SimpleProxy(Socket s, string beAddr)
        {
            this.listenSock = s;
            this.beAddr = beAddr;
        }
        public void Start()
        {
            Console.WriteLine("Accepting on {0}", listenSock.LocalEndPoint);
            while (true)
            {
                Socket s = listenSock.AcptAsync().Result;
                Console.WriteLine("Accepted from {0}", s.RemoteEndPoint);
                ProxyMain(s).ContinueWith(t =>
                {
                    if (t.IsFaulted)
                        Console.WriteLine("got exception: {0}", t.Exception.InnerException);
                });
            }
        }
        private async Task ProxyMain(Socket fes)
        {
            Socket bes = await beAddr.ConnectAsync();
            using (PgFeConnection fecnn = new PgFeConnection(fes))
            using (PgBeConnection becnn = new PgBeConnection(bes))
            {
                // 先处理startup msg
                while (true)
                {
                    Msg msg = await fecnn.ReadStartMsgAsync();
                    if (msg is SSLRequest)
                    {
                        await fecnn.WriteNoSslAsync();
                        continue;
                    }
                    await becnn.WriteMsgAsync(msg);
                    break;
                }
                Poller poll = new Poller();
                poll.Register(fecnn, Poller.POLLIN).Register(becnn, Poller.POLLIN);
                while (true)
                {
                    await poll.PollAsync();
                    await becnn.WriteMsgsAsync(await fecnn.ReadMsgsAsync(-1, 0));
                    await fecnn.WriteMsgsAsync(await becnn.ReadMsgsAsync(-1, 0));
                }
            }
        }
    }
}