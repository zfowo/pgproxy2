// 
// pgnet : 连接类
// 
using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using System.Linq;
using System.Collections;
using System.Collections.Generic;
using PGProtocol3;
using Zhb.Utils;
using Zhb.ExtensionMethods;

namespace PGNet
{
    public class PgNetException : Exception
    {
        public PgNetException(string message = "") : base(message)
        { }
    }
    public class PgErrorException : PgNetException
    {
        public ErrorResponse ErrorMsg;
        public string ClientEncoding;
        public PgErrorException(ErrorResponse errorMsg, string encoding)
        {
            ErrorMsg = errorMsg;
            ClientEncoding = encoding;
        }
        public PgErrorException(string message) : base(message)
        { }
        public override string Message
        {
            get
            {
                if (string.IsNullOrEmpty(base.Message))
                    return ErrorMsg[FieldType.Message].ToString(ClientEncoding);
                else
                    return base.Message;
            }
        }
    }
    public class PgAuthException : PgNetException
    {
        public PgAuthException(string message) : base(message)
        { }
    }

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
        public void Dispose() => Close();
        public void Close()
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
        public StartupMessage StartupMsg;
        public override bool IsFe => true;
        public PgFeConnection(Socket s, int bufSize = 1024 * 4, bool own = true) : base(s, bufSize, own)
        { }
        // 读取第一个消息，如果第1个是SSLRequest，那么会有第2个startup msg。
        public async Task<Msg> ReadStartMsgAsync(int milliSecondsTimeout = -1)
        {
            Msg msg = readBuf.GetStartupMsg();
            if (msg != null)
            {
                StartupMsg = msg as StartupMessage;
                return msg;
            }
            readBuf.Append(await sock.RcvAsync2(1024 * 4, milliSecondsTimeout));
            msg = readBuf.GetStartupMsg();
            StartupMsg = msg as StartupMessage;
            return msg;
        }
        public async Task WriteNoSslAsync()
        {
            await sock.SndAsync("N".Ascii());
        }
        // 与be进行auth过程。在调用之前应该已经接收了StartupMessage，也就是说已经处理过SSLRequest/CancelRequest。
        public async Task StartAuthAsync(PgBeConnection beConn)
        {
            if (StartupMsg == null)
                throw new PgNetException($"StartupMsg is null in PgFeConnection:{sock.RemoteEndPoint}");
            await beConn.WriteMsgAsync(StartupMsg);
            Poller poller = new Poller();
            poller.Register(this, Poller.POLLIN).Register(beConn, Poller.POLLIN);
            List<Msg> authOkMsgs = new List<Msg>();
            while (true)
            {
                await poller.PollAsync();
                Msg[] msgs = await this.ReadMsgsAsync(-1, 0);
                await beConn.WriteMsgsAsync(msgs);
                msgs = await beConn.ReadMsgsAsync(-1, 0);
                await this.WriteMsgsAsync(msgs);

                authOkMsgs.AddRange(msgs);
                if (authOkMsgs.Count > 0)
                {
                    Msg lastMsg = authOkMsgs[authOkMsgs.Count - 1];
                    if (lastMsg is ReadyForQuery)
                        break;
                    if (lastMsg is ErrorResponse errMsg)
                        throw new PgAuthException(errMsg[FieldType.Message].Bytes());
                }
            }
            int idx = authOkMsgs.FindIndex(m =>
            {
                return m is Authentication m2 && m2.AType == AuthType.Ok;
            });
            authOkMsgs = authOkMsgs.GetRange(idx, authOkMsgs.Count - idx);
            beConn.StartupMsg = this.StartupMsg;
            // 目前认为auth过程不会有异步消息NoticeResponse和NotificationResponse
            //Func<Msg, bool> pred = m => (m.MsgType == (byte)BeMsgType.NoticeResponse || m.MsgType == (byte)BeMsgType.NotificationResponse);
            beConn.AuthOkMsgs = authOkMsgs;//.Where(pred).ToList();
        }
    }

    public class PgBeConnection : PgConnectionBase
    {
        // auth成功后需要设置StartupMsg和AuthOkMsgs
        public StartupMessage StartupMsg;
        private List<Msg> authOkMsgs;
        public List<Msg> AuthOkMsgs
        {
            get => authOkMsgs;
            set
            {
                authOkMsgs = value;
                foreach (Msg msg in authOkMsgs)
                {
                    if (msg is ParameterStatus ps)
                        ParamsStatus[ps.Name.Ascii()] = ps.Value;
                    else if (msg is BackendKeyData bekd)
                        BeKeyData = bekd;
                }
            }
        }
        public Dictionary<string, byte[]> ParamsStatus = new Dictionary<string, byte[]>();
        public BackendKeyData BeKeyData;
        public string ClientEncoding => ParamsStatus["client_encoding"].Ascii();
        public override bool IsFe => false;
        public PgBeConnection(Socket s, int bufSize = 1024 * 4, bool own = true) : base(s, bufSize, own)
        { }

        // 读消息一直读到ReadyForQuery为止，如果discard=true则抛弃消息。
        public async Task<Msg[]> ReadMsgsUntilReadyAsync(bool discard = false)
        {
            List<Msg> msgs = new List<Msg>();
            while (true)
            {
                Msg msg = await ReadMsgAsync();
                if (!discard)
                    msgs.Add(msg);
                if (msg is ReadyForQuery)
                    break;
            }
            return msgs.ToArray();
        }
        public void PrintInfo()
        {
            Console.WriteLine(StartupMsg);
            Console.WriteLine(BeKeyData);
            foreach (var item in ParamsStatus)
                Console.WriteLine("{0} = {1}", item.Key, item.Value.Bytes());
        }
    }

    // 下面这些类可以作为客户端库使用
    public class NotificationEventArgs : EventArgs
    {
        public int Pid;
        public string Channel;
        public string Payload;
        public NotificationEventArgs(int pid, string channel, string payload)
        {
            Pid = pid;
            Channel = channel;
            Payload = payload;
        }
    }
    public class ParamStatusEventArgs : EventArgs
    {
        public string Name;
        public string Value;
        public ParamStatusEventArgs(string name, string value)
        {
            Name = name;
            Value = value;
        }
    }
    public class NoticeEventArgs : EventArgs
    {
        public List<(FieldType, string)> Fields;
        public NoticeEventArgs(List<(FieldType, string)> fields)
        {
            Fields = fields;
        }
    }
    public class PgConnection : PgBeConnection
    {
        // 3个异步消息的事件
        public event EventHandler<NotificationEventArgs> NotificationArrived;
        public event EventHandler<ParamStatusEventArgs> ParamStatusArrived;
        public event EventHandler<NoticeEventArgs> NoticeArrived;

        public PgConnection(Socket s, int bufSize = 1024 * 4, bool own = true) : base(s, bufSize, own)
        { }

        public async Task LoginAsync(string database = "postgres", string user = null, string password = null,
                                     string client_encoding = null, string application_name = null,
                                     string others = null)
        {
            user = user ?? Environment.UserName;
            password = password ?? "";
            client_encoding = client_encoding ?? "UTF8";
            application_name = application_name ?? "PgConnection";
            string connStr = string.Format("database={0} user={1} client_encoding={2} application_name={3} {4}",
                                           database, user, client_encoding, application_name, others);
            StartupMsg = connStr.StartupMessag(' ', '=', client_encoding);
            await WriteMsgAsync(StartupMsg);
            await ProcessAuthAsync(user.GetBytes(client_encoding), password.GetBytes(client_encoding));
        }
        private async Task ProcessAuthAsync(byte[] user, byte[] password)
        {
            Msg msg = await ReadMsgAsync();
            Authentication authMsg = msg as Authentication;
            if (authMsg == null)
                throw new PgAuthException($"unexpected message type. should be Authentication msg. {msg}");
            if (authMsg.AType == AuthType.Ok)
            {
                await ProcessAuthEndAsync(authMsg);
            }
            else if (authMsg.AType == AuthType.CleartextPassword)
            {
                await WriteMsgAsync(password.PasswordMessage(user, null));
                await ProcessAuthEndAsync(null);
            }
            else if (authMsg.AType == AuthType.MD5Password)
            {
                await WriteMsgAsync(password.PasswordMessage(user, authMsg.Md5Salt));
                await ProcessAuthEndAsync(null);
            }
            else if (authMsg.AType == AuthType.SASL)
            {
                await ProcessAuthSASLAsync(authMsg);
            }
            else
            {
                throw new PgAuthException($"unsupported auth type:{authMsg.AType}");
            }
        }
        // auth完成。auth时如果接收到ErrorResponse那么后面就不会再有ReadyForQuery消息；
        // 而auth成功之后，每次查询最后都会有ReadyForQuery，即使有ErrorResponse。
        private async Task ProcessAuthEndAsync(Authentication okMsg)
        {
            List<Msg> okMsgs = new List<Msg>();
            if (okMsg != null)
                okMsgs.Add(okMsg);
            while (true)
            {
                Msg[] msgs = await ReadMsgsAsync();
                okMsgs.AddRange(msgs);
                Msg lastMsg = okMsgs[okMsgs.Count - 1];
                if (lastMsg is ReadyForQuery)
                    break;
                if (lastMsg is ErrorResponse errMsg)
                    throw new PgAuthException(errMsg[FieldType.Message].Bytes());
            }
            // 目前认为auth过程不会有异步消息NoticeResponse和NotificationResponse
            //Func<Msg, bool> pred = m => (m.MsgType == (byte)BeMsgType.NoticeResponse || m.MsgType == (byte)BeMsgType.NotificationResponse);
            AuthOkMsgs = okMsgs;//.Where(pred).ToList();
        }
        private async Task ProcessAuthSASLAsync(Authentication saslMsg)
        {
            throw new PgAuthException("unsupported auth type: SASL");
        }
        // simple query
        public async Task<QueryResult> QueryAsync(string sql)
        {
            await WriteMsgAsync(sql.Query(ClientEncoding));
            SimpleQueryProcesser processer = new SimpleQueryProcesser(this);
            await processer.ProcessAsync(true);

            if (processer.GotCopyIn)
            {
                await WriteMsgAsync("abort because QueryAsync do not support copy in".CopyFail(ClientEncoding));
                await processer.ProcessAsync(true);
            }
            if (processer.GotCopyOut)
            {
                await processer.ProcessAsync(true);
            }
            return processer.Result;
        }
        // extended query
        // 如果discardRs=true，那么服务器端不会发送NoData和RowDescription消息，因此返回的QueryResult为null。
        // 对于大数据量的insert/update/delete可以提高性能。
        public async Task<QueryResult> Query2Async(string sql, IEnumerable<string[]> paramValuesList, bool discardRs = false)
        {
            await WriteMsgAsync(sql.Parse(encoding: ClientEncoding));
            ExtendedQueyrProcesser processer = new ExtendedQueyrProcesser(this, discardRs);
            foreach (var paramValues in paramValuesList)
            {
                if (discardRs)
                    await WriteMsgsAsync("".Bind(paramValues.ToList(), ClientEncoding), Execute.Default);
                else
                    await WriteMsgsAsync("".Bind(paramValues.ToList(), ClientEncoding), Describe.Portal(), Execute.Default);

                // 这里需要先检查GotCopyIn/GotCopyOut，然后再检查GotException
                await processer.ProcessAsync(false);
                if (processer.GotCopyIn)
                {
                    await WriteMsgAsync("abort because QueryAsync2 do not support copy in".CopyFail(ClientEncoding));
                    break;
                }
                if (processer.GotCopyOut)
                    break;
                if (processer.GotException)
                    break;
            }
            await WriteMsgsAsync(PGProtocol3.Close.Portal(), Sync.Default);
            await processer.ProcessAsync(true);
            return processer.Result;
        }
        public async Task<QueryResult> Query2Async(string sql, params string[] paramValues)
        {
            return await Query2Async(sql, new List<string[]> { paramValues });
        }

        public async Task CopyInAsync(string sql, IEnumerable<string> rows)
        {

        }

        internal void ProcessAsyncMsg(Msg msg)
        {
            switch ((BeMsgType)msg.MsgType)
            {
                case BeMsgType.NoticeResponse:
                    NoticeResponse msg1 = msg as NoticeResponse;
                    NoticeEventArgs eargs1 = new NoticeEventArgs(msg1.Fields.Select(fi => (fi.FType, fi.FValue.ToString(ClientEncoding))).ToList());
                    NoticeArrived?.Invoke(this, eargs1);
                    break;
                case BeMsgType.NotificationResponse:
                    NotificationResponse msg2 = msg as NotificationResponse;
                    NotificationEventArgs eargs2 = new NotificationEventArgs(msg2.Pid, msg2.Channel.ToString(ClientEncoding), msg2.Payload.ToString(ClientEncoding));
                    NotificationArrived?.Invoke(this, eargs2);
                    break;
                case BeMsgType.ParameterStatus:
                    // TODO: 需要更新ParamsStatus和AuthOkMsgs
                    ParameterStatus msg3 = msg as ParameterStatus;
                    ParamStatusEventArgs eargs3 = new ParamStatusEventArgs(msg3.Name.ToString(ClientEncoding), msg3.Value.ToString(ClientEncoding));
                    ParamStatusArrived?.Invoke(this, eargs3);
                    break;
                default:
                    Console.WriteLine("unknown async msg:{0}", msg);
                    break;
            }
        }
    }
    // gotReady变成true之后，先检查errEx再读取qres。
    // 一个SimpleQueryProcesser在接收到ReadyForQuery之后就不能再用了。
    class SimpleQueryProcesser
    {
        protected PgConnection pgConn;
        protected bool discardRs;
        protected QueryResult qres = null;
        protected PgErrorException errEx = null;
        protected RowDescription rowDesc = null;
        protected List<string[]> rows = null;
        protected string cmdTag = null;
        protected bool gotReady = false;
        protected bool gotCopyIn = false;
        protected bool gotCopyOut = false;

        public bool GotReady => gotReady;
        public bool GotException => errEx != null;
        public bool GotCopyIn => gotCopyIn;
        public bool GotCopyOut => gotCopyOut;
        public QueryResult Result
        {
            get
            {
                if (gotCopyIn || GotCopyOut)
                {
                    string error = "do not support copy in/out.";
                    if (errEx != null)
                        error += errEx.Message;
                    throw new PgErrorException(error);
                }
                if (errEx != null)
                    throw errEx;
                return qres;
            }
        }
        public SimpleQueryProcesser(PgConnection conn, bool discardRs = false)
        {
            this.pgConn = conn;
            this.discardRs = discardRs;
        }
        // 如果untilGotReady=true，那么处理消息直到接收到ReadyForQuery/CopyInResponse/CopyOutResponse；
        // 如果为false，那么直到没有消息可接收为止。
        public async Task ProcessAsync(bool untilGotReady = true)
        {
            if (gotReady)
                throw new PgErrorException("has got ReadyForQuery");
            while (true)
            {
                Msg msg = null;
                if (untilGotReady)
                {
                    msg = await pgConn.ReadMsgAsync();
                }
                else
                {
                    msg = await pgConn.ReadMsgAsync(0);
                    if (msg == null)
                        return;
                }
                Console.WriteLine("BE: {0}", msg);
                if (msg.IsAsyncMsg())
                {
                    pgConn.ProcessAsyncMsg(msg);
                    continue;
                }
                switch ((BeMsgType)msg.MsgType)
                {
                    case BeMsgType.RowDescription:
                        rowDesc = msg as RowDescription;
                        rows = new List<string[]>();
                        break;
                    case BeMsgType.DataRow:
                        if (!discardRs)
                            rows.Add((msg as DataRow).ColValues.Select(bs => bs?.ToString(pgConn.ClientEncoding)).ToArray());
                        break;
                    case BeMsgType.CommandComplete:
                        cmdTag = (msg as CommandComplete).Tag.ToString(pgConn.ClientEncoding);
                        if (!discardRs)
                            qres = new QueryResult(rowDesc, rows, cmdTag, pgConn.ClientEncoding);
                        rowDesc = null;
                        rows = null;
                        cmdTag = null;
                        break;
                    case BeMsgType.ReadyForQuery:
                        gotReady = true;
                        return;
                    
                    case BeMsgType.ErrorResponse:
                        errEx = new PgErrorException(msg as ErrorResponse, pgConn.ClientEncoding);
                        break;
                    case BeMsgType.EmptyQueryResponse:
                        errEx = new PgErrorException("Empty query");
                        break;
                    // copy msg
                    case BeMsgType.CopyInResponse:
                        gotCopyIn = true;
                        return;
                    case BeMsgType.CopyOutResponse:
                        gotCopyOut = true;
                        return;
                    case BeMsgType.CopyData:
                        break;
                    case BeMsgType.CopyDone:
                        break;
                    // extended query msg
                    case BeMsgType.ParseComplete:
                        break;
                    case BeMsgType.BindComplete:
                        break;
                    case BeMsgType.NoData:
                        break;
                    case BeMsgType.CloseComplete:
                        break;
                    default:
                        Console.WriteLine("unknown msg: {0}", msg);
                        errEx = new PgErrorException($"unknown msg: {msg}");
                        break;
                }
            }
        }
    }
    class ExtendedQueyrProcesser : SimpleQueryProcesser
    {
        public ExtendedQueyrProcesser(PgConnection conn, bool discardRs = false) : base(conn, discardRs)
        { }
    }
    public class QueryResult : IEnumerable<QueryResult.Row>
    {
        public class Row : IEnumerable<string>
        {
            private string[] rowData;
            private QueryResult qRes;

            public int Count => rowData.Length;
            public Row(QueryResult qres, string[] data)
            {
                this.qRes = qres;
                this.rowData = data;
            }

            public string this[int colIdx]
            {
                get
                {
                    return rowData[colIdx];
                }
            }
            public string this[string colName]
            {
                get
                {
                    int colIdx = qRes.ColNameMap[colName];
                    return this[colIdx];
                }
            }

            public override string ToString() => rowData.ToString2("()");

            public IEnumerator<string> GetEnumerator()
            {
                foreach (string s in rowData)
                    yield return s;
            }
            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }
        }

        public RowDescription RowDesc { get; }
        public Dictionary<string, int> ColNameMap = new Dictionary<string, int>();
        public List<string> ColNames = new List<string>();
        public (string, int, int) CmdTag;
        private List<string[]> rows;

        public int Count => rows.Count;
        public int EffectedCount => CmdTag.Item2;
        public string CmdName => CmdTag.Item1;
        // rowDesc为null表示没有查询结果的语句，此时只有CmdTag有意义。
        public QueryResult(RowDescription rowDesc, List<string[]> rows, string cmdTag, string encoding)
        {
            RowDesc = rowDesc;
            if (RowDesc != null)
            {
                for (int i = 0; i < rowDesc.Columns.Count; ++i)
                {
                    string colName = rowDesc.Columns[i].Name.ToString(encoding);
                    ColNameMap[colName] = i;
                    ColNames.Add(colName);
                }
            }
            this.rows = rows;
            ParseCmdTag(cmdTag);
        }

        private void ParseCmdTag(string cmdTag)
        {
            CmdTag.Item1 = cmdTag;
            CmdTag.Item2 = 0;
            CmdTag.Item3 = 0;
            string[] items = cmdTag.Split(' ');
            string x = items[0];
            if (x == "UPDATE" || x == "DELETE" || x == "SELECT" || x == "MOVE" || x == "FETCH" || x == "COPY")
            {
                CmdTag.Item1 = x;
                CmdTag.Item2 = int.Parse(items[1]);
            }
            else if (x == "INSERT")
            {
                CmdTag.Item1 = x;
                CmdTag.Item2 = int.Parse(items[2]); // rownum
                CmdTag.Item3 = int.Parse(items[1]); // oid
            }
        }

        public Row this[int idx] => new Row(this, rows[idx]);
        public IEnumerator<Row> GetEnumerator()
        {
            foreach (string[] data in rows)
                yield return new Row(this, data);
        }
        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public override string ToString()
        {
            return string.Format("<QueryResult CmdTag={0}>", CmdTag);
        }
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
                    break;
                }
                await fecnn.StartAuthAsync(becnn);
                Poller poller = new Poller();
                poller.Register(fecnn, Poller.POLLIN).Register(becnn, Poller.POLLIN);
                while (true)
                {
                    await poller.PollAsync();
                    await becnn.WriteMsgsAsync(await fecnn.ReadMsgsAsync(-1, 0));
                    await fecnn.WriteMsgsAsync(await becnn.ReadMsgsAsync(-1, 0));
                }
            }
        }
    }
}