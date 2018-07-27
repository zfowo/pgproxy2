// 
// 解析postgresql c/s version 3协议，不包括复制相关的协议。
// postgresql消息格式: type + len + data。type是一个字节，len是4个字节表示大小，包括len本身的4个字节。
// FE的第一个消息不包含type部分。
// 
// 关于从多个数据计算hash值: 选2个素数，其中一个作为hash值的初始值，然后hash值和另一个素数相乘后再和数据的hash值相加。比如: 
//     int hv = 17;
//     foreach (var obj in ....)
//         hv = hv * 53 + var.GetHashCode();
// 最好不要用xor，因为两个相同数值的xor结果为0。
// 
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Zhb.Utils;
using Zhb.ExtensionMethods;

namespace PGProtocol3
{
    public enum FeMsgType
    {
        Query = 'Q',                // Query
        Parse = 'P',                // Parse (大写P)
        Bind = 'B',                 // Bind
        Execute = 'E',              // Execute
        Describe = 'D',             // Describe
        Close = 'C',                // Close
        Sync = 'S',                 // Sync
        Flush = 'H',                // Flush
        CopyData = 'd',             // CopyData (和be共用)
        CopyDone = 'c',             // CopyDone (和be共用)
        CopyFail = 'f',             // CopyFail
        FunctionCall = 'F',         // FunctionCall
        Terminate = 'X',            // Terminate
        // 'p'类型的消息是对Authentication的响应。类似于Authentication，包括多个具体类型，不过只能从上下文中判断。
        AuthResponse = 'p',         // (小写p)具体类型包括: PasswordMessage,SASLInitialResponse,SASLResponse,GSSResponse。
    }
    // NoticeResponse,NotificationResponse,ParameterStatus是异步消息。
    public enum BeMsgType
    {
        Authentication = 'R',       // AuthenticationXXX
        BackendKeyData = 'K',       // BackendKeyData
        BindComplete = '2',         // BindComplete
        CloseComplete = '3',        // CloseComplete
        CommandComplete = 'C',      // CommandComplete
        CopyData = 'd',             // CopyData
        CopyDone = 'c',             // CopyDone
        CopyInResponse = 'G',       // CopyInResponse
        CopyOutResponse = 'H',      // CopyOutResponse
        CopyBothResponse = 'W',     // CopyBothResponse (only for Streaming Replication)
        DataRow = 'D',              // DataRow
        EmptyQueryResponse = 'I',   // EmptyQueryResponse
        ErrorResponse = 'E',        // ErrorResponse
        NoticeResponse = 'N',       // NoticeResponse (async message)
        FunctionCallResponse = 'V', // FunctionCallResponse (大写V)
        NoData = 'n',               // NoData
        NotificationResponse = 'A', // NotificationResponse (async message)
        ParameterDescription = 't', // ParameterDescription
        ParameterStatus = 'S',      // ParameterStatus (async message while reloading configure file)
        ParseComplete = '1',        // ParseComplete
        PortalSuspended = 's',      // PortalSuspended
        ReadyForQuery = 'Z',        // ReadyForQuery
        RowDescription = 'T',       // RowDescription
    }
    public enum ObjType : byte
    {
        PreparedStmt = (byte)'S',
        Portal = (byte)'P',
    }
    public enum TransStatus : byte
    {
        Idle = (byte)'I',
        InBlock = (byte)'T',
        Fail = (byte)'E',
    }
    public enum FieldType : byte
    {
        Severity = (byte)'S',
        Severity2 = (byte)'V',
        Code = (byte)'C',
        Message = (byte)'M',
        Detail = (byte)'D',
        Hint = (byte)'H',
        Position = (byte)'P',
        InternalPos = (byte)'p',
        InternalQuery = (byte)'q',
        Where = (byte)'W',
        SchemaName = (byte)'s',
        TableName = (byte)'t',
        ColumnName = (byte)'c',
        DataType = (byte)'d',
        ConstraintName = (byte)'n',
        File = (byte)'F',
        Line = (byte)'L',
        Routine = (byte)'R',
    }
    public enum AuthType
    {
        Ok = 0,
        KerberosV5 = 2,
        CleartextPassword = 3,
        MD5Password = 5,
        SCMCredential = 6,
        GSS = 7,
        GSSContinue = 8,
        SSPI = 9,
        SASL = 10,
        SASLContinue = 11,
        SASLFinal = 12,
    }
    class PgProtoException : Exception
    {
        public PgProtoException(string message) : base(message)
        { }
    }
    // 从MyBuffer获得下一个Msg。调用之前确保有完整消息
    public delegate Msg FromBufferFunc(MyBuffer buf, int idx);
    public abstract class Msg
    {
        public abstract byte MsgType { get; }
        public abstract byte[] ToBytes();
        protected MyBuffer FixHeader(MyBuffer buf)
        {
            buf[1, 4] = (buf.Count - 1).GetBytes();
            return buf;
        }

        public static Dictionary<byte, FromBufferFunc> FeMsgMap;
        public static Dictionary<byte, FromBufferFunc> BeMsgMap;
        static Msg()
        {
            FeMsgMap = new Dictionary<byte, FromBufferFunc>()
            {
                [(byte)FeMsgType.Query] = Query.FromBuffer,
                [(byte)FeMsgType.Parse] = Parse.FromBuffer,
                [(byte)FeMsgType.Bind] = Bind.FromBuffer,
                [(byte)FeMsgType.Execute] = Execute.FromBuffer,
                [(byte)FeMsgType.Describe] = Describe.FromBuffer,
                [(byte)FeMsgType.Close] = Close.FromBuffer,
                [(byte)FeMsgType.Sync] = Sync.FromBuffer,
                [(byte)FeMsgType.Flush] = Flush.FromBuffer,
                [(byte)FeMsgType.CopyData] = CopyData.FromBuffer,
                [(byte)FeMsgType.CopyDone] = CopyDone.FromBuffer,
                [(byte)FeMsgType.CopyFail] = CopyFail.FromBuffer,
                [(byte)FeMsgType.FunctionCall] = FunctionCall.FromBuffer,
                [(byte)FeMsgType.Terminate] = Terminate.FromBuffer,
                [(byte)FeMsgType.AuthResponse] = AuthResponse.FromBuffer,
            };
            BeMsgMap = new Dictionary<byte, FromBufferFunc>
            {
                [(byte)BeMsgType.Authentication] = Authentication.FromBuffer,
                [(byte)BeMsgType.BackendKeyData] = BackendKeyData.FromBuffer,
                [(byte)BeMsgType.BindComplete] = BindComplete.FromBuffer,
                [(byte)BeMsgType.CloseComplete] = CloseComplete.FromBuffer,
                [(byte)BeMsgType.CommandComplete] = CommandComplete.FromBuffer,
                [(byte)BeMsgType.CopyData] = CopyData.FromBuffer,
                [(byte)BeMsgType.CopyDone] = CopyDone.FromBuffer,
                [(byte)BeMsgType.CopyInResponse] = CopyInResponse.FromBuffer,
                [(byte)BeMsgType.CopyOutResponse] = CopyOutResponse.FromBuffer,
                [(byte)BeMsgType.CopyBothResponse] = CopyBothResponse.FromBuffer,
                [(byte)BeMsgType.DataRow] = DataRow.FromBuffer,
                [(byte)BeMsgType.EmptyQueryResponse] = EmptyQueryResponse.FromBuffer,
                [(byte)BeMsgType.ErrorResponse] = ErrorResponse.FromBuffer,
                [(byte)BeMsgType.NoticeResponse] = NoticeResponse.FromBuffer,
                [(byte)BeMsgType.FunctionCallResponse] = FunctionCallResponse.FromBuffer,
                [(byte)BeMsgType.NoData] = NoData.FromBuffer,
                [(byte)BeMsgType.NotificationResponse] = NotificationResponse.FromBuffer,
                [(byte)BeMsgType.ParameterDescription] = ParameterDescription.FromBuffer,
                [(byte)BeMsgType.ParameterStatus] = ParameterStatus.FromBuffer,
                [(byte)BeMsgType.ParseComplete] = ParseComplete.FromBuffer,
                [(byte)BeMsgType.PortalSuspended] = PortalSuspended.FromBuffer,
                [(byte)BeMsgType.ReadyForQuery] = ReadyForQuery.FromBuffer,
                [(byte)BeMsgType.RowDescription] = RowDescription.FromBuffer,
            };
        }

        public const int PG_PROTO_VERSION2_CODE = 131072;
        public const int PG_PROTO_VERSION3_CODE = 196608;
        public const int PG_CANCELREQUEST_CODE = 80877102;
        public const int PG_SSLREQUEST_CODE = 80877103;
    }
    // 只有消息类型和长度的消息
    public abstract class EmptyMsg<T> : Msg where T : EmptyMsg<T>, new()
    {
        public static readonly T Default = new T();

        public static Msg FromBuffer(MyBuffer buf, int idx)
        {
            return new T();
        }
        public override byte[] ToBytes()
        {
            MyBuffer buf = new MyBuffer();
            buf.Append(MsgType).Append(4);
            return buf.ToBytes();
        }
        public override string ToString() => string.Format("<{0}>", typeof(T).Name);
    }
    // fe msg type
    public class Query : Msg
    {
        public byte[] Sql;

        public static Msg FromBuffer(MyBuffer buf, int idx)
        {
            idx += 5;
            byte[] sql = buf.GetCStr(idx); idx += sql.Length + 1;
            return new Query { Sql = sql };
        }
        public override byte MsgType => (byte)FeMsgType.Query;
        public override byte[] ToBytes()
        {
            MyBuffer buf = new MyBuffer();
            buf.Append(MsgType).Append(0).AppendCStr(Sql);
            return FixHeader(buf).ToBytes();
        }
        public override string ToString() => string.Format("<Query Sql={0}>", Sql.Bytes());
    }
    public class Parse : Msg
    {
        public byte[] Stmt = MiscUtil.EmptyBytes;
        public byte[] Sql;
        public short ParamCnt { get => (short)ParamOids.Length; }
        public int[] ParamOids = MiscUtil.EmptyInts;

        public static Msg FromBuffer(MyBuffer buf, int idx)
        {
            idx += 5;
            byte[] stmt = buf.GetCStr(idx); idx += stmt.Length + 1;
            byte[] sql = buf.GetCStr(idx); idx += sql.Length + 1;
            short paramcnt = buf.GetShort(idx); idx += 2;
            int[] paramoids = buf.GetN<int>(paramcnt, idx); idx += 4 * paramoids.Length;
            return new Parse { Stmt = stmt, Sql = sql, ParamOids = paramoids };
        }
        public override byte MsgType => (byte)FeMsgType.Parse;
        public override byte[] ToBytes()
        {
            MyBuffer buf = new MyBuffer();
            buf.Append(MsgType).Append(0).AppendCStr(Stmt).AppendCStr(Sql).Append(ParamCnt).Append(ParamOids);
            return FixHeader(buf).ToBytes();
        }
        public override string ToString() => string.Format("<Parse Stmt={0} Sql={1} ParamOids={2}>", Stmt.Bytes(), Sql.Bytes(), ParamOids.ToString2());
    }
    public class Bind : Msg
    {
        public byte[] Portal = MiscUtil.EmptyBytes;
        public byte[] Stmt = MiscUtil.EmptyBytes;
        public short FcCnt { get => (short)Fcs.Length; }
        public short[] Fcs = MiscUtil.EmptyShorts;
        public List<byte[]> Params = new List<byte[]>();
        public short ResFcCnt { get => (short)ResFcs.Length; }
        public short[] ResFcs = MiscUtil.EmptyShorts;

        public static Msg FromBuffer(MyBuffer buf, int idx)
        {
            idx += 5;
            byte[] portal = buf.GetCStr(idx); idx += portal.Length + 1;
            byte[] stmt = buf.GetCStr(idx); idx += stmt.Length + 1;
            short fccnt = buf.GetShort(idx); idx += 2;
            short[] fcs = buf.GetN<short>(fccnt, idx); idx += 2 * fcs.Length;
            List<byte[]> param = buf.Get24X(idx, out int sz); idx += sz;
            short resfccnt = buf.GetShort(idx); idx += 2;
            short[] resfcs = buf.GetN<short>(resfccnt, idx); idx += 2 * resfcs.Length;
            return new Bind { Portal = portal, Stmt = stmt, Fcs = fcs, Params = param, ResFcs = resfcs };
        }
        public override byte MsgType => (byte)FeMsgType.Bind;
        public override byte[] ToBytes()
        {
            MyBuffer buf = new MyBuffer();
            buf.Append(MsgType).Append(0).AppendCStr(Portal).AppendCStr(Stmt).Append(FcCnt).Append(Fcs).Append24X(Params).Append(ResFcCnt).Append(ResFcs);
            return FixHeader(buf).ToBytes();
        }
        public override string ToString()
        {
            return string.Format("<Bind Portal={0} Stmt={1} Fcs={2} ResFcs={3}>", Portal.Bytes(), Stmt.Bytes(), Fcs.ToString2(), ResFcs.ToString2());
        }
    }
    public class Execute : Msg
    {
        public byte[] Portal = MiscUtil.EmptyBytes;
        public int MaxNum = 0;
        public static readonly Execute Default = new Execute();

        public static Msg FromBuffer(MyBuffer buf, int idx)
        {
            idx += 5;
            byte[] portal = buf.GetCStr(idx); idx += portal.Length + 1;
            int maxnum = buf.GetInt(idx); idx += 4;
            return new Execute { Portal = portal, MaxNum = maxnum };
        }
        public override byte MsgType => (byte)FeMsgType.Execute;
        public override byte[] ToBytes()
        {
            MyBuffer buf = new MyBuffer();
            buf.Append(MsgType).Append(0).AppendCStr(Portal).Append(MaxNum);
            return FixHeader(buf).ToBytes();
        }
        public override string ToString() => string.Format("<Execute Portal={0} MaxNum={1}>", Portal.Bytes(), MaxNum);
    }
    public abstract class DescribeClose<T> : Msg where T : DescribeClose<T>, new()
    {
        public ObjType OType;
        public byte[] OName = MiscUtil.EmptyBytes;

        public static Msg FromBuffer(MyBuffer buf, int idx)
        {
            idx += 5;
            byte objtype = buf[idx]; idx += 1;
            byte[] objname = buf.GetCStr(idx); idx += objname.Length + 1;
            return new T { OType = (ObjType)objtype, OName = objname };
        }
        public override byte[] ToBytes()
        {
            MyBuffer buf = new MyBuffer();
            buf.Append(MsgType).Append(0).Append((byte)OType).AppendCStr(OName);
            return FixHeader(buf).ToBytes();
        }
        public override string ToString() => string.Format("<{0} OType={1} OName={2}>", typeof(T).Name, OType, OName.Bytes());

        public static T Stmt(byte[] objname = null)
        {
            objname = objname ?? MiscUtil.EmptyBytes;
            return new T { OType = ObjType.PreparedStmt, OName = objname };
        }
        public static T Portal(byte[] objname = null)
        {
            objname = objname ?? MiscUtil.EmptyBytes;
            return new T { OType = ObjType.Portal, OName = objname };
        }
    }
    public class Describe : DescribeClose<Describe>
    {
        public override byte MsgType => (byte)FeMsgType.Describe;
    }
    public class Close : DescribeClose<Close>
    {
        public override byte MsgType => (byte)FeMsgType.Close;
    }
    public class Sync : EmptyMsg<Sync>
    {
        public override byte MsgType => (byte)FeMsgType.Sync;
    }
    public class Flush : EmptyMsg<Flush>
    {
        public override byte MsgType => (byte)FeMsgType.Flush;
    }
    public class CopyFail : Msg
    {
        public byte[] ErrInfo;

        public static Msg FromBuffer(MyBuffer buf, int idx)
        {
            idx += 5;
            byte[] errinfo = buf.GetCStr(idx); idx += errinfo.Length + 1;
            return new CopyFail { ErrInfo = errinfo };
        }
        public override byte MsgType => (byte)FeMsgType.CopyFail;
        public override byte[] ToBytes()
        {
            MyBuffer buf = new MyBuffer();
            buf.Append(MsgType).Append(0).AppendCStr(ErrInfo);
            return FixHeader(buf).ToBytes();
        }
        public override string ToString() => string.Format("<CopyFail ErrInfo={0}>", ErrInfo.Bytes());
    }
    public class FunctionCall : Msg
    {
        public int FuncOid;
        public short FcCnt => (short)Fcs.Length;
        public short[] Fcs = MiscUtil.EmptyShorts;
        public List<byte[]> Args = new List<byte[]>();
        public short ResFc = 0;

        public static Msg FromBuffer(MyBuffer buf, int idx)
        {
            idx += 5;
            int foid = buf.GetInt(idx); idx += 4;
            short fccnt = buf.GetShort(idx); idx += 2;
            short[] fcs = buf.GetN<short>(fccnt, idx); idx += 2 * fcs.Length;
            List<byte[]> args = buf.Get24X(idx, out int sz); idx += sz;
            short resfc = buf.GetShort(idx); idx += 2;
            return new FunctionCall { FuncOid = foid, Fcs = fcs, Args = args, ResFc = resfc };
        }
        public override byte MsgType => (byte)FeMsgType.FunctionCall;
        public override byte[] ToBytes()
        {
            MyBuffer buf = new MyBuffer();
            buf.Append(MsgType).Append(0).Append(FuncOid).Append(FcCnt).Append(Fcs).Append24X(Args).Append(ResFc);
            return FixHeader(buf).ToBytes();
        }
        public override string ToString()
        {
            return string.Format("<FunctionCall FuncOid={0} Fcs={1}, Args={2}, ResFc={3}", 
                                 FuncOid, Fcs.ToString2(), Args.Select(bs => bs.Bytes()).ToString2(), ResFc);
        }
    }
    public class Terminate : EmptyMsg<Terminate>
    {
        public override byte MsgType => (byte)FeMsgType.Terminate;
    }
    // AuthResponse只能由FromBuf创建，当需要手动创建该类型消息时，可以创建具体的AuthResponse消息，比如PasswordMessage等。
    public class AuthResponse : Msg
    {
        public byte[] Data { get; }
        private AuthResponse(byte[] data)
        {
            Data = data;
        }

        public static Msg FromBuffer(MyBuffer buf, int idx)
        {
            idx += 1;
            int sz = buf.GetInt(idx); idx += 4;
            byte[] data = buf[idx, sz - 4];
            return new AuthResponse(data);
        }
        public override byte MsgType => (byte)FeMsgType.AuthResponse;
        public override byte[] ToBytes()
        {
            MyBuffer buf = new MyBuffer();
            buf.Append(MsgType).Append(0).Append(Data);
            return FixHeader(buf).ToBytes();
        }
        public override string ToString() => string.Format("<AuthResponse Data={0}>", Data.Bytes());

        public static explicit operator PasswordMessage(AuthResponse ar)
        {
            MyBuffer buf = new MyBuffer(ar.Data, false);
            byte[] pwd = buf.GetCStr(0);
            return new PasswordMessage { Password = pwd };
        }
        public static explicit operator SASLInitialResponse(AuthResponse ar)
        {
            MyBuffer buf = new MyBuffer(ar.Data, false);
            int idx = 0;
            byte[] name = buf.GetCStr(idx); idx += name.Length + 1;
            byte[] data = buf.Get4x(idx, out int sz); idx += sz;
            return new SASLInitialResponse { MechName = name, Data = data };
        }
        public static explicit operator SASLResponse(AuthResponse ar)
        {
            return new SASLResponse { Data = ar.Data };
        }
        public static explicit operator GSSResponse(AuthResponse ar)
        {
            return new GSSResponse { Data = ar.Data };
        }
    }
    public class PasswordMessage : Msg
    {
        public byte[] Password;

        public override byte MsgType => (byte)FeMsgType.AuthResponse;
        public override byte[] ToBytes()
        {
            MyBuffer buf = new MyBuffer();
            buf.Append(MsgType).Append(0).AppendCStr(Password);
            return FixHeader(buf).ToBytes();
        }
        public override string ToString() => string.Format("<PasswordMessage Password={0}>", Password.Bytes());
    }
    public class SASLInitialResponse : Msg
    {
        public byte[] MechName;
        public byte[] Data;

        public override byte MsgType => (byte)FeMsgType.AuthResponse;
        public override byte[] ToBytes()
        {
            MyBuffer buf = new MyBuffer();
            buf.Append(MsgType).Append(0).AppendCStr(MechName).Append4x(Data);
            return FixHeader(buf).ToBytes();
        }
        public override string ToString() => string.Format("<SASLInitialResponse Name={0} Data={1}>", MechName.Bytes(), Data.Bytes());
    }
    public class SASLResponse : Msg
    {
        public byte[] Data;

        public override byte MsgType => (byte)FeMsgType.AuthResponse;
        public override byte[] ToBytes()
        {
            MyBuffer buf = new MyBuffer();
            buf.Append(MsgType).Append(0).Append(Data);
            return FixHeader(buf).ToBytes();
        }
        public override string ToString() => string.Format("<SASLResponse Data={0}>", Data.Bytes());
    }
    public class GSSResponse : SASLResponse
    {
        public override string ToString() => string.Format("<GSSResponse Data={0}>", Data.Bytes());
    }

    // be msg type
    public class Authentication : Msg
    {
        public AuthType AType { get; }
        public byte[] Data { get; }
        private Authentication(AuthType authType, byte[] data = null)
        {
            AType = authType;
            Data = data ?? MiscUtil.EmptyBytes;
        }

        public static Msg FromBuffer(MyBuffer buf, int idx)
        {
            idx += 1;
            int sz = buf.GetInt(idx); idx += 4;
            int authType = buf.GetInt(idx); idx += 4;
            byte[] data = buf[idx, sz - 8];
            return new Authentication((AuthType)authType, data);
        }
        public override byte MsgType => (byte)BeMsgType.Authentication;
        public override byte[] ToBytes()
        {
            MyBuffer buf = new MyBuffer();
            buf.Append(MsgType).Append(0).Append((int)AType).Append(Data);
            return FixHeader(buf).ToBytes();
        }
        public override string ToString() => string.Format("<Authentication AType={0} Data={1}>", AType, Data.Bytes());

        public byte[] Md5Salt
        {
            get
            {
                if (AType != AuthType.Ok)
                    throw new PgProtoException("this is not Ok auth type");
                return Data;
            }
        }
        public List<byte[]> MechNames
        {
            get
            {
                if (AType != AuthType.SASL)
                    throw new PgProtoException("this is not SASL auth type");
                return (new MyBuffer(Data, false)).GetX(0, out int sz);
            }
        }

        public static Authentication Ok() => new Authentication(AuthType.Ok);
        public static Authentication KerberosV5() => new Authentication(AuthType.KerberosV5);
        public static Authentication CleartextPassword() => new Authentication(AuthType.CleartextPassword);
        public static Authentication MD5Password(byte[] salt)
        {
            if (salt.Length != 4)
                throw new PgProtoException("salt for MD5Password should be 4 length");
            return new Authentication(AuthType.MD5Password, salt);
        }
        public static Authentication SCMCredential() => new Authentication(AuthType.SCMCredential);
        public static Authentication GSS() => new Authentication(AuthType.GSS);
        public static Authentication SSPI() => new Authentication(AuthType.SSPI);
        public static Authentication GSSContinue(byte[] data) => new Authentication(AuthType.GSSContinue, data);
        public static Authentication SASL(params byte[][] mechNames)
        {
            MyBuffer buf = new MyBuffer();
            buf.AppendX(mechNames);
            return new Authentication(AuthType.SASL, buf.ToBytes());
        }
        public static Authentication SASLContinue(byte[] data) => new Authentication(AuthType.SASLContinue, data);
        public static Authentication SASLFinal(byte[] data) => new Authentication(AuthType.SASLFinal, data);
    }
    public class BackendKeyData : Msg
    {
        public int Pid;
        public int SecretKey;

        public static Msg FromBuffer(MyBuffer buf, int idx)
        {
            idx += 5;
            int pid = buf.GetInt(idx); idx += 4;
            int key = buf.GetInt(idx); idx += 4;
            return new BackendKeyData { Pid = pid, SecretKey = key };
        }
        public override byte MsgType => (byte)BeMsgType.BackendKeyData;
        public override byte[] ToBytes()
        {
            MyBuffer buf = new MyBuffer();
            buf.Append(MsgType).Append(0).Append(Pid).Append(SecretKey);
            return FixHeader(buf).ToBytes();
        }
        public override string ToString() => string.Format("<BackendKeyData Pid={0} SecretKey={1}>", Pid, SecretKey);
    }
    public class BindComplete : EmptyMsg<BindComplete>
    {
        public override byte MsgType => (byte)BeMsgType.BindComplete;
    }
    public class CloseComplete : EmptyMsg<CloseComplete>
    {
        public override byte MsgType => (byte)BeMsgType.CloseComplete;
    }
    public class CommandComplete : Msg
    {
        public byte[] Tag;

        public static Msg FromBuffer(MyBuffer buf, int idx)
        {
            idx += 5;
            byte[] tag = buf.GetCStr(idx); idx += tag.Length + 1;
            return new CommandComplete { Tag = tag };
        }
        public override byte MsgType => (byte)BeMsgType.CommandComplete;
        public override byte[] ToBytes()
        {
            MyBuffer buf = new MyBuffer();
            buf.Append(MsgType).Append(0).AppendCStr(Tag);
            return FixHeader(buf).ToBytes();
        }
    }
    public class CopyData : Msg
    {
        public byte[] Data;

        public static Msg FromBuffer(MyBuffer buf, int idx)
        {
            idx += 1;
            int sz = buf.GetInt(idx); idx += 4;
            byte[] data = buf[idx, sz - 4];
            return new CopyData { Data = data };
        }
        public override byte MsgType => (byte)BeMsgType.CopyData;
        public override byte[] ToBytes()
        {
            MyBuffer buf = new MyBuffer();
            buf.Append(MsgType).Append(0).Append(Data);
            return FixHeader(buf).ToBytes();
        }
    }
    public class CopyDone : EmptyMsg<CopyDone>
    {
        public override byte MsgType => (byte)BeMsgType.CopyDone;
    }
    public abstract class CopyResponse<T> : Msg where T : CopyResponse<T>, new()
    {
        public byte OverallFc;
        public short FcCnt => (short)Fcs.Length;
        public short[] Fcs = MiscUtil.EmptyShorts;

        public static Msg FromBuffer(MyBuffer buf, int idx)
        {
            idx += 5;
            byte overallFc = buf.GetByte(idx); idx += 1;
            short fcCnt = buf.GetShort(idx); idx += 2;
            short[] fcs = buf.GetN<short>(fcCnt, idx); idx += 2 * fcs.Length;
            return new T { OverallFc = overallFc, Fcs = fcs };
        }
        public override byte[] ToBytes()
        {
            MyBuffer buf = new MyBuffer();
            buf.Append(MsgType).Append(0).Append(OverallFc).Append(FcCnt).Append(Fcs);
            return FixHeader(buf).ToBytes();
        }
    }
    public class CopyInResponse : CopyResponse<CopyInResponse>
    {
        public override byte MsgType => (byte)BeMsgType.CopyInResponse;
    }
    public class CopyOutResponse : CopyResponse<CopyOutResponse>
    {
        public override byte MsgType => (byte)BeMsgType.CopyOutResponse;
    }
    public class CopyBothResponse : CopyResponse<CopyBothResponse>
    {
        public override byte MsgType => (byte)BeMsgType.CopyBothResponse;
    }
    public class DataRow : Msg
    {
        public List<byte[]> ColValues = new List<byte[]>();

        public static Msg FromBuffer(MyBuffer buf, int idx)
        {
            idx += 5;
            List<byte[]> colValues = buf.Get24X(idx, out int sz); idx += sz;
            return new DataRow { ColValues = colValues };
        }
        public override byte MsgType => (byte)BeMsgType.DataRow;
        public override byte[] ToBytes()
        {
            MyBuffer buf = new MyBuffer();
            buf.Append(MsgType).Append(0).Append24X(ColValues);
            return FixHeader(buf).ToBytes();
        }
    }
    public class EmptyQueryResponse : EmptyMsg<EmptyQueryResponse>
    {
        public override byte MsgType => (byte)BeMsgType.EmptyQueryResponse;
    }
    public class FieldInfo
    {
        public FieldType FType { get; }
        public byte[] FValue { get; }
        public FieldInfo(FieldType fType, byte[] fValue)
        {
            FType = fType;
            FValue = fValue;
        }
        public byte[] ToBytes()
        {
            byte[] res = new byte[FValue.Length + 1];
            res[0] = (byte)FType;
            Array.Copy(FValue, 0, res, 1, FValue.Length);
            return res;
        }
    }
    public static class FieldInfoExtensions
    {
        public static FieldInfo Make(this FieldType ft, byte[] value) => new FieldInfo(ft, value);
    }
    public abstract class ErrorNoticeResponse<T> : Msg where T : ErrorNoticeResponse<T>, new()
    {
        public List<FieldInfo> Fields = new List<FieldInfo>();

        public static Msg FromBuffer(MyBuffer buf, int idx)
        {
            idx += 5;
            List<byte[]> rawFields = buf.GetX(idx, out int sz); idx += sz;
            List<FieldInfo> fields = new List<FieldInfo>();
            foreach (byte[] f in rawFields)
            {
                fields.Add(new FieldInfo((FieldType)f[0], f.Slice(1)));
            }
            return new T { Fields = fields };
        }
        public override byte[] ToBytes()
        {
            MyBuffer buf = new MyBuffer();
            buf.Append(MsgType).Append(0);
            buf.AppendX(Fields.Select(fi => fi.ToBytes()));
            return FixHeader(buf).ToBytes();
        }

        public static T MakeError(byte[] message, byte[] detail = null, byte[] hint = null)
        {
            T msg = new T();
            byte[] err = "ERROR".Ascii();
            msg.Fields.Add(FieldType.Severity.Make(err));
            msg.Fields.Add(FieldType.Severity2.Make(err));
            if (detail != null)
                msg.Fields.Add(FieldType.Detail.Make(detail));
            if (hint != null)
                msg.Fields.Add(FieldType.Hint.Make(hint));
            return msg;
        }
    }
    public class ErrorResponse : ErrorNoticeResponse<ErrorResponse>
    {
        public override byte MsgType => (byte)BeMsgType.ErrorResponse;
    }
    public class NoticeResponse : ErrorNoticeResponse<NoticeResponse>
    {
        public override byte MsgType => (byte)BeMsgType.NoticeResponse;
    }
    public class FunctionCallResponse : Msg
    {
        public byte[] ResValue;

        public static Msg FromBuffer(MyBuffer buf, int idx)
        {
            idx += 5;
            byte[] val = buf.Get4x(idx, out int sz); idx += sz;
            return new FunctionCallResponse { ResValue = val };
        }
        public override byte MsgType => (byte)BeMsgType.FunctionCallResponse;
        public override byte[] ToBytes()
        {
            MyBuffer buf = new MyBuffer();
            buf.Append(MsgType).Append(0).Append4x(ResValue);
            return FixHeader(buf).ToBytes();
        }
    }
    public class NoData : EmptyMsg<NoData>
    {
        public override byte MsgType => (byte)BeMsgType.NoData;
    }
    public class NotificationResponse : Msg
    {
        public int Pid;
        public byte[] Channel;
        public byte[] Payload;

        public static Msg FromBuffer(MyBuffer buf, int idx)
        {
            idx += 5;
            int pid = buf.GetInt(idx); idx += 4;
            byte[] channel = buf.GetCStr(idx); idx += channel.Length + 1;
            byte[] payload = buf.GetCStr(idx); idx += payload.Length + 1;
            return new NotificationResponse { Pid = pid, Channel = channel, Payload = payload };
        }
        public override byte MsgType => (byte)BeMsgType.NotificationResponse;
        public override byte[] ToBytes()
        {
            MyBuffer buf = new MyBuffer();
            buf.Append(MsgType).Append(0).Append(Pid).AppendCStr(Channel).AppendCStr(Payload);
            return FixHeader(buf).ToBytes();
        }
    }
    public class ParameterDescription : Msg
    {
        public short OidCnt => (short)Oids.Length;
        public int[] Oids = MiscUtil.EmptyInts;

        public static Msg FromBuffer(MyBuffer buf, int idx)
        {
            idx += 5;
            short oidCnt = buf.GetShort(idx); idx += 2;
            int[] oids = buf.GetN<int>(oidCnt, idx); idx += 4 * oids.Length;
            return new ParameterDescription { Oids = oids };
        }
        public override byte MsgType => (byte)BeMsgType.ParameterDescription;
        public override byte[] ToBytes()
        {
            MyBuffer buf = new MyBuffer();
            buf.Append(MsgType).Append(0).Append(OidCnt).Append(Oids);
            return FixHeader(buf).ToBytes();
        }
    }
    public class ParameterStatus : Msg
    {
        public byte[] Name;
        public byte[] Value;

        public static Msg FromBuffer(MyBuffer buf, int idx)
        {
            idx += 5;
            byte[] name = buf.GetCStr(idx); idx += name.Length + 1;
            byte[] value = buf.GetCStr(idx); idx += value.Length + 1;
            return new ParameterStatus { Name = name, Value = value };
        }
        public override byte MsgType => (byte)BeMsgType.ParameterStatus;
        public override byte[] ToBytes()
        {
            MyBuffer buf = new MyBuffer();
            buf.Append(MsgType).Append(0).AppendCStr(Name).AppendCStr(Value);
            return FixHeader(buf).ToBytes();
        }
    }
    public class ParseComplete : EmptyMsg<ParseComplete>
    {
        public override byte MsgType => (byte)BeMsgType.ParseComplete;
    }
    public class PortalSuspended : EmptyMsg<PortalSuspended>
    {
        public override byte MsgType => (byte)BeMsgType.PortalSuspended;
    }
    public class ReadyForQuery : Msg
    {
        public TransStatus TStatus;

        public static Msg FromBuffer(MyBuffer buf, int idx)
        {
            idx += 5;
            TransStatus status = (TransStatus)buf.GetByte(idx); idx += 1;
            return new ReadyForQuery { TStatus = status };
        }
        public override byte MsgType => (byte)BeMsgType.ReadyForQuery;
        public override byte[] ToBytes()
        {
            MyBuffer buf = new MyBuffer();
            buf.Append(MsgType).Append(0).Append((byte)TStatus);
            return FixHeader(buf).ToBytes();
        }

        public static readonly ReadyForQuery Idle = new ReadyForQuery { TStatus = TransStatus.Idle };
        public static readonly ReadyForQuery InBlock = new ReadyForQuery { TStatus = TransStatus.InBlock };
        public static readonly ReadyForQuery Fail = new ReadyForQuery { TStatus = TransStatus.Fail };
    }
    public class ColumnInfo
    {
        public byte[] Name;
        public int TableOid = 99999;
        public short AttNum;
        public int TypOid = 25;
        public short TypLen = -1;
        public int TypMod = -1;
        public short FmtCode = 0;

        public byte[] ToBytes()
        {
            MyBuffer buf = new MyBuffer();
            buf.AppendCStr(Name).Append(TableOid).Append(AttNum).Append(TypOid).Append(TypLen).Append(TypMod).Append(FmtCode);
            return buf.ToBytes();
        }
        public static ColumnInfo FromBuffer(MyBuffer buf, int idx, out int sz)
        {
            int oldidx = idx;
            byte[] name = buf.GetCStr(idx); idx += name.Length + 1;
            int tableOid = buf.GetInt(idx); idx += 4;
            short attNum = buf.GetShort(idx); idx += 2;
            int typOid = buf.GetInt(idx); idx += 4;
            short typLen = buf.GetShort(idx); idx += 2;
            int typMod = buf.GetInt(idx); idx += 4;
            short fmtCode = buf.GetShort(idx); idx += 2;
            sz = idx - oldidx;
            return new ColumnInfo
            {
                Name = name,
                TableOid = tableOid, AttNum = attNum,
                TypOid = typOid, TypLen = typLen,
                TypMod = typMod, FmtCode = fmtCode
            };
        }
    }
    public class RowDescription : Msg
    {
        public short ColumnCount => (short)Columns.Count;
        public List<ColumnInfo> Columns = new List<ColumnInfo>();

        public static Msg FromBuffer(MyBuffer buf, int idx)
        {
            idx += 5;
            short colCnt = buf.GetShort(idx); idx += 2;
            RowDescription msg = new RowDescription();
            for (short i = 0; i < colCnt; ++i)
            {
                msg.Columns.Add(ColumnInfo.FromBuffer(buf, idx, out int sz));
                idx += sz;
            }
            return msg;
        }
        public override byte MsgType => (byte)BeMsgType.RowDescription;
        public override byte[] ToBytes()
        {
            MyBuffer buf = new MyBuffer();
            buf.Append(MsgType).Append(0).Append(ColumnCount);
            foreach (ColumnInfo ci in Columns)
                buf.Append(ci.ToBytes());
            return buf.ToBytes();
        }
    }
    public static class ColumnRowExtensions
    {
        public static ColumnInfo Column(this byte[] name, short attNum) => new ColumnInfo { Name = name, AttNum = attNum };
        public static RowDescription RowDesc(this byte[] names, byte delim = 0)
        {
            if (delim == 0)
                delim = (byte)' ';
            byte[][] nmlist = names.Split(delim);
            RowDescription msg = new RowDescription();
            for (int i = 0; i < nmlist.Length; ++i)
            {
                msg.Columns.Add(nmlist[i].Column((short)(i + 1)));
            }
            return msg;
        }
        public static RowDescription RowDesc(this IEnumerable<byte[]> names)
        {
            RowDescription msg = new RowDescription();
            short attNum = 1;
            foreach (byte[] nm in names)
            {
                msg.Columns.Add(nm.Column(attNum));
                ++attNum;
            }
            return msg;
        }
    }
    // first msg from fe
    public class ParamInfo : IComparable<ParamInfo>, IEquatable<ParamInfo>, IComparable
    {
        public string Name;
        public byte[] Value;

        public ParamInfo(string name, byte[] value)
        {
            Name = name;
            Value = value;
        }

        public int CompareTo(ParamInfo other)
        {
            if (other == null)
                return 1;
            int res = Name.CompareTo(other.Name);
            if (res != 0)
                return res;
            return ((IStructuralComparable)Value).CompareTo(other.Value, Comparer<byte>.Default);
        }
        public int CompareTo(object obj)
        {
            return CompareTo(obj as ParamInfo);
        }
        public bool Equals(ParamInfo other)
        {
            if (other == null)
                return false;
            return Name.Equals(other.Name) && ((IStructuralEquatable)Value).Equals(other.Value, EqualityComparer<byte>.Default);
        }
        public override bool Equals(object obj)
        {
            return Equals(obj as ParamInfo);
        }
        public override int GetHashCode()
        {
            unchecked
            {
                int hv = 17;
                hv = hv * 23 + Name.GetHashCode();
                hv = hv * 23 + ((IStructuralEquatable)Value).GetHashCode(EqualityComparer<byte>.Default);
                return hv;
            }
        }
    }
    // 手动创建StartupMesage后，需要调用Params.Sort()。
    public class StartupMessage : Msg, IEquatable<StartupMessage>
    {
        public int Code => Msg.PG_PROTO_VERSION3_CODE;
        public List<ParamInfo> Params = new List<ParamInfo>();

        public static Msg FromBuffer(MyBuffer buf, int idx)
        {
            idx += 8;
            List<byte[]> kvlist = buf.GetX(idx, out int sz); idx += sz;
            StartupMessage msg = new StartupMessage();
            for (int i = 0; i < kvlist.Count; ++i)
            {
                byte[] key = kvlist[i];
                byte[] val = kvlist[++i];
                msg.Params.Add(new ParamInfo(key.Ascii(), val));
            }
            msg.Params.Sort();
            return msg;
        }
        public override byte MsgType => (byte)0;
        public override byte[] ToBytes()
        {
            MyBuffer buf = new MyBuffer();
            buf.Append(0).Append(Code);
            List<byte[]> kvlist = new List<byte[]>();
            foreach (ParamInfo pi in Params)
            {
                kvlist.Add(pi.Name.Ascii());
                kvlist.Add(pi.Value);
            }
            buf.AppendX(kvlist);
            buf[0, 4] = buf.Count.GetBytes();
            return buf.ToBytes();
        }

        public override bool Equals(object obj)
        {
            return Equals(obj as StartupMessage);
        }
        public bool Equals(StartupMessage other)
        {
            if (other == null)
                return false;
            if (Params.Count != other.Params.Count)
                return false;
            for (int i = 0; i < Params.Count; ++i)
            {
                if (!Params[i].Equals(other.Params[i]))
                    return false;
            }
            return true;
        }
        public override int GetHashCode()
        {
            unchecked
            {
                int hv = 17;
                foreach (ParamInfo pi in Params)
                {
                    hv = hv * 23 + pi.GetHashCode();
                }
                return hv;
            }
        }
    }
    public class CancelRequest : Msg
    {
        public int Code => Msg.PG_CANCELREQUEST_CODE;
        public int Pid;
        public int SecretKey;

        public static Msg FromBuffer(MyBuffer buf, int idx)
        {
            idx += 8;
            int pid = buf.GetInt(idx); idx += 4;
            int skey = buf.GetInt(idx); idx += 4;
            return new CancelRequest { Pid = pid, SecretKey = skey };
        }
        public override byte MsgType => (byte)0;
        public override byte[] ToBytes()
        {
            MyBuffer buf = new MyBuffer();
            buf.Append(0).Append(Code).Append(Pid).Append(SecretKey);
            buf[0, 4] = buf.Count.GetBytes();
            return buf.ToBytes();
        }
    }
    public class SSLRequest : Msg
    {
        public int Code => Msg.PG_SSLREQUEST_CODE;

        public static Msg FromBuffer(MyBuffer buf, int idx)
        {
            idx += 8;
            return new SSLRequest();
        }
        public override byte MsgType => (byte)0;
        public override byte[] ToBytes()
        {
            MyBuffer buf = new MyBuffer();
            buf.Append(8).Append(Code);
            return buf.ToBytes();
        }
    }
    public static class MsgExtensions
    {
        public static Query Query(this string sql, string encoding = null) => new Query { Sql = sql.GetBytes(encoding) };
        public static Query Query(this byte[] sql) => new Query { Sql = sql };

        public static Parse Parse(this string sql, string stmt = null, string encoding = null, int[] paramOids = null)
        {
            return new Parse
            {
                Stmt = stmt ==null ? MiscUtil.EmptyBytes : stmt.GetBytes(encoding),
                Sql = sql.GetBytes(encoding),
                ParamOids = paramOids ?? MiscUtil.EmptyInts
            };
        }
        public static Parse Parse(this byte[] sql, byte[] stmt = null, int[] paramOids = null)
        {
            return new Parse
            {
                Stmt = stmt ?? MiscUtil.EmptyBytes,
                Sql = sql,
                ParamOids = paramOids ?? MiscUtil.EmptyInts
            };
        }

        public static Bind Bind(this string stmt, List<string> paramValues = null, string encoding = null, string portal = null, short[] fcs = null, short[] resFcs = null)
        {
            return new Bind
            {
                Portal = portal == null ? MiscUtil.EmptyBytes : portal.GetBytes(encoding),
                Stmt = stmt.GetBytes(encoding),
                Fcs = fcs ?? MiscUtil.EmptyShorts,
                Params = paramValues == null ? new List<byte[]>() : paramValues.Select(s => s?.GetBytes(encoding)).ToList(),
                ResFcs = resFcs ?? MiscUtil.EmptyShorts
            };
        }
        public static Bind Bind(this byte[] stmt, List<byte[]> paramValues = null, byte[] portal = null, short[] fcs = null, short[] resFcs = null)
        {
            return new Bind
            {
                Portal = portal ?? MiscUtil.EmptyBytes,
                Stmt = stmt,
                Fcs = fcs ?? MiscUtil.EmptyShorts,
                Params = paramValues ?? new List<byte[]>(),
                ResFcs = resFcs ?? MiscUtil.EmptyShorts
            };
        }

        public static Execute Execute(this string portal, string encoding = null, int maxNum = 0)
        {
            return new Execute { Portal = portal.GetBytes(encoding), MaxNum = maxNum };
        }
        public static Execute Execute(this byte[] portal, int maxNum = 0)
        {
            return new Execute { Portal = portal, MaxNum = maxNum };
        }

        public static Describe DescribeStmt(this string name, string encoding = null) => Describe.Stmt(name.GetBytes(encoding));
        public static Describe DescribePortal(this string name, string encoding = null) => Describe.Portal(name.GetBytes(encoding));
        public static Describe DescribeStmt(this byte[] name) => Describe.Stmt(name);
        public static Describe DescribePortal(this byte[] name) => Describe.Portal(name);

        public static Close CloseStmt(this string name, string encoding = null) => Close.Stmt(name.GetBytes(encoding));
        public static Close ClosePortal(this string name, string encoding = null) => Close.Portal(name.GetBytes(encoding));
        public static Close CloseStmt(this byte[] name) => Close.Stmt(name);
        public static Close ClosePortal(this byte[] name) => Close.Portal(name);

        public static CopyFail CopyFail(this string errInfo, string encoding = null) => new CopyFail { ErrInfo = errInfo.GetBytes(encoding) };
        public static CopyFail CopyFail(this byte[] errInfo) => new CopyFail { ErrInfo = errInfo };

        public static FunctionCall FunctionCall(this int funcOid, List<string> args, string encoding = null, short[] fcs = null, short resFc = 0)
        {
            List<byte[]> args2 = args.Select(s => s?.GetBytes(encoding)).ToList();
            return funcOid.FunctionCall(args2, fcs, resFc);
        }
        public static FunctionCall FunctionCall(this int funcOid, List<byte[]> args, short[] fcs = null, short resFc = 0)
        {
            return new FunctionCall
            {
                FuncOid = funcOid,
                Fcs = fcs ?? MiscUtil.EmptyShorts,
                Args = args,
                ResFc = resFc
            };
        }

        // encoding对md5salt无效，md5salt是字节串。
        public static PasswordMessage PasswordMessage(this string pwd, string user = null, string md5salt = null, string encoding = null)
        {
            return pwd.GetBytes(encoding).PasswordMessage(user?.GetBytes(encoding), md5salt?.Bytes());
        }
        public static PasswordMessage PasswordMessage(this byte[] pwd, byte[] user = null, byte[] md5salt = null)
        {
            if (md5salt != null)
            {
                if (user == null)
                    throw new PgProtoException(string.Format("Argument user should not be null while md5salt is not null"));
                byte[] md5prefix = "md5".Ascii();
                if (pwd.StartsWith(md5prefix) && pwd.Length == 35)
                    pwd = md5prefix.Concat(pwd.Slice(3).Concat(md5salt).Md5());
                else
                    pwd = md5prefix.Concat(pwd.Concat(user).Md5().Concat(md5salt).Md5());
            }
            return new PasswordMessage { Password = pwd };
        }

        // MechName/Data都是只包含可打印字符，因此基本上可以用任何encoding都可以。
        public static SASLInitialResponse SASLInitialResponse(this string mechName, string data, string encoding = null)
        {
            return new SASLInitialResponse { MechName = mechName.GetBytes(encoding), Data = data.GetBytes(encoding) };
        }
        public static SASLInitialResponse SASLInitialResponse(this byte[] mechName, byte[] data)
        {
            return new SASLInitialResponse { MechName = mechName, Data = data };
        }
        
        public static SASLResponse SASLResponse(this string data, string encoding = null) => new SASLResponse { Data = data.GetBytes(encoding) };
        public static SASLResponse SASLResponse(this byte[] data) => new SASLResponse { Data = data };

        public static GSSResponse GSSResponse(this string data, string encoding = null) => new GSSResponse { Data = data.GetBytes(encoding) };
        public static GSSResponse GSSResponse(this byte[] data) => new GSSResponse { Data = data };

        public static Authentication Ok(this string s) => Authentication.Ok();
        public static Authentication KerberosV5(this string s) => Authentication.KerberosV5();
        public static Authentication CleartextPassword(this string s) => Authentication.CleartextPassword();
        public static Authentication MD5Password(this string salt) => Authentication.MD5Password(salt.Bytes());
        public static Authentication MD5Password(this byte[] salt) => Authentication.MD5Password(salt);
        public static Authentication SCMCredential(this string s) => Authentication.SCMCredential();
        public static Authentication GSS(this string s) => Authentication.GSS();
        public static Authentication SSPI(this string s) => Authentication.SSPI();
        public static Authentication GSSContinue(this string data, string encoding = null) => Authentication.GSSContinue(data.GetBytes(encoding));
        public static Authentication GSSContinue(this byte[] data) => Authentication.GSSContinue(data);
        public static Authentication SASL(this string mechNames, string encoding = null)
        {
            byte[][] names = mechNames.Split(' ').Select(s => s.GetBytes(encoding)).ToArray();
            return Authentication.SASL(names);
        }
        public static Authentication SASL(this byte[] mechNames)
        {
            byte[][] names = mechNames.Split((byte)' ');
            return Authentication.SASL(names);
        }
        public static Authentication SASLContinue(this string data, string encoding = null) => Authentication.SASLContinue(data.GetBytes(encoding));
        public static Authentication SASLContinue(this byte[] data) => Authentication.SASLContinue(data);
        public static Authentication SASLFinal(this string data, string encoding = null) => Authentication.SASLFinal(data.GetBytes(encoding));
        public static Authentication SASLFinal(this byte[] data) => Authentication.SASLFinal(data);

        public static BackendKeyData BackendKeyData(this int pid, int secretKey) => new BackendKeyData { Pid = pid, SecretKey = secretKey };

        public static StartupMessage StartupMessag(this string paramStr, char delim1 = ' ', char delim2 = '=', string encoding = null)
        {
            string[] paramArr = paramStr.Split(delim1);
            StartupMessage msg = new StartupMessage();
            foreach (string p in paramArr)
            {
                string[] kv = p.Split(delim2, 2);
                msg.Params.Add(new ParamInfo(kv[0], kv[1].GetBytes(encoding)));
            }
            msg.Params.Sort();
            return msg;
        }
    }
    public static class MyBufferExtensions
    {
        public static Msg GetMsg(this MyBuffer buf, bool fe = true, bool advance = true)
        {
            return buf.GetMsg(0, fe, advance);
        }
        public static Msg GetMsg(this MyBuffer buf, int startIdx, bool fe = true, bool advance = true)
        {
            if (startIdx + 5 > buf.Count)
                return null;
            int sz = buf.GetInt(startIdx + 1);
            if (startIdx + sz + 1 > buf.Count)
                return null;
            byte msgtype = buf[startIdx];
            Msg msg = (fe ? Msg.FeMsgMap : Msg.BeMsgMap)[msgtype](buf, startIdx);
            if (advance)
                buf.Advance(sz + 1);
            return msg;
        }
        public static Msg GetStartupMsg(this MyBuffer buf)
        {
            if (buf.Count < 4)
                return null;
            int sz = buf.GetInt(0);
            if (buf.Count < sz)
                return null;
            int code = buf.GetInt(4);
            Msg msg = null;
            if (code == Msg.PG_PROTO_VERSION3_CODE)
                msg = StartupMessage.FromBuffer(buf, 0);
            else if (code == Msg.PG_CANCELREQUEST_CODE)
                msg = CancelRequest.FromBuffer(buf, 0);
            else if (code == Msg.PG_SSLREQUEST_CODE)
                msg = SSLRequest.FromBuffer(buf, 0);
            else
                throw new PgProtoException(string.Format("unknown startup msg. code:{0}", code));
            buf.Advance(sz);
            return msg;
        }
    }
} // end of namespace PGProtocol3
