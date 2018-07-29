// 
// 实用类
// 
using System;
using System.Text;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using System.Security.Cryptography;
using System.Net;
using System.Net.Sockets;
using System.Globalization;

using Zhb.ExtensionMethods;
namespace Zhb.Utils
{
    public static class MiscUtil
    {
        public static readonly byte[] EmptyBytes = Array.Empty<byte>();
        public static readonly short[] EmptyShorts = Array.Empty<short>();
        public static readonly int[] EmptyInts = Array.Empty<int>();
        public static int ThreadId => Thread.CurrentThread.ManagedThreadId;
    }

    public class MyBuffer
    {
        private byte[] buf;
        private int sidx, eidx;
        private int expandsz;
        public static int OutWidth = 20; // ToString最多输出多少个字节
    
        // bufsz : 初始buf大小
        // expandsz : 每次扩展时新增的单位大小
        public MyBuffer(int bufsz = 512, int expandsz = 0)
        {
            this.buf = new byte[bufsz];
            this.sidx = this.eidx = 0;
            this.expandsz = (expandsz <= 0 ? bufsz : expandsz);
        }
        public MyBuffer(MyBuffer frombuffer, int startidx, int count, int expandsz = 0)
        {
            frombuffer.CheckIdx(startidx).CheckIdx(startidx + count - 1);
            this.buf = new byte[count];
            Array.Copy(frombuffer.buf, frombuffer.sidx + startidx, this.buf, 0, count);
            this.sidx = 0;
            this.eidx = count;
            this.expandsz = (expandsz <= 0 ? frombuffer.expandsz : expandsz);
        }
        public MyBuffer(byte[] data, bool copy = true, int expandsz =  0)
        {
            this.buf = data;
            if (copy)
            {
                this.buf = new byte[data.Length];
                Array.Copy(data, 0, this.buf, 0, data.Length);
            }
            this.sidx = 0;
            this.eidx = data.Length;
            this.expandsz = (expandsz <= 0 ? data.Length : expandsz);
        }
        public int Count => eidx - sidx;
        public int Capacity => buf.Length;
        public byte this[int idx]
        {
            get => CheckIdx(idx).buf[sidx + idx];
            set => CheckIdx(idx).buf[sidx + idx] = value;
        }
        public byte[] this[int idx, int count]
        {
            get
            {
                if (count < 0 || idx + count > Count)
                    count = Count - idx;
                CheckIdx(idx).CheckIdx(idx + count - 1);
                byte[] res = new byte[count];
                Array.Copy(buf, sidx + idx, res, 0, count);
                return res;
            }
            set
            {
                CheckIdx(idx).CheckIdx(idx + count - 1);
                if (value.Length != count)
                    throw new ArgumentException($"value.Length({value.Length}) != count({count})");
                Array.Copy(value, 0, buf, sidx + idx, count);
            }
        }
        public MyBuffer Advance(int count)
        {
            if (count > Count)
                throw new ArgumentException($"count({count}) > Count({Count})");
            sidx += count;
            return this;
        }
        public byte[] ToBytes()
        {
            byte[] res = new byte[Count];
            Array.Copy(buf, sidx, res, 0, Count);
            return res;
        }
        // ************************************************************************************************
        // Append/Get基本类型
        public MyBuffer Append(byte[] data)
        {
            if (data.Length <= buf.Length - eidx)
            {
                Array.Copy(data, 0, buf, eidx, data.Length);
                eidx += data.Length;
                return this;
            }
            ExtendBuf(data.Length);
            return Append(data);
        }
        public MyBuffer Append(byte b)
        {
            ExtendBuf(1);
            buf[eidx++] = b;
            return this;
        }
        public MyBuffer Append(sbyte b) => Append((byte)b);
        public MyBuffer Append(ushort s, bool bigendian = true) => Append(s.GetBytes(bigendian));
        public MyBuffer Append(short s, bool bigendian = true) => Append(s.GetBytes(bigendian));
        public MyBuffer Append(uint n, bool bigendian = true) => Append(n.GetBytes(bigendian));
        public MyBuffer Append(int n, bool bigendian = true) => Append(n.GetBytes(bigendian));
        public MyBuffer Append(ulong n, bool bigendian = true) => Append(n.GetBytes(bigendian));
        public MyBuffer Append(long n, bool bigendian = true) => Append(n.GetBytes(bigendian));
        // 有3种方法来调用具体类型的Append(速度从慢到快排列，数字是静态绑定的倍数):
        // .) 用dynamic，运行时解析重载。10
        // .) 通过类型T查找appendMap，获得对应类型的delegate。4
        // .) 直接指定appendF参数。2
        public MyBuffer Append<T>(T[] arr, bool bigendian = true, Func<T, bool, MyBuffer> appendF = null) where T : struct
        {
            Func<MyBuffer, T, bool, MyBuffer> appendF2 = null;
            if (appendF == null)
                appendF2 = (Func<MyBuffer, T, bool, MyBuffer>)appendMap[typeof(T)];
            foreach (T t in arr)
            {
                // 好像不支持直接调用Append(t, bigendian)。
                // 不过可以用Append((dynamic)t, bigendian)，就是比较慢。
                if (appendF == null)
                    appendF2(this, t, bigendian);
                else
                    appendF(t, bigendian);
            }
            return this;
        }
    
        public byte GetByte(int idx) => this[idx];
        public sbyte GetSByte(int idx) => (sbyte)this[idx];
        public ushort GetUShort(int idx, bool bigendian = true) => this[idx, 2].ToUInt16(bigendian);
        public short GetShort(int idx, bool bigendian = true) => this[idx, 2].ToInt16(bigendian);
        public uint GetUInt(int idx, bool bigendian = true) => this[idx, 4].ToUInt32(bigendian);
        public int GetInt(int idx, bool bigendian = true) => this[idx, 4].ToInt32(bigendian);
        public ulong GetULong(int idx, bool bigendian = true) => this[idx, 8].ToUInt64(bigendian);
        public long GetLong(int idx, bool bigendian = true) => this[idx, 8].ToInt64(bigendian);
        public T[] GetN<T>(int n, int idx, bool bigendian = true, Func<int, bool, T> getF = null) where T : struct
        {
            Func<MyBuffer, int, bool, T> getF2 = null;
            if (getF == null)
                getF2 = (Func<MyBuffer, int, bool, T>)getMap[typeof(T)];
            T[] res = new T[n];
            for (int i = 0; i < n; ++i)
            {
                if (getF == null)
                    res[i] = getF2(this, idx, bigendian);
                else
                    res[i] = getF(idx, bigendian);
                idx += Marshal.SizeOf<T>();
            }
            return res;
        }
        // *******************************************************************************************
        // Append/Get特殊类型
        // x 获得\x00结尾的字节串，不包括结尾的\x00
        public byte[] GetCStr(int idx)
        {
            int idx2 = idx;
            while (this[idx2] != 0)
                idx2++;
            return this[idx, idx2 - idx];
        }
        public MyBuffer AppendCStr(byte[] s)
        {
            Append(s);
            if (s.Length == 0 || s[s.Length - 1] != 0)
                Append((byte)0);
            return this;
        }
        // X : 多个非空字节串，其中单个字节串以\x00结尾，最后整个X也以\x00结尾
        public List<byte[]> GetX(int idx, out int sz)
        {
            var res = new List<byte[]>();
            int oldidx = idx;
            while (this[idx] != 0)
            {
                byte[] s = GetCStr(idx);
                res.Add(s);
                idx += s.Length + 1;
            }
            sz = idx - oldidx + 1;
            return res;
        }
        public MyBuffer AppendX(IEnumerable<byte[]> X)
        {
            foreach (byte[] s in X)
                AppendCStr(s);
            return Append((byte)0);
        }
        // 4x : 单个字节串，开头4个字节表示字节串的长度，如果小于0表示NULL。
        public byte[] Get4x(int idx, out int sz)
        {
            byte[] res = null;
            int slen = GetInt(idx);
            if (slen >= 0)
                res = this[idx + 4, slen];
            sz = (res == null ? 4 : 4 + slen);
            return res;
        }
        public MyBuffer Append4x(byte[] s)
        {
            if (s == null)
                Append((int)-1);
            else
                Append((int)s.Length).Append(s);
            return this;
        }
        // 24X : 开始2个字节表示字节串的个数，每个字节串前面4个字节表示字节串的大小，如果小于0表示是NULL。
        public List<byte[]> Get24X(int idx, out int sz)
        {
            var res = new List<byte[]>();
            int oldidx = idx;
            int cnt = GetShort(idx);
            idx += 2;
            for (int i = 0; i < cnt; ++i)
            {
                int sz2 = 0;
                res.Add(Get4x(idx, out sz2));
                idx += sz2;
            }
            sz = idx - oldidx;
            return res;
        }
        public MyBuffer Append24X(List<byte[]> XX)
        {
            Append((short)XX.Count);
            foreach (byte[] s in XX)
                Append4x(s);
            return this;
        }

        public override string ToString()
        {
            string prefix = $"<MyBuffer sidx:{sidx} eidx:{eidx} buf:{buf.Length} expandsz:{expandsz}> ";
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < Count && i < OutWidth; ++i)
            {
                byte b = buf[sidx + i];
                if (b > 32 && b < 127)
                    sb.AppendFormat("{0}", (char)b);
                else
                    sb.AppendFormat("\\x{0:X2}", b);
            }
            if (Count > OutWidth)
                sb.Append(" ......");
            return prefix + sb.ToString();
        }
        // ********** private **************************************************
        private MyBuffer ExtendBuf(int addsz)
        {
            if (addsz <= buf.Length - eidx)
                return this;
            int oldcount = Count;
            if (addsz <= Capacity - oldcount)
            {
                Array.Copy(buf, sidx, buf, 0, oldcount);
            }
            else
            {
                int sz = addsz - (Capacity - oldcount);
                byte[] newbuf = new byte[buf.Length + (sz / expandsz + 1) * expandsz];
                Array.Copy(buf, sidx, newbuf, 0, oldcount);
                buf = newbuf;
            }
            sidx = 0;
            eidx = oldcount;
            return this;
        }
        private MyBuffer CheckIdx(int idx)
        {
            if (idx < 0 || idx >= Count)
                throw new ArgumentOutOfRangeException("idx", $"idx:{idx} Count:{Count} sidx:{sidx} eidx:{eidx}");
            return this;
        }
        // ***************** static member *******************************************
        static Dictionary<Type, Delegate> appendMap = new Dictionary<Type, Delegate>();
        static Dictionary<Type, Delegate> getMap = new Dictionary<Type, Delegate>();
        static void Add2AppendMap<T>()
        {
            appendMap[typeof(T)] = typeof(MyBuffer).GetMethod("Append", new[]{typeof(T), typeof(bool)}).CreateDelegate(typeof(Func<MyBuffer, T, bool, MyBuffer>));
        }
        static void Add2GetMap<T>(string mname)
        {
            getMap[typeof(T)] = typeof(MyBuffer).GetMethod(mname).CreateDelegate(typeof(Func<MyBuffer, int, bool, T>));
        }
        static MyBuffer()
        {
            Add2AppendMap<ushort>(); Add2AppendMap<short>(); 
            Add2AppendMap<uint>(); Add2AppendMap<int>();
            Add2AppendMap<ulong>(); Add2AppendMap<long>();
        
            Add2GetMap<ushort>("GetUShort"); Add2GetMap<short>("GetShort"); 
            Add2GetMap<uint>("GetUInt"); Add2GetMap<int>("GetInt"); 
            Add2GetMap<ulong>("GetULong"); Add2GetMap<long>("GetLong"); 
        }
    }

    public class PrintDuration : IDisposable
    {
        private Stopwatch sw = new Stopwatch();
        private string prefixStr;
        public PrintDuration(string s = "")
        {
            this.prefixStr = s;
            this.sw.Start();
        }
        public void Dispose()
        {
            this.sw.Stop();
            double sec = (double)sw.ElapsedMilliseconds / 1000.0;
            Console.WriteLine("{0}{1:f3} seconds", this.prefixStr, sec);
        }
    }
} // end of Zhb.Utils

namespace Zhb.ExtensionMethods
{
    public static class ArrayUtil
    {
        public static void ForEach<T>(this IEnumerable<T> cobj, Action<T> act)
        {
            foreach (T t in cobj)
                act(t);
        }
        public static byte[] Md5(this byte[] data, bool hex = true)
        {
            MD5 md5hash = MD5.Create();
            byte[] data2 = md5hash.ComputeHash(data);
            if (!hex)
                return data2;
            StringBuilder sb = new StringBuilder();
            foreach (byte b in data2)
                sb.Append(b.ToString("x2"));
            return Encoding.ASCII.GetBytes(sb.ToString());
        }
        public static T[] Slice<T>(this T[] arr, int startIdx, int sz = -1)
        {
            if (sz < 0)
                sz = arr.Length - startIdx;
            T[] res = new T[sz];
            Array.Copy(arr, startIdx, res, 0, sz);
            return res;
        }
        public static T[] Concat<T>(this T[] arr, params T[][] args)
        {
            int sz = arr.Length;
            foreach (T[] x in args)
                sz += x.Length;
            T[] res = new T[sz];
            int idx = 0;
            Array.Copy(arr, 0, res, idx, arr.Length);
            idx += arr.Length;
            foreach (T[] x in args)
            {
                Array.Copy(x, 0, res, idx, x.Length);
                idx += x.Length;
            }
            return res;
        }
        public static bool SubAt<T>(this T[] arr, int startIdx,  T[] subArr)
        {
            if (subArr.Length > arr.Length - startIdx)
                return false;
            for (int i = 0; i < subArr.Length; ++i)
            {
                if (!subArr[i].Equals(arr[startIdx + i]))
                    return false;
            }
            return true;
        }
        public static bool StartsWith<T>(this T[] arr, T[] subArr)
        {
            return arr.SubAt(0, subArr);
        }
        public static bool EndsWith<T>(this T[] arr, T[] subArr)
        {
            if (subArr.Length > arr.Length)
                return false;
            return arr.SubAt(arr.Length - subArr.Length, subArr);
        }
        public static int IndexOf<T>(this T[] arr, int startIdx, T[] subArr)
        {
            for (int i = startIdx; i < arr.Length; ++i)
            {
                if (arr.SubAt(i, subArr))
                    return i;
            }
            return -1;
        }
        public static int IndexOf<T>(this T[] arr, T[] subArr)
        {
            return arr.IndexOf(0, subArr);
        }
        public static int IndexOf<T>(this T[] arr, int startIdx, T t)
        {
            for (int i = startIdx; i < arr.Length; ++i)
            {
                if (t.Equals(arr[i]))
                    return i;
            }
            return -1;
        }
        public static int IndexOf<T>(this T[] arr, T t)
        {
            return arr.IndexOf(0, t);
        }
        public static int IndexOfAny<T>(this T[] arr, int startIdx, T[] anyArr)
        {
            for (int i = startIdx; i < arr.Length; ++i)
            {
                for (int j = 0; j < anyArr.Length; ++j)
                    if (anyArr[j].Equals(arr[i]))
                        return i;
            }
            return -1;
        }
        public static int IndexOfAny<T>(this T[] arr, T[] anyArr)
        {
            return arr.IndexOfAny(0, anyArr);
        }

        public static T[][] Split<T>(this T[] data, T delim, bool skipEmpty = true)
        {
            List<T[]> res = new List<T[]>();
            int sidx = 0;
            for (int i = 0; i < data.Length; ++i)
            {
                if (data[i].Equals(delim))
                {
                    T[] tmp = data.Slice(sidx, i - sidx);
                    if (!skipEmpty || tmp.Length > 0)
                        res.Add(tmp);
                    sidx = i + 1;
                    continue;
                }
            }
            if (sidx < data.Length)
            {
                res.Add(data.Slice(sidx));
            }
            else if (!skipEmpty)
            {
                res.Add(new T[0]);
            }
            return res.ToArray();
        }

        public static string ToString2<T>(this IEnumerable<T> arr)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("[");
            foreach (T t in arr)
            {
                if (t == null)
                    sb.Append("<null>, ");
                else
                    sb.AppendFormat("{0}, ", t);
            }
            if (sb.Length > 1)
                sb.Length -= 2;
            sb.Append("]");
            return sb.ToString();
        }
    }
    public class MyEncodingInfo
    {
        public Func<byte[], string> GetString { get; }
        public Func<string, byte[]> GetBytes { get; }
        public MyEncodingInfo(Func<byte[], string> getString, Func<string, byte[]> getBytes)
        {
            GetString = getString;
            GetBytes = getBytes;
        }
    }
    internal static class MyEncodingUtil
    {
        public static string Bytes2String(byte[] data)
        {
            StringBuilder sb = new StringBuilder();
            foreach (byte b in data)
            {
                if (b >= 32 && b < 127)
                {
                    if (b == (byte)'\\')
                        sb.Append(@"\\");
                    else
                        sb.AppendFormat("{0}", (char)b);
                }
                else
                {
                    sb.AppendFormat(@"\x{0:x2}", b);
                }
            }
            return sb.ToString();
        }
        public static byte[] String2Bytes(string s)
        {
            byte[] res = new byte[s.Length];
            int idx = 0;
            for (int i = 0; i < s.Length; ++i)
            {
                if (s[i] == '\\')
                {
                    ++i;
                    if (s[i] == '\\')
                        res[idx] = (byte)'\\';
                    else if (s[i] == 'x')
                    {
                        ++i;
                        if (!int.TryParse(s.Substring(i, 2), NumberStyles.HexNumber, null, out int v))
                            throw new ArgumentException("s is not byte string format");
                        res[idx] = (byte)v;
                        ++i;
                    }
                    else
                        throw new ArgumentException("s is not byte string format");
                }
                else
                {
                    res[idx] = (byte)s[i];
                }
                ++idx;
            }
            return res.Slice(0, idx);
        }
    }
    public static class BitConvertUtil
    {
        public static Dictionary<string, MyEncodingInfo> EncMap = new Dictionary<string, MyEncodingInfo>()
        {
            ["ascii"] = new MyEncodingInfo(Encoding.ASCII.GetString, Encoding.ASCII.GetBytes), 
            ["utf8"] = new MyEncodingInfo(Encoding.UTF8.GetString, Encoding.UTF8.GetBytes), 
            ["utf-8"] = new MyEncodingInfo(Encoding.UTF8.GetString, Encoding.UTF8.GetBytes), 
            ["utf16"] = new MyEncodingInfo(Encoding.Unicode.GetString, Encoding.Unicode.GetBytes), 
            ["utf-16"] = new MyEncodingInfo(Encoding.Unicode.GetString, Encoding.Unicode.GetBytes), 
            ["utf16be"] = new MyEncodingInfo(Encoding.BigEndianUnicode.GetString, Encoding.BigEndianUnicode.GetBytes), 
            ["utf-16-be"] = new MyEncodingInfo(Encoding.BigEndianUnicode.GetString, Encoding.BigEndianUnicode.GetBytes), 
            ["utf32"] = new MyEncodingInfo(Encoding.UTF32.GetString, Encoding.UTF32.GetBytes), 
            ["utf-32"] = new MyEncodingInfo(Encoding.UTF32.GetString, Encoding.UTF32.GetBytes), 
            ["bytes"] = new MyEncodingInfo(MyEncodingUtil.Bytes2String, MyEncodingUtil.String2Bytes), 
        };

        private static T ToT<T>(this byte[] data, int startIdx, int sz, bool bigEndian, Func<byte[], int, T> conv)
        {
            if (BitConverter.IsLittleEndian == bigEndian)
                Array.Reverse(data, startIdx, sz);
            return conv(data, startIdx);
        }
        public static short ToInt16(this byte[] data, bool bigEndian = true, int startIdx = 0) => data.ToT(startIdx, 2, bigEndian, BitConverter.ToInt16);
        public static int ToInt32(this byte[] data, bool bigEndian = true, int startIdx = 0) => data.ToT(startIdx, 4, bigEndian, BitConverter.ToInt32);
        public static long ToInt64(this byte[] data, bool bigEndian = true, int startIdx = 0) => data.ToT(startIdx, 8, bigEndian, BitConverter.ToInt64);
        public static ushort ToUInt16(this byte[] data, bool bigEndian = true, int startIdx = 0) => (ushort)data.ToInt16(bigEndian, startIdx);
        public static uint ToUInt32(this byte[] data, bool bigEndian = true, int startIdx = 0) => (uint)data.ToInt32(bigEndian, startIdx);
        public static ulong ToUInt64(this byte[] data, bool bigEndian = true, int startIdx = 0) => (ulong)data.ToInt64(bigEndian, startIdx);
        public static string ToString(this byte[] data, string encoding)
        {
            encoding = encoding ?? "utf8";
            return EncMap[encoding].GetString(data);
        }
        public static string Utf8(this byte[] data) => data.ToString("utf8");
        public static string Ascii(this byte[] data) => data.ToString("ascii");
        public static string Bytes(this byte[] data) => data.ToString("bytes");

        private static byte[] GetBytes<T>(this T t, bool bigEndian, Func<T, byte[]> conv)
        {
            byte[] data = conv(t);
            if (BitConverter.IsLittleEndian == bigEndian)
                Array.Reverse(data);
            return data;
        }
        public static byte[] GetBytes(this short n, bool bigEndian = true) => n.GetBytes(bigEndian, BitConverter.GetBytes);
        public static byte[] GetBytes(this int n, bool bigEndian = true) => n.GetBytes(bigEndian, BitConverter.GetBytes);
        public static byte[] GetBytes(this long n, bool bigEndian = true) => n.GetBytes(bigEndian, BitConverter.GetBytes);
        public static byte[] GetBytes(this ushort n, bool bigEndian = true) => n.GetBytes(bigEndian, BitConverter.GetBytes);
        public static byte[] GetBytes(this uint n, bool bigEndian = true) => n.GetBytes(bigEndian, BitConverter.GetBytes);
        public static byte[] GetBytes(this ulong n, bool bigEndian = true) => n.GetBytes(bigEndian, BitConverter.GetBytes);
        public static byte[] GetBytes(this string s, string encoding = null)
        {
            encoding = encoding ?? "utf8";
            return EncMap[encoding].GetBytes(s);
        }
        public static byte[] Utf8(this string s) => s.GetBytes("utf8");
        public static byte[] Ascii(this string s) => s.GetBytes("ascii");
        public static byte[] Bytes(this string s) => s.GetBytes("bytes");
    }

    public class PeerClosedException : SocketException
    {
        public PeerClosedException() : base((int)SocketError.ConnectionReset)
        { }
    }
    public static class SocketUtil
    {
        public static int GetErrorCode(this Socket s)
        {
            return (int)s.GetSocketOption(SocketOptionLevel.Socket, SocketOptionName.Error);
        }
        public static Task<bool> PollAsync(this Socket s, int milliSecondsTimeout, SelectMode selectMode)
        {
            return Task.Run(async () =>
            {
                if (milliSecondsTimeout == 0)
                {
                    if (s.Poll(0, selectMode))
                        return true;
                    if (s.Poll(0, SelectMode.SelectError))
                        throw new SocketException(s.GetErrorCode());
                    return false;
                }

                int elapsedms = 0, pollms = 1;
                while (true)
                {
                    if (s.Poll(pollms * 1000, selectMode))
                        return true;
                    if (s.Poll(0, SelectMode.SelectError))
                        throw new SocketException(s.GetErrorCode());

                    elapsedms += pollms;
                    if (milliSecondsTimeout > 0 && elapsedms >= milliSecondsTimeout)
                        return false;
                    await Task.Yield();
                }
            });
        }
        public static Task<bool> PollReadAsync(this Socket s, int milliSecondsTimeout = -1)
        {
            return PollAsync(s, milliSecondsTimeout, SelectMode.SelectRead);
        }
        public static Task<bool> PollWriteAsync(this Socket s, int milliSecondsTimeout = -1)
        {
            return PollAsync(s, milliSecondsTimeout, SelectMode.SelectWrite);
        }
        public static Task<byte[]> RcvAsync(this Socket s, int sz)
        {
            return Task.Run(async () =>
            {
                byte[] buf = new byte[sz];
                int idx = 0, tmp;
                while (true)
                {
                    await s.PollReadAsync();
                    tmp = s.Receive(buf, idx, sz - idx, SocketFlags.None);
                    if (tmp == 0)
                        throw new PeerClosedException();
                    idx += tmp;
                    if (idx == sz)
                        break;
                }
                return buf;
            });
        }
        public static Task<byte[]> RcvAsync2(this Socket s, int maxsz = 4096, int milliSecondsTimeout = -1)
        {
            return Task.Run(async () =>
            {
                byte[] buf = new byte[maxsz];
                bool canRead = await s.PollReadAsync(milliSecondsTimeout);
                if (!canRead)
                    return Array.Empty<byte>();
                int sz = s.Receive(buf);
                if (sz == 0)
                    throw new PeerClosedException();
                return buf.Slice(0, sz);
            });
        }
        public static Task SndAsync(this Socket s, byte[] data)
        {
            return Task.Run(async () =>
            {
                int idx = 0;
                while (true)
                {
                    await s.PollWriteAsync();
                    idx += s.Send(data, idx, data.Length - idx, SocketFlags.None);
                    if (idx == data.Length)
                        break;
                }
            });
        }
        public static Task SndAsync(this Socket s, params byte[][] datas)
        {
            return Task.Run(async () =>
            {
                Utils.MyBuffer buf = new Utils.MyBuffer(1024);
                foreach (byte[] data in datas)
                    buf.Append(data);
                await s.SndAsync(buf.ToBytes());

            });
        }
        // extension method for string
        public static IPEndPoint Address(this string addr)
        {
            string[] hostport = addr.Split(':');
            if (hostport.Length != 2)
                throw new ArgumentException(string.Format("addr should be 'host[:port]'", addr));
            string host = hostport[0];
            int port = int.Parse(hostport[1]);
            return new IPEndPoint(IPAddress.Parse(host), port);
        }
        public static Socket Listen(this string addr, bool blocking = false)
        {
            Socket s = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            s.Blocking = blocking;
            s.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
            s.Bind(addr.Address());
            s.Listen(100);
            return s;
        }
        public static Task<Socket> AcptAsync(this Socket s)
        {
            return Task.Run(async () =>
            {
                await s.PollReadAsync();
                return s.Accept();
            });
        }
        public static Task<Socket> ConnectAsync(this string addr, bool blocking = false, int secondsTimeout = -1)
        {
            return Task.Run(async () =>
            {
                Socket s = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                s.Blocking = blocking;
                try
                {
                    s.Connect(addr.Address());
                }
                catch (SocketException se)
                {
                    if (se.SocketErrorCode != SocketError.WouldBlock &&
                        se.SocketErrorCode != SocketError.InProgress)
                        throw;
                }
                if (secondsTimeout <= 0)
                    secondsTimeout = -1;
                bool x = await s.PollAsync(secondsTimeout * 1000, SelectMode.SelectWrite);
                if (!x)
                    throw new TimeoutException();
                return s;
            });
        }
    } // end of SocketUtil
} // end of Zhb.ExtensionMethods

namespace Zhb.Utils
{
    public interface ISocketable
    {
        Socket GetSocket();
    }
    public class Socketable : ISocketable, IDisposable
    {
        private Socket sock;
        public Socketable(Socket s) => this.sock = s;
        public Socket GetSocket() => this.sock;
        public void Dispose()
        {
            this.sock.Shutdown(SocketShutdown.Both);
            this.sock.Close();
        }

        public static implicit operator Socketable(Socket s) => new Socketable(s);
        public static explicit operator Socket(Socketable s) => s.sock;
    }
    public class Poller
    {
        public const int POLLIN = 0x01;
        public const int POLLOUT = 0x02;
        public const int POLLERR = 0x04;
        public const int POLLINOUT = POLLIN | POLLOUT;

        private Dictionary<IntPtr, (ISocketable, int)> registedObjs = new Dictionary<IntPtr, (ISocketable, int)>(); 

        public void Clear()
        {
            registedObjs.Clear();
        }
        public Poller Register(ISocketable s, int masks)
        {
            registedObjs[s.GetSocket().Handle] = (s, masks);
            return this;
        }
        public Poller Unregister(ISocketable s)
        {
            registedObjs.Remove(s.GetSocket().Handle);
            return this;
        }
        public Task<List<(ISocketable, int)>> PollAsync(int milliSecondsTimeout = -1)
        {
            if (registedObjs.Count <= 0)
                return Task.FromResult(new List<(ISocketable, int)>());

            return Task.Run(async () =>
            {
                List<Socket> readList = new List<Socket>();
                List<Socket> writeList = new List<Socket>();
                List<Socket> errorList = new List<Socket>();
                MakeCheckList(readList, writeList, errorList);
                if (milliSecondsTimeout == 0)
                {
                    Socket.Select(readList, writeList, errorList, 0);
                    return MakePollResult(readList, writeList, errorList);
                }

                int elapsedms = 0, pollms = 1;
                while (true)
                {
                    Socket.Select(readList, writeList, errorList, pollms * 1000);
                    var res = MakePollResult(readList, writeList, errorList);
                    if (res.Count > 0)
                        return res;
                    elapsedms += pollms;
                    if (milliSecondsTimeout > 0 && elapsedms >= milliSecondsTimeout)
                        return res;
                    await Task.Yield();
                    MakeCheckList(readList, writeList, errorList);
                }
            });
        }
        private List<(ISocketable, int)> MakePollResult(List<Socket> readList, List<Socket> writeList, List<Socket> errorList)
        {
            List<(ISocketable, int)> res = new List<(ISocketable, int)>();
            foreach (Socket s in readList)
                res.Add((registedObjs[s.Handle].Item1, POLLIN));
            foreach (Socket s in writeList)
                res.Add((registedObjs[s.Handle].Item1, POLLOUT));
            foreach (Socket s in errorList)
                res.Add((registedObjs[s.Handle].Item1, POLLERR));
            return res;
        }
        private void MakeCheckList(List<Socket> readList, List<Socket> writeList, List<Socket> errorList)
        {
            foreach (var item in registedObjs)
            {
                var (ss, masks) = item.Value;
                var s = ss.GetSocket();
                if ((masks & POLLIN) == POLLIN)
                    readList.Add(s);
                else if ((masks & POLLOUT) == POLLOUT)
                    writeList.Add(s);
                else if ((masks & POLLERR) == POLLERR)
                    errorList.Add(s);
            }
        }
    }
} // end of Zhb.Utils
