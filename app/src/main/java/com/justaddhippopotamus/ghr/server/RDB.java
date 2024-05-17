package com.justaddhippopotamus.ghr.server;

import com.justaddhippopotamus.ghr.server.types.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteOrder;
import java.util.HashMap;

//Functions for doing rdb type stuff...mostly for dump/restore since we are saving out
//in a resp-ish database type thing.
public class RDB {
    public static final ByteOrder bo = ByteOrder.nativeOrder();
    /* Defines related to the dump file format. To store 32 bits lengths for short
     * keys requires a lot of space, so we check the most significant 2 bits of
     * the first byte to interpreter the length:
     *
     * 00|XXXXXX => if the two MSB are 00 the len is the 6 bits of this byte
     * 01|XXXXXX XXXXXXXX =>  01, the len is 14 bits, 6 bits + 8 bits of next byte
     * 10|000000 [32 bit integer] => A full 32 bit len in net byte order will follow
     * 10|000001 [64 bit integer] => A full 64 bit len in net byte order will follow
     * 11|OBKIND this means: specially encoded object will follow. The six bits
     *           number specify the kind of object that follows.
     *           See the RDB_ENC_* defines.
     *
     * Lengths up to 63 are stored using a single byte, most DB keys, and may
     * values, will fit inside. */
    public static final int RDB_VERSION = 10;
    public static final int RDB_6BITLEN = 0;
    public static final int RDB_14BITLEN = 1;
    public static final int RDB_32BITLEN = 0x80;
    public static final int RDB_64BITLEN = 0x81;
    public static final int RDB_ENCVAL = 3;
    public static final long RDB_ENCERR = 0xFFFFFFFFFFFFFFFFL;
    public static final int RDB_ENC_INT8 = 0;        /* 8 bit signed integer */
    public static final int RDB_ENC_INT16 = 1;       /* 16 bit signed integer */
    public static final int RDB_ENC_INT32 = 2;      /* 32 bit signed integer */
    public static final int RDB_ENC_LZF = 3;         /* string compressed with FASTLZ */

    /* Map object types to RDB object types. Macros starting with OBJ_ are for
     * memory storage and may change. Instead RDB types must be fixed because
     * we store them on disk. */
    public static final int RDB_TYPE_STRING  = 0;
    public static final int RDB_TYPE_LIST    = 1;
    public static final int RDB_TYPE_SET     = 2;
    public static final int RDB_TYPE_ZSET    = 3;
    public static final int RDB_TYPE_HASH    = 4;
    public static final int RDB_TYPE_ZSET_2  = 5;/* ZSET version 2 with doubles stored in binary. */
    public static final int RDB_TYPE_MODULE  = 6;
    public static final int RDB_TYPE_MODULE_2  = 7; /* Module value with annotations for parsing without
                                                        the generating module being loaded. */
    /* NOTE: WHEN ADDING NEW RDB TYPE, UPDATE rdbIsObjectType() BELOW */
    /* Object types for encoded objects. */
    public static final int RDB_TYPE_HASH_ZIPMAP     = 9;
    public static final int RDB_TYPE_LIST_ZIPLIST   = 10;
    public static final int RDB_TYPE_SET_INTSET    = 11;
    public static final int RDB_TYPE_ZSET_ZIPLIST  = 12;
    public static final int RDB_TYPE_HASH_ZIPLIST  = 13;
    public static final int RDB_TYPE_LIST_QUICKLIST = 14;
    public static final int RDB_TYPE_STREAM_LISTPACKS = 15;
    public static final int RDB_TYPE_HASH_LISTPACK = 16;
    public static final int RDB_TYPE_ZSET_LISTPACK = 17;
    public static final int RDB_TYPE_LIST_QUICKLIST_2   = 18;
    public static final int RDB_TYPE_STREAM_LISTPACKS_2 = 19;
    public static boolean rdbIsObjectType(long v) {
        return (( v >= 0 && v <= 7) || (v >= 9 && v <= 19));
    }

    /* Special RDB opcodes (saved/loaded with rdbSaveType/rdbLoadType). */
    public static final int RDB_OPCODE_FUNCTION2=  245;   /* function library data */
    public static final int RDB_OPCODE_FUNCTION =  246;   /* old function library data for 7.0 rc1 and rc2 */
    public static final int RDB_OPCODE_MODULE_AUX = 247;   /* Module auxiliary data. */
    public static final int RDB_OPCODE_IDLE      = 248;   /* LRU idle time. */
    public static final int RDB_OPCODE_FREQ      = 249;   /* LFU frequency. */
    public static final int RDB_OPCODE_AUX       = 250;   /* RDB aux field. */
    public static final int RDB_OPCODE_RESIZEDB  = 251;   /* Hash table resize hint. */
    public static final int RDB_OPCODE_EXPIRETIME_MS = 252;    /* Expire time in milliseconds. */
    public static final int RDB_OPCODE_EXPIRETIME = 253;       /* Old expire time in seconds. */
    public static final int RDB_OPCODE_SELECTDB   = 254;   /* DB number of the following keys. */
    public static final int RDB_OPCODE_EOF        = 255;   /* End of the RDB file. */

            /* Module serialized values sub opcodes */
    public static final int RDB_MODULE_OPCODE_EOF  = 0;   /* End of module value. */
    public static final int RDB_MODULE_OPCODE_SINT  = 1;   /* Signed integer. */
    public static final int RDB_MODULE_OPCODE_UINT  = 2;   /* Unsigned integer. */
    public static final int RDB_MODULE_OPCODE_FLOAT = 3;   /* Float. */
    public static final int RDB_MODULE_OPCODE_DOUBLE = 4;  /* Double. */
    public static final int RDB_MODULE_OPCODE_STRING = 5;  /* String. */

    //Save and load stuff from InputStreams() where we can mangle our
    //outputs to match what is supposed to be saved into the rdb format.

    //Files are stored in little endian. First bytes in are least significant.
    private static long bytesToLong( Ccharstar bytes, int size, boolean signed ) {
        //Most significant bit in last byte is 'negative' if we're signed...
        //sign extend the int we read out to the end of the accumulator.
        if( bytes.left() < size ) {
            throw new RuntimeException("Failed to read enough bytes to make an int.");
        }
        long accumulator = signed&&bytes.get(size-1)>127?-1:0;
        for( int i = 0; i < size; ++i ) {
            accumulator = accumulator << 8;
            accumulator += bytes.get(size - i - 1);
        }
        return accumulator;
    }
    private static long bytesToLongBigE( Ccharstar bytes, int size, boolean signed ) {
        //Most significant bit in first byte is 'negative' if we're signed...
        //sign extend the int we read out to the end of the accumulator.
        if( bytes.left() < size ) {
            throw new RuntimeException("Failed to read enough bytes to make an int.");
        }
        long accumulator = signed&&bytes.get(0)>127?-1:0;
        for( int i = 0; i < size; ++i ) {
            accumulator = accumulator << 8;
            accumulator += bytes.get(i);
        }
        return accumulator;
    }
    public static int saveType(int type, OutputStream os) throws IOException {
        os.write(type);
        return 1;
    }

    public static int loadType(InputStream is) throws IOException {
        return is.read();
    }

    public static RedisType loadObject(int type, InputStream is) throws IOException {
        switch( type ) {
            case RDB_TYPE_STRING:
                return new RedisString(loadString(is));
            case RDB_TYPE_LIST:
                long len = loadLength(is,null);
                RedisList returnList = new RedisList();
                while(len-->0) {
                    returnList.rpush(loadJavaString(is));
                }
                return returnList;
            case RDB_TYPE_SET:
                len = loadLength(is,null);
                RedisSet returnSet = new RedisSet();
                while(len-->0) {
                    returnSet.add(loadJavaString(is));
                }
                return returnSet;
            case RDB_TYPE_ZSET:
            case RDB_TYPE_ZSET_2:
                len = loadLength(is,null);
                HashMap<String,Double> inputMap = new HashMap<>((int)len);
                while(len-->0) {
                    if( type == RDB_TYPE_ZSET_2)
                        inputMap.put(loadJavaString(is),loadBinaryDouble(is));
                    else
                        inputMap.put(loadJavaString(is),loadDouble(is));
                }
                return new RedisSortedSet(inputMap);
            case RDB_TYPE_HASH:
                len = loadLength(is,null);
                RedisHash returnHash = new RedisHash();
                while(len-->0){
                    returnHash.value.put(loadJavaString(is),loadRedisString(is));
                }

            case RDB_TYPE_LIST_QUICKLIST:
            case RDB_TYPE_LIST_QUICKLIST_2:
            case RDB_TYPE_HASH_ZIPMAP:
            case RDB_TYPE_LIST_ZIPLIST:
            case RDB_TYPE_SET_INTSET:
            case RDB_TYPE_ZSET_ZIPLIST:
            case RDB_TYPE_ZSET_LISTPACK:
            case RDB_TYPE_HASH_ZIPLIST:
            case RDB_TYPE_HASH_LISTPACK:
            case RDB_TYPE_STREAM_LISTPACKS:
            case RDB_TYPE_STREAM_LISTPACKS_2:
            case RDB_TYPE_MODULE:
            case RDB_TYPE_MODULE_2:
            default:
                throw new RuntimeException("Dude...bad redis type in a rdb.");

        }
    }

    public static void saveObject(RedisType t, OutputStream s) {

    }

    private static double loadDouble(InputStream is) throws IOException {
        byte [] buf = new byte[255];
        int a = is.read();
        switch(a) {
            case 255:
                return Double.NEGATIVE_INFINITY;
            case 256:
                return Double.POSITIVE_INFINITY;
            case 253:
                return Double.NaN;
            default:
                try {
                    is.read(buf, 0, a);
                    return Double.parseDouble(new String(buf, Server.CHARSET));
                } catch (NumberFormatException e) {
                    throw new RuntimeException("Cannot parse a double.");
                }
        }
    }

    private static double loadBinaryDouble(InputStream is) throws IOException {
        Ccharstar p = loadBytes(is,8);
        if(bo == ByteOrder.BIG_ENDIAN) {
            //Swap the bytes.
            p.endianSwap(8);
        }
        return p.toDouble();
    }

    private static byte [] loadInteger(InputStream is, int encodeType) throws IOException {
        long integerValue;
        Ccharstar p;
        switch( encodeType ) {
            case RDB_ENC_INT8:
                p = loadBytes(is,1);
                integerValue = p.get(0);
                break;
            case RDB_ENC_INT16:
                p = loadBytes(is,2);
                integerValue = p.get() | p.getl()<<8;
                break;
            case RDB_ENC_INT32:
                p = loadBytes(is,4);
                integerValue = p.get() | p.getl()<<8 | p.getl()<<16 + p.getl()<<24;
            default:
                throw new RuntimeException("Bad integer encoding.");
        }
        return String.valueOf(integerValue).getBytes(Server.CHARSET);
    }

    private static String loadJavaString(InputStream is) throws IOException {
        return new String(loadString(is),Server.CHARSET);
    }
    private static RedisString loadRedisString(InputStream is) throws IOException {
        return new RedisString(loadString(is));
    }
    private static byte [] loadString(InputStream is) throws IOException {
        boolean [] encoded = {false};
        int len = (int)loadLength(is,encoded);

        if( encoded[0] ) {
            switch(len) {
                case RDB_ENC_INT8:
                case RDB_ENC_INT16:
                case RDB_ENC_INT32:
                    //Load integer object. But we're going to return the bytes;
                    return loadInteger(is,len);
                case RDB_ENC_LZF:
                    //Load compressed string
                    return loadCompressedBytes(is);
                default:
                    throw new RuntimeException("Bad RDB");
            }
        }
        byte [] returnValue = new byte[(int)len];
        if( len != is.readNBytes(returnValue,0,(int)len) ) {
            throw new RuntimeException("Failed to read n bytes reading a string from RDB");
        }
        return returnValue;
    }

    private static byte [] loadCompressedBytes(InputStream is) throws IOException {
        long compressedLength = loadLength(is,null);
        long uncompressedLength = loadLength(is,null);
        var in = loadBytes(is,compressedLength);
        var out = Ccharstar.grabNew(uncompressedLength);
        var ref = Ccharstar.getNull();

        while( in.left() > 0 ) {
            int ctrl = in.get();//get moves the in pointer...
            if( ctrl < (1<<5) ) {
                ++ctrl;
                in.copyTo(out,ctrl);//moves both pointers
            } else {
                int len = ctrl >> 5;
                ref.set(out,((ctrl & 0x1f) << 8) - 1);
                if( len == 7 ) {
                    len += in.get();
                }
                ref.add(-in.get());
                ref.copyTo(out,len+2);
            }
        }
        return out.buff();
    }

    //Small and temporary
    private static Ccharstar loadBytes(InputStream is, long nBytes) throws IOException {
        var p = Ccharstar.grabOne((int)nBytes);
        if( nBytes != is.readNBytes(p.buff(),0,(int)nBytes) ) throw new RuntimeException("Failed to read");
        return p;
    }

    //Reads a 4 byte time from older rdb types. Stored as little Endian.
    private static long loadTime(InputStream is) throws IOException {
        return bytesToLong(loadBytes(is,4),4,false) * 1000l;
    }

    //Millisecond stored as an int 64 Big Endian
    private static long loadTimeMS(InputStream is) throws IOException {
        return bytesToLongBigE(loadBytes(is,8),8,false);
    }

    private static long loadLength(InputStream is, boolean [] encoded) throws IOException {
        var p = loadBytes(is,1);
        int what = p.get(0);
        int type = (what & 0xC0) >> 6;
        if( encoded != null ) encoded[0] = false;
        if( type == RDB_ENCVAL ) { if(encoded!=null) encoded[0] = true; return what&0x3F;}
        if( type == RDB_6BITLEN ) return what&0x3F;
        if( type == RDB_14BITLEN ) {
            var p2 = loadBytes(is,1);
            return ((what&0x3F)<<8)|p2.get(0);
        }
        if( what == RDB_32BITLEN ) {
            //Stored as big endian
            var p2 = loadBytes(is,4);
            return bytesToLongBigE(p2,4,false);
        }
        if( what == RDB_64BITLEN ) {
            var p2 = loadBytes(is,8);
            return bytesToLongBigE(p2,8,false);
        }
        throw new RuntimeException("RDB error, bad length value.");

    }

    public static int encodeInteger(long value, Ccharstar buffer ) throws IOException {
        if( value > -(1<<7) && value <= (1<<7)-1 ) {
            buffer.put(0,(((RDB_ENCVAL<<6)|RDB_ENC_INT8)));
            buffer.put(1,value);
            return 2;
        } else if (value >= -(1<<15) && value <= (1<<15)-1) {
            buffer.put(0,(RDB_ENCVAL<<6)|RDB_ENC_INT16);
            buffer.put(1,value&0xFF);
            buffer.put(2,(value>>8)&0xFF);
            return 3;
        } else if (value >= -(1L<<31) && value <= (1L<<31)-1) {
            buffer.put(0,(RDB_ENCVAL<<6)|RDB_ENC_INT32);
            buffer.put(1,value&0xFF);
            buffer.put(2,(value>>8)&0xFF);
            buffer.put(3,(value>>16)&0xFF);
            buffer.put(4,(value>>24)&0xFF);
            return 5;
        }
        return 0;
    }
}
