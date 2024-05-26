package com.justaddhippopotamus.ghr.server.types;

import com.justaddhippopotamus.ghr.RESP.RESPBulkString;
import com.justaddhippopotamus.ghr.server.Ccharstar;

import java.io.IOException;
import java.io.OutputStream;
import java.io.StreamCorruptedException;
import java.util.List;

/* The Redis HyperLogLog implementation is based on the following ideas:
 *
 * * The use of a 64 bit hash function as proposed in [1], in order to estimate
 *   cardinalities larger than 10^9, at the cost of just 1 additional bit per
 *   register.
 * * The use of 16384 6-bit registers for a great level of accuracy, using
 *   a total of 12k per key.
 * * The use of the Redis string data type. No new type is introduced.
 * * No attempt is made to compress the data structure as in [1]. Also the
 *   algorithm used is the original HyperLogLog Algorithm as in [2], with
 *   the only difference that a 64 bit hash function is used, so no correction
 *   is performed for values near 2^32 as in [1].
 *
 * [1] Heule, Nunkesser, Hall: HyperLogLog in Practice: Algorithmic
 *     Engineering of a State of The Art Cardinality Estimation Algorithm.
 *
 * [2] P. Flajolet, Éric Fusy, O. Gandouet, and F. Meunier. Hyperloglog: The
 *     analysis of a near-optimal cardinality estimation algorithm.
 *
 *     analysis of a near-optimal cardinality estimation algorithm.
 *
 * Redis uses two representations:
 *
 * 1) A "dense" representation where every entry is represented by
 *    a 6-bit integer.
 * 2) A "sparse" representation using run length compression suitable
 *    for representing HyperLogLogs with many registers set to 0 in
 *    a memory efficient way.
 *
 *
 * HLL header
 * ===
 *
 * Both the dense and sparse representation have a 16 byte header as follows:
 *
 * +------+---+-----+----------+
 * | HYLL | E | N/U | Cardin.  |
 * +------+---+-----+----------+
 *
 * The first 4 bytes are a magic string set to the bytes "HYLL".
 * "E" is one byte encoding, currently set to HLL_DENSE or
 * HLL_SPARSE. N/U are three not used bytes.
 *
 * The "Cardin." field is a 64 bit integer stored in little endian format
 * with the latest cardinality computed that can be reused if the data
 * structure was not modified since the last computation (this is useful
 * because there are high probabilities that HLLADD operations don't
 * modify the actual data structure and hence the approximated cardinality).
 *
 * When the most significant bit in the most significant byte of the cached
 * cardinality is set, it means that the data structure was modified and
 * we can't reuse the cached value that must be recomputed.
 *
 * Dense representation
 * ===
 *
 * The dense representation used by Redis is the following:
 *
 * +--------+--------+--------+------//      //--+
 * |11000000|22221111|33333322|55444444 ....     |
 * +--------+--------+--------+------//      //--+
 *
 * The 6 bits counters are encoded one after the other starting from the
 * LSB to the MSB, and using the next bytes as needed.
 *
 * Sparse representation
 * ===
 *
 * The sparse representation encodes registers using a run length
 * encoding composed of three opcodes, two using one byte, and one using
 * of two bytes. The opcodes are called ZERO, XZERO and VAL.
 *
 * ZERO opcode is represented as 00xxxxxx. The 6-bit integer represented
 * by the six bits 'xxxxxx', plus 1, means that there are N registers set
 * to 0. This opcode can represent from 1 to 64 contiguous registers set
 * to the value of 0.
 *
 * XZERO opcode is represented by two bytes 01xxxxxx yyyyyyyy. The 14-bit
 * integer represented by the bits 'xxxxxx' as most significant bits and
 * 'yyyyyyyy' as least significant bits, plus 1, means that there are N
 * registers set to 0. This opcode can represent from 0 to 16384 contiguous
 * registers set to the value of 0.
 *
 * VAL opcode is represented as 1vvvvvxx. It contains a 5-bit integer
 * representing the value of a register, and a 2-bit integer representing
 * the number of contiguous registers set to that value 'vvvvv'.
 * To obtain the value and run length, the integers vvvvv and xx must be
 * incremented by one. This opcode can represent values from 1 to 32,
 * repeated from 1 to 4 times.
 *
 * The sparse representation can't represent registers with a value greater
 * than 32, however it is very unlikely that we find such a register in an
 * HLL with a cardinality where the sparse representation is still more
 * memory efficient than the dense representation. When this happens the
 * HLL is converted to the dense representation.
 *
 * The sparse representation is purely positional. For example a sparse
 * representation of an empty HLL is just: XZERO:16384.
 *
 * An HLL having only 3 non-zero registers at position 1000, 1020, 1021
 * respectively set to 2, 3, 3, is represented by the following three
 * opcodes:
 *
 * XZERO:1000 (Registers 0-999 are set to 0)
 * VAL:2,1    (1 register set to value 2, that is register 1000)
 * ZERO:19    (Registers 1001-1019 set to 0)
 * VAL:3,2    (2 registers set to value 3, that is registers 1020,1021)
 * XZERO:15362 (Registers 1022-16383 set to 0)
 *
 * In the example the sparse representation used just 7 bytes instead
 * of 12k in order to represent the HLL registers. In general for low
 * cardinality there is a big win in terms of space efficiency, traded
 * with CPU time since the sparse representation is slower to access:
 *
 * The following table shows average cardinality vs bytes used, 100
 * samples per cardinality (when the set was not representable because
 * of registers with too big value, the dense representation size was used
 * as a sample).
 *
 * 100 267
 * 200 485
 * 300 678
 * 400 859
 * 500 1033
 * 600 1205
 * 700 1375
 * 800 1544
 * 900 1713
 * 1000 1882
 * 2000 3480
 * 3000 4879
 * 4000 6089
 * 5000 7138
 * 6000 8042
 * 7000 8823
 * 8000 9500
 * 9000 10088
 * 10000 10591
 *
 * The dense representation uses 12288 bytes, so there is a big win up to
 * a cardinality of ~2000-3000. For bigger cardinalities the constant times
 * involved in updating the sparse representation is not justified by the
 * memory savings. The exact maximum length of the sparse representation
 * when this implementation switches to the dense representation is
 * configured via the define server.hll_sparse_max_bytes.
 */
//Above for reference while I'm writing this :P Going to do dense representation for
//the moment only.
public class RedisHyperLogLog extends RedisString {
    public static final char prefix = 'H';
    //struct hllhdr {
    //    char magic[4];      /* "HYLL" */
    //    uint8_t encoding;   /* HLL_DENSE or HLL_SPARSE. */
    //    uint8_t notused[3]; /* Reserved for future use, must be zero. */
    //    uint8_t card[8];    /* Cached cardinality, little endian. */
    //    uint8_t registers[]; /* Data bytes. */
    //};
    Ccharstar card;
    Ccharstar registers;
    private boolean validCache() {
        return card.or((1<<7),7) != 0;
    }
    private void invalidateCache() {
        card.orAt((1<<7),7);
    }

    @Override
    public synchronized void writeTo(OutputStream os) throws IOException {
        os.write(prefix);
        writeTTL(os);
        RESPBulkString bs = new RESPBulkString(this);
        bs.publishTo(os);
    }

    public static final int HLL_P = 14; /* The greater is P, the smaller the error. */
    public static final int HLL_Q = (64-HLL_P); /* The number of bits of the hash value used for
                                            determining the number of leading zeros. */
    public static final int HLL_REGISTERS = (1<<HLL_P); /* With P=14, 16384 registers. */
    public static final int HLL_P_MASK = (HLL_REGISTERS-1); /* Mask to index register. */
    public static final int HLL_BITS = 6; /* Enough to count up to 63 leading zeroes. */
    public static final int HLL_REGISTER_MAX = ((1<<HLL_BITS)-1);
    public static final int HLL_HDR_SIZE = 17;
    //Note, we're tacking a +1 on here because later on they assume there's an extra byte on the buffer( because a string
    //stored in redis has an implicit null on the end) and they use it in the way they take in particular registers
    public static final int HLL_DENSE_SIZE = (HLL_HDR_SIZE+((HLL_REGISTERS*HLL_BITS+7)/8)) + 1;
    public static final int HLL_DENSE = 0; /* Dense encoding. */
    public static final int HLL_SPARSE = 1; /* Sparse encoding. */
    public static final int HLL_RAW = 255; /* Only used internally, never exposed. */
    public static final int HLL_MAX_ENCODING = 1;

    public RedisHyperLogLog(RedisString input) {
        value = input.value;
        if(value[0] != 'H' || value[1] != 'Y' || value[2] != 'L' || value[3] != 'L')
        {
            throw new RuntimeException("Redis String is not a HyperLogLog");
        }
        card = Ccharstar.on(value, 9);
        registers = Ccharstar.on(value,17);
    }

    public RedisHyperLogLog() {
        value = new byte[HLL_DENSE_SIZE];
        integerValue = 0;
        isInteger = false;
        value[0] = 'H';
        value[1] = 'Y';
        value[2] = 'L';
        value[3] = 'L';
        card = Ccharstar.on(value, 9);
        registers = Ccharstar.on(value,17);
    }

    public int getRegisterDense(int which) {
        int _byte = (which * HLL_BITS) / 8;
        int _fb = (which * HLL_BITS) & 7;
        int _fb8 = 8 - _fb;
        int b0 = registers.get(_byte);
        int b1 = registers.get(_byte+1);
        return ((b0 >>> _fb) | (b1 << _fb8)) & HLL_REGISTER_MAX;
    }
    public void setRegisterDense(int which, int value) {
        int _byte = which * HLL_BITS / 8;
        int _fb = (which * HLL_BITS) & 7;
        int _fb8 = 8 - _fb;
        registers.andAt(~(HLL_REGISTER_MAX<<_fb),_byte);
        registers.orAt(value<<_fb,_byte);
        registers.andAt(~(HLL_REGISTER_MAX>>>_fb8),++_byte);
        registers.orAt(value>>>_fb8,_byte);
    }

    public long murmurmhash64a(Ccharstar key, long seed) {
        final long m = 0xc6a4a7935bd1e995L;
        final int r =  47;
        long h = seed ^ (key.left() * m);
        Ccharstar data = key.dup();
        while(data.left() > 8) {
            long k = data.grab64bitLongLittleEndian();
            k *= m;
            k ^= k >>> r;
            k *= m;
            h ^= k;
            h *= m;
        }
        h ^= data.grabRemaining64bitLongLittleEndian();
        h *= m;
        h ^= h >>> r;
        h *= m;
        h ^= h >>> r;
        return h;
    }

    private int hllPatLength(byte [] element, int [] regp) {
        long hash = murmurmhash64a(Ccharstar.on(element,0),0xadc83b19L);
        int index = (int)hash & HLL_P_MASK;
        hash >>>= HLL_P;
        hash |= 1L << HLL_Q;
        long bit = 1;
        int count = 1;
        while( (hash & bit) == 0 ) {
            ++count; bit<<=1;
        }
        regp[0] = index;
        return count;
    }

    public int hllDenseSet(int index, int count) {
        int regVal = getRegisterDense(index);
        if( count > regVal ) {
            setRegisterDense(index, count);
            invalidateCache();
            return 1;
        }
        return 0;
    }
    public int hllDenseAdd(byte [] element) {
        int [] index = {0};
        int count = hllPatLength(element,index);
        return hllDenseSet(index[0],count);
    }

    public int [] getHistogram() {
        //Note, this is a magic number because if someone overwrites the middle
        //of the string that is based on we could write beyond the # of registers
        //defined as HLL_Q + 2. So like in the redis implementation we tack on
        //enough safe space to deal with it.
        int [] histogram = new int [64];
        for( int i = 0; i < HLL_REGISTERS; ++i ) {
            histogram[getRegisterDense(i)]++;
        }
        return histogram;
    }
    public static int [] getHistogramOfBytes(byte [] uncompressedRegisters) {
        int [] histogram = new int [64];
        for( int i = 0; i < HLL_REGISTERS; ++i ) {
            histogram[uncompressedRegisters[i]]++;
        }
        return histogram;
    }
    /* Helper function sigma as defined in
     * "New cardinality estimation algorithms for HyperLogLog sketches"
     * Otmar Ertl, arXiv:1702.01284 */
    private static double hllSigma(double x) {
        if( x == 1.0d ) return Double.POSITIVE_INFINITY;
        double y = 1.0d;
        double z = x;
        double zPrime = z;
        do {
            x *= x;
            zPrime = z;
            z += x * y;
            y += y;
        } while ( zPrime != z );
        return z;
    }
    /* Helper function tau as defined in
     * "New cardinality estimation algorithms for HyperLogLog sketches"
     * Otmar Ertl, arXiv:1702.01284 */
    private static double hllTau(double x) {
        if (x == 0.0d || x == 1.0d) return 0.0d;
        double zPrime;
        double y = 1.0;
        double z = 1 - x;
        do {
            x = Math.sqrt(x);
            zPrime = z;
            y *= 0.5d;
            z -= Math.pow(1.0d - x, 2.0d)*y;
        } while(zPrime != z);
        return z / 3.0;
    }
    public static double HLL_ALPHA_INF = 0.721347520444481703680d;
    public static long hllCount(int [] histogram) {
        double m = HLL_REGISTERS;
        /* Estimate cardinality from register histogram. See:
         * "New cardinality estimation algorithms for HyperLogLog sketches"
         * Otmar Ertl, arXiv:1702.01284 */
        double z = m * hllTau((m-histogram[HLL_Q+1])/(double)m);
        for (int j = HLL_Q; j >= 1; --j) {
            z += histogram[j];
            z *= 0.5;
        }
        z += m * hllSigma(histogram[0]/(double)m);
        return Math.round(HLL_ALPHA_INF*m*m/z);
    }

    public long hllSelfCount() {
        int [] histogram = getHistogram();
        return hllCount(histogram);
   }

    public synchronized int hllAdd(byte [] element) {
        return hllDenseAdd(element);
    }

    public synchronized int hllAdd(List<RESPBulkString> elements) {
        int modified = 0;
        for(var e : elements) {
            if( hllDenseAdd(e.value) != 0 ) {
                ++modified;
            }
        }
        return modified==0?0:1;
    }

    public synchronized byte [] mergeMax(byte [] maxBytes) {
        if( maxBytes == null ) {
            maxBytes = new byte[HLL_REGISTERS];
        }
        for( int i = 0; i < HLL_REGISTERS; ++i ) {
            var val = getRegisterDense(i);
            var toSet = Math.max(maxBytes[i]&0xFF,val);
            maxBytes[i] = (byte)toSet;
        }
        return maxBytes;
    }

    public synchronized void setFromMax(byte [] maxBytes) {
        for(int i = 0; i < HLL_REGISTERS; ++i ) {
            setRegisterDense(i,maxBytes[i]&0xFF);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(4096);
        sb.append("HyperLogLog: [");
        for( int i = 0; i < HLL_REGISTERS; ++i ) {
            sb.append("0x");
            sb.append(Integer.toHexString(getRegisterDense(i)));
            sb.append(',');
        }
        sb.deleteCharAt(sb.length()-1);
        sb.append(']');
        return sb.toString();
    }
    public String toSparseString() {
        StringBuilder sb = new StringBuilder(4096);
        sb.append("HyperLogLog: [");
        for( int i = 0; i < HLL_REGISTERS; ++i ) {
            int val = getRegisterDense(i);
            if( val != 0 ) {
                sb.append(i);
                sb.append(':');
                sb.append("0x");
                sb.append(Integer.toHexString(val));
                sb.append(',');
            }
        }
        sb.deleteCharAt(sb.length()-1);
        sb.append(']');
        return sb.toString();
    }
}
