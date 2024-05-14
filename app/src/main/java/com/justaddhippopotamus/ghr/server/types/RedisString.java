package com.justaddhippopotamus.ghr.server.types;

import com.justaddhippopotamus.ghr.RESP.IRESP;
import com.justaddhippopotamus.ghr.RESP.RESPBulkString;
import com.justaddhippopotamus.ghr.server.Server;
import com.justaddhippopotamus.ghr.server.Utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Locale;

public class RedisString extends RedisType {
    public static final char prefix = '$';
    byte[] value;
    boolean isInteger;
    long integerValue;

    public static boolean isStringNumeric(final String str) {
        if (str == null || str.length() == 0)
            return false;
        final int length = str.length();
        for (int i = 0; i < length; ++i) {
            if (!Character.isDigit(str.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    public RedisString(InputStream is) throws IOException {
        readFrom(is);
    }

    public RedisString(String string) {
        set(string);
    }

    public RedisString() {
        value = new byte[0];
        isInteger = false;
        integerValue = 0;
    }

    public static final int BITS_PER_BYTE = 8;

    private RedisString ensureBitCapacity(int numBits) {
        int currentCapacity = value.length * Byte.BYTES * BITS_PER_BYTE;
        if (currentCapacity < numBits) {
            ensureByteCapacity((numBits / 8) + 1);
        }
        return this;
    }

    public synchronized RedisString ensureByteCapacity(int numBytes) {
        int currentByteSize = value.length * Byte.BYTES;
        if (numBytes > currentByteSize) {
            byte[] newBuffer = new byte[numBytes];
            System.arraycopy(value, 0, newBuffer, 0, value.length);
            value = newBuffer;
        }
        return this;
    }

    public static final int[] BIT_SELECT = {
            0b10000000,
            0b01000000,
            0b00100000,
            0b00010000,
            0b00001000,
            0b00000100,
            0b00000010,
            0b00000001
    };
    public static final int[] FIRST_BYTE_MASK = {
            0b11111111,
            0b01111111,
            0b00111111,
            0b00011111,
            0b00001111,
            0b00000111,
            0b00000011,
            0b00000001
    };
    public static final int[] LAST_BYTE_MASK = {
            0b10000000,
            0b11000000,
            0b11100000,
            0b11110000,
            0b11111000,
            0b11111100,
            0b11111110,
            0b11111111
    };

    public synchronized int bitcount(int start, int end, boolean BIT_INDEXING) {
        int startBit, endBit;
        int startByte, endByte;
        if (BIT_INDEXING) {
            int lengthInBits = value.length * BITS_PER_BYTE;
            startBit = realIndex(start, lengthInBits);
            endBit = realIndex(end, lengthInBits);
            startByte = startBit / 8;
            endByte = endBit / 8;
        } else {
            int lengthInBytes = value.length;
            startBit = 0;
            endBit = 7;
            startByte = realIndex(start, lengthInBytes);
            endByte = realIndex(end, lengthInBytes);
        }

        int bitCount = 0;
        long cumulativeLong = value[startByte] & FIRST_BYTE_MASK[startBit % 8];
        for (int i = startByte + 1; i < endByte; ++i) {
            cumulativeLong = (cumulativeLong << 8) + value[i];
            if (i % Long.BYTES == 0) {
                bitCount += Long.bitCount(cumulativeLong);
                cumulativeLong = 0;
            }
        }
        if (startByte == endByte)
            cumulativeLong = cumulativeLong & LAST_BYTE_MASK[endBit % 8];
        else
            cumulativeLong = (cumulativeLong << 8) + (value[endByte] & LAST_BYTE_MASK[endBit % 8]);
        bitCount += Long.bitCount(cumulativeLong);
        return bitCount;
    }

    public synchronized int setbit(int offset, int newBit) {
        ensureBitCapacity(offset);
        int whichByte = offset / BITS_PER_BYTE;
        int bitMask = BIT_SELECT[offset % BITS_PER_BYTE];
        int oldValue = (value[whichByte] & bitMask) != 0 ? 1 : 0;
        if (oldValue != newBit) {
            value[whichByte] ^= bitMask;
        }
        return oldValue;
    }

    public synchronized int bitpos(int bit, int start, int end, boolean BIT_INDEXING) {
        int startBit, endBit;
        int startByte, endByte;
        if (BIT_INDEXING) {
            int lengthInBits = value.length * BITS_PER_BYTE;
            startBit = realIndex(start, lengthInBits);
            endBit = realIndex(end, lengthInBits);
            startByte = startBit / 8;
            endByte = endBit / 8;
        } else {
            int lengthInBytes = value.length;
            startBit = 0;
            endBit = 7;
            startByte = realIndex(start, lengthInBytes);
            endByte = realIndex(end, lengthInBytes);
        }
        if (startByte >= value.length)
            return -1;
        if (startByte < 0)
            return -1;
        if (startByte > endByte)
            return -1;
        final int xorValue = bit == 0 ? 0xFFFFFFFF : 0;
        int firstByteMask = FIRST_BYTE_MASK[startBit % 8] & 0xFF;
        int lastByteMask = LAST_BYTE_MASK[endBit % 8] & 0xFF;
        final int fullIntMask = bit == 0 ? 0xFFFFFF00 : 0;
        int currentByteValue = value[startByte] & firstByteMask | (~firstByteMask);
        for (int currentByte = startByte; currentByte <= endByte; ++currentByte) {
            currentByteValue = (value[currentByte] & 0xFF) | fullIntMask;
            if (currentByte == startByte) {
                currentByteValue = currentByteValue & firstByteMask | fullIntMask;
                if (bit == 0) currentByteValue |= ~firstByteMask;
            }
            if (currentByte == endByte) {
                currentByteValue = currentByteValue & lastByteMask | fullIntMask;
                if (bit == 0) currentByteValue |= ~lastByteMask;
            }
            currentByteValue ^= xorValue;
            if (currentByteValue != 0) {
                //We found our 'set' bit. Cool.
                int setBit = 8;
                do {
                    setBit--;
                    currentByteValue = currentByteValue >> 1;
                } while (currentByteValue != 0);
                return currentByte * BITS_PER_BYTE + setBit;
            }
        }
        return -1;
    }

    //Note, we're slicing off any overflow on the top end that doesn't
    //fit without saying anything
    public synchronized void setLongValue(long longValue, int offset, int length) {
        ensureBitCapacity(offset + length);
        int firstByte = offset / 8;
        int firstBits = offset % 8;
        int lastByte = (offset + length - 1) / 8;
        int lastBits = (offset + length - 1) % 8;
        int firstByteMask = FIRST_BYTE_MASK[firstBits % 8] & 0xFF;
        int lastByteMask = LAST_BYTE_MASK[lastBits % 8] & 0xFF;

        long accumulator = longValue;

        if (lastBits < 7) {
            accumulator = accumulator << 7 - lastBits;
        }
        for (int i = lastByte; i >= firstByte; --i) {
            long currentValue = accumulator & 0xFF;
            long mask = 0;
            if (i == lastByte) {
                mask = ~LAST_BYTE_MASK[lastBits] & 0xFF;
            }
            if (i == firstByte) {
                currentValue = currentValue & FIRST_BYTE_MASK[firstBits];
                mask |= ~FIRST_BYTE_MASK[firstBits] & 0xFF;
            }
            value[i] = (byte) (value[i] & mask | currentValue);
            accumulator = accumulator >> 8;
        }
    }

    public synchronized long getLongValue(int offset, int length, boolean signed) {
        int firstByte = offset / 8;
        int firstBits = offset % 8;
        int lastByte = (offset + length - 1) / 8;
        int lastBits = (offset + length - 1) % 8;
        if (firstByte >= value.length)
            return 0;
        long accumulator = 0;
        boolean negative = false;
        for (int i = firstByte; i <= lastByte; ++i) {
            long currentByte = value[i] & 0xFF;
            if (i == firstByte) {
                accumulator = currentByte & FIRST_BYTE_MASK[firstBits];
                if (signed && (accumulator & BIT_SELECT[firstBits]) != 0) {
                    //We're negative,extend the ones all the way to the left.
                    negative = true;
                    accumulator |= 0xFFFFFF00;
                    accumulator |= LAST_BYTE_MASK[firstBits];
                }
            } else {
                accumulator = (accumulator << 8) + currentByte;
            }
            if (i == lastByte) {
                accumulator = accumulator & (0xFFFFFF00 | LAST_BYTE_MASK[lastBits]);
            }
        }
        if (lastBits < 7) {
            accumulator = accumulator >> (7 - lastBits);
            if (negative) {
                accumulator |= ((long) LAST_BYTE_MASK[lastBits] << 24);
            }
        }
        return accumulator;
    }

    public synchronized int getbit(int offset) {
        int whichByte = offset / BITS_PER_BYTE;
        int bitMask = BIT_SELECT[offset % BITS_PER_BYTE];
        return (value[whichByte] & bitMask) != 0 ? 1 : 0;
    }

    private long append(byte [] newBytes) {
        byte[] newBuffer = new byte[newBytes.length + value.length];
        System.arraycopy(value, 0, newBuffer, 0, value.length);
        System.arraycopy(newBytes, 0, newBuffer, value.length, newBytes.length);
        value = newBuffer;
        return newBuffer.length;
    }

    public synchronized long append(String s) {
        return append(s.getBytes(Server.CHARSET));
    }
    public synchronized long append(RESPBulkString s) {
        return append(s.value);
    }

    private void setFromBulkString(RESPBulkString bulkString) {
        if( bulkString.isNumeric() ) {
            set(bulkString.toString());
        } else {
            isInteger = false;
            integerValue = 0;
            value = bulkString.value;
        }}

    public RedisString(long integerValue) {
        isInteger = true;
        this.integerValue = integerValue;
        value = null;
    }

    public static RedisString redisStringOfLength(int length) {
        RedisString rs = new RedisString();
        rs.value = new byte[length];
        rs.isInteger = false;
        rs.integerValue = 0;
        return rs;
    }

    public static RedisString redisStringLength(int size) {
        return new RedisString().ensureByteCapacity(size);
    }

    public synchronized long increment(long by) {
        makeIntegerOutOfValue();
        integerValue += by;
        return integerValue;
    }

    public synchronized RedisString incrementFloat(String by) {
        try {
            double f = Utils.stringToDoubleRedisStyle(by);
            double mine = Utils.stringToDoubleRedisStyle(this.toString());
            set(Utils.doubleToStringRedisStyle(mine+f));
        } catch (NumberFormatException e) {
            throw new RuntimeException("BADARG Cannot parse float");
        }
        return this;
    }

    public synchronized long decrement(long by) {
        makeIntegerOutOfValue();
        integerValue -= by;
        return integerValue;
    }
    public RedisString(RESPBulkString bulkString) {
       setFromBulkString(bulkString);
    }
    public RedisString(byte [] bytes) {
        isInteger = false;
        integerValue = 0;
        value = bytes;
    }
    public synchronized void set(String newValue) {
        isInteger = false;
        integerValue = 0;
        value = newValue.getBytes(Server.CHARSET);
    }
    @Override
    public String toString() {
        if( isInteger ) {
            return String.valueOf(integerValue);
        } else {
            if( value == null ) {
                return null;
            }
            return new String(value,Server.CHARSET);
        }
    }

    @Override
    public synchronized void writeTo(OutputStream os) throws IOException {
        os.write(prefix);
        writeTTL(os);
        RESPBulkString bs = new RESPBulkString(this);
        bs.publishTo(os);
    }

    @Override
    public synchronized void readFrom(InputStream is) throws IOException {
        readTTL(is);
        RESPBulkString bs = RESPBulkString.readFull(is);
        if( bs != null ) {
            setFromBulkString(bs);
        }
    }

    @Override
    public synchronized IRESP wireType(IRESP.RESPVersion v) {
        return new RESPBulkString(this);
    }

    private long makeValueOutOfIntegerIfRequired()  {
        if( isInteger ) {
            value = String.valueOf(integerValue).getBytes(Server.CHARSET);
            isInteger = false;
        }
        return integerValue;
    }

    private long makeIntegerOutOfValue() {
        if( !isInteger ) {
            String valString = new String(value, Server.CHARSET);
            try {
                integerValue = Long.parseLong(valString);
                isInteger = true;
            } catch (NumberFormatException e) {
                throw new RuntimeException("Could not make a number out of " + valString);
            }
        }
        return integerValue;
    }

    private void makeIntegerAgain(long value) {
        isInteger = true;
        integerValue = value;
    }

    public int length() {
        makeValueOutOfIntegerIfRequired();
        return value.length;
    }

    public boolean isNumeric() {
        return isInteger;
    }

    public byte [] getBytes() {
        return value;
    }

    @Override
    public void copyFrom(RedisType other) {
        super.copyFrom(other);
        RedisString rs = (RedisString)other;
        value = new byte [rs.value.length];
        System.arraycopy(rs.value,0, value, 0, rs.value.length);
    }

    public RedisString(RedisString other) {
        copyFrom(other);
    }

    public synchronized int setrange(int offset, RedisString append) {
        makeValueOutOfIntegerIfRequired();
        if (append.length() + offset > value.length) {
            byte[] newValue = new byte[append.length() + offset];
            System.arraycopy(value, 0, newValue, 0, value.length);
            value = newValue;
        }
        System.arraycopy(append.value, 0, value, offset, append.value.length);
        return value.length;
    }

    public synchronized RESPBulkString range(int start, int end) {
        boolean wasInteger = isInteger;
        long oldIntValue = makeValueOutOfIntegerIfRequired();
        int len = value.length;
        int realStart = realIndex(start,len);
        int realEnd = realIndex(end,len);
        ++realEnd;
        if( realStart >= realEnd )
            return new RESPBulkString("");
        if( realStart < 0 )
            realStart = 0;
        if( realEnd > len )
            realEnd = len;
        int count = realEnd - realStart;
        RESPBulkString returnValue = new RESPBulkString(value,realStart,count);
        if( wasInteger ) makeIntegerAgain(oldIntValue);
        return returnValue;
    }


    public void setBitOp(RedisString other) {
        System.arraycopy(other.value,0,value,0,other.value.length);
    }

    public void and(RedisString other) {
        if( other == null ) {
            //Special case, set everything to 0.
            value = new byte [value.length];
        } else {
            int minLen = Math.min(value.length,other.length());
            for( int i = 0; i < minLen; ++i ) {
                value[i] = (byte)(value[i] & other.value[i]);
            }
        }
    }

    public void or(RedisString other) {
        if( other == null ) {
            //Do nothing...
        } else {
            int minLen = Math.min(value.length,other.length());
            for( int i = 0; i < minLen; ++i ) {
                value[i] = (byte) (value[i] | other.value[i]);
            }
        }
    }

    public void xor(RedisString other) {
        if( other == null ) {
            //xoring zeros produces the same output.
        } else {
            int minLen = Math.min(value.length,other.length());
            for( int i = 0; i < minLen; ++i ) {
                value[i] = (byte) (value[i] ^ other.value[i]);
            }
        }
    }

    public boolean valuesSameUpToMinLength(RedisString other) {
        int minLen = Math.min(value.length,other.length());
        for( int i = 0; i < minLen; ++i ) {
            if( value[i] != other.value[i] ) return false;
        }
        return true;
    }

    public synchronized RedisString not() {
        int len = value.length;
        RedisString returnValue = redisStringLength(len);
        for( int i = 0; i < len; ++i ) {
            returnValue.value[i] = (byte)~value[i];
        }
        return returnValue;

    }
    @Override
    public <T extends RedisType> T copy(Class<T> type) {
        RedisString returnValue = new RedisString(this);
        return (T)returnValue;
    }

    public static enum BitFieldOverflowMode {
        WRAP,
        SAT,
        FAIL
    }

    public static BitFieldOverflowMode modeForString(String mode) {
        switch(mode) {
            case "WRAP":
                return BitFieldOverflowMode.WRAP;
            case "SAT":
                return BitFieldOverflowMode.SAT;
            case "FAIL":
                return BitFieldOverflowMode.FAIL;
            default:
                throw new RuntimeException("Unknown overflow mode in BITFIELD");
        }
    }
    public abstract static class BitFieldOp {
        int numbits;
        boolean signed;
        int offset;
        boolean ranked;
        BitFieldOverflowMode mode;
        public abstract Long operateOn(RedisString rs);

        //In pairs, if x greater than the first one or less than the second
        //new value will over/under flow for signed integers for each
        //Index is in 'numbits'
        protected static final long [] overUnderFlowSigned = {
                0L,0L,
                0L,-1L,
                1L,-2L,
                3L,-4L,
                7L,-8L,
                15L,-16L,
                31L,-32L,
                63L,-64L,
                127L,-128L,
                255L,-256L,
                511L,-512L,
                1023L,-1024L,
                2047L,-2048L,
                4095L,-4096L,
                8191L,-8192L,
                16383L,-16384L,
                32767L,-32768L,
                65535L,-65536L,
                131071L,-131072L,
                262143L,-262144L,
                524287L,-524288L,
                1048575L,-1048576L,
                2097151L,-2097152L,
                4194303L,-4194304L,
                8388607L,-8388608L,
                16777215L,-16777216L,
                33554431L,-33554432L,
                67108863L,-67108864L,
                134217727L,-134217728L,
                268435455L,-268435456L,
                536870911L,-536870912L,
                1073741823L,-1073741824L,
                2147483647L,-2147483648L,
                4294967295L,-4294967296L,
                8589934591L,-8589934592L,
                17179869183L,-17179869184L,
                34359738367L,-34359738368L,
                68719476735L,-68719476736L,
                137438953471L,-137438953472L,
                274877906943L,-274877906944L,
                549755813887L,-549755813888L,
                1099511627775L,-1099511627776L,
                2199023255551L,-2199023255552L,
                4398046511103L,-4398046511104L,
                8796093022207L,-8796093022208L,
                17592186044415L,-17592186044416L,
                35184372088831L,-35184372088832L,
                70368744177663L,-70368744177664L,
                140737488355327L,-140737488355328L,
                281474976710655L,-281474976710656L,
                562949953421311L,-562949953421312L,
                1125899906842623L,-1125899906842624L,
                2251799813685247L,-2251799813685248L,
                4503599627370495L,-4503599627370496L,
                9007199254740991L,-9007199254740992L,
                18014398509481983L,-18014398509481984L,
                36028797018963967L,-36028797018963968L,
                72057594037927935L,-72057594037927936L,
                144115188075855871L,-144115188075855872L,
                288230376151711743L,-288230376151711744L,
                576460752303423487L,-576460752303423488L,
                1152921504606846975L,-1152921504606846976L,
                2305843009213693951L,-2305843009213693952L,
                4611686018427387903L,-4611686018427387904L,
                9223372036854775807L,-9223372036854775808L,
                0xFFFFFFFFFFFFFFFFL,0
        };

        protected boolean overflow(long value) {
            if( signed ) {
                return ( value > overUnderFlowSigned[numbits*2] ||
                         value < overUnderFlowSigned[numbits*2+1] );
            } else {
                return value > overUnderFlowSigned[(numbits+1)*2];
            }
        }

        protected long overflowValue(long value) {
            if( signed ) {
                if (value < 0) {
                    switch (mode) {
                        case SAT:
                            return overUnderFlowSigned[numbits * 2 + 1];
                        case WRAP:
                            long nVal = value;
                            long decr = overUnderFlowSigned[(numbits+1) * 2 + 1];
                            do nVal -= decr; while( overflow(nVal) );
                            return nVal;
                    }
                } else {
                    switch (mode) {
                        case SAT:
                            return overUnderFlowSigned[numbits * 2];
                        case WRAP:
                            long nVal = value;
                            long incr = overUnderFlowSigned[(numbits+1) * 2 + 1];
                            do nVal += incr; while( overflow(nVal) );
                            return nVal;
                    }
                }
            } else {
                switch( mode ) {
                    case SAT:
                        return overUnderFlowSigned[(numbits+1) * 2];
                    case WRAP:
                        return value + overUnderFlowSigned[(numbits+1) * 2 + 1];
                }
            }
            return value;
        }

        protected int realIndex() {
            return offset * (ranked?numbits:1);
        }
        protected boolean processOffset( String offsetString ) {
            int lengthAt = 0;
            if( offsetString.charAt(0) == '#' ) {
                lengthAt = 1;
                ranked = true;
            }
            try {
                offset = Integer.parseInt(offsetString.substring(lengthAt));
            } catch (NumberFormatException e) {
                throw new RuntimeException("Bad offset");
            }
            return false;
        }
        protected void processEncoding( String encoding ) {
            int lengthAt = 0;
            switch (encoding.charAt(0) ) {
                case 'i':
                case 'I':
                    lengthAt = 1;
                    signed = true;
                    break;
                case 'u':
                case 'U':
                    lengthAt = 1;
                    signed = false;
                    break;
                case '0':
                case '1':
                case '2':
                case '3':
                case '4':
                case '5':
                case '6':
                case '7':
                case '8':
                case '9':
                    signed = true;
                    lengthAt = 0;
                    break;
                default:
                    throw new RuntimeException("Bad encoding for bitfield");
            }
            try {
                numbits = Integer.parseInt(encoding.substring(lengthAt));
            } catch(NumberFormatException e) {
                throw new RuntimeException("Can't parse int for encoding on bitfield.");
            }
        }
    }
    public static class BitFieldGet extends BitFieldOp {
        public BitFieldGet(String encoding,String offset) {
            processEncoding(encoding);
            processOffset(offset);
        }

        @Override
        public Long operateOn(RedisString rs) {
            return rs.getLongValue(realIndex(),numbits,signed);
        }
    }

    public static class BitFieldSet extends BitFieldOp {
        public long amount;

        public BitFieldSet(String encoding, String offset, long amount, BitFieldOverflowMode mode ) {
            processEncoding(encoding);
            processOffset(offset);
            this.amount = amount;
            this.mode = mode;
        }

        @Override
        public Long operateOn(RedisString rs) {
            long returnValue = rs.getLongValue(realIndex(),numbits,signed);
            if( overflow(amount) ) {
                if( mode == BitFieldOverflowMode.FAIL )
                    return null;
                amount = overflowValue(amount);
            }
            rs.setLongValue(amount,realIndex(),numbits);
            return returnValue;
        }
    }

    public static class BitFieldIncr extends BitFieldOp {
        long amount;
        public BitFieldIncr(String encoding, String offset, long amount, BitFieldOverflowMode mode ) {
            processEncoding(encoding);
            processOffset(offset);
            this.amount = amount;
            this.mode = mode;
        }

        @Override
        public Long operateOn(RedisString rs) {
            int realIndex = realIndex();
            long oldValue = rs.getLongValue(realIndex,numbits,signed);
            long newValue = oldValue + amount;
            //We hardcore overflowed in addition....
            if (((amount ^ newValue) & (oldValue ^ newValue)) < 0 ) {
                //64 bit wraps properly
                if( mode == BitFieldOverflowMode.FAIL )
                    return null;
                if( mode == BitFieldOverflowMode.SAT )
                    if( amount > 0 )
                        return overUnderFlowSigned[(numbits+(signed?0:1))*2];
                    else
                        return 0L;
            } else if (overflow(newValue)) {
                if( mode == BitFieldOverflowMode.FAIL )
                    return null;
                newValue = overflowValue(newValue);
            }
            rs.setLongValue(newValue,realIndex,numbits);
            return newValue;
        }
    }

    @Override
    public String type() { return "string";}
}
