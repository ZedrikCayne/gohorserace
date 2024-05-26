package com.justaddhippopotamus.ghr.server;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
public class Ccharstar {
    private static class CcharstarStack {
        private Ccharstar [] tempBuffers;
        private int currentBuffer;
        protected int sizeOfBuffers;
        public CcharstarStack(int num, int size) {
            sizeOfBuffers = size;
            tempBuffers = new Ccharstar [num];
            currentBuffer = 0;
            for( int i = 0; i < num; ++i ) {
                tempBuffers[i] = new Ccharstar(new byte[size]);
            }
        }
        public Ccharstar grabOne() {
            currentBuffer=(currentBuffer+1)%tempBuffers.length;
            tempBuffers[currentBuffer].reset();
            return tempBuffers[currentBuffer];
        }
    }
    private static Map<Integer,CcharstarStack> stacks = new HashMap<>();
    private static int sizeNextPow2(int size) {
        int nextSize = size;
        nextSize--;
        nextSize |= nextSize >> 1;
        nextSize |= nextSize >> 2;
        nextSize |= nextSize >> 4;
        nextSize |= nextSize >> 8;
        nextSize |= nextSize >> 16;
        nextSize++;
        return nextSize;
    }

    private static final int MAX_SIZE_OF_STACK = 16384;
    private static final int MIN_NUMBER_OF_TEMPS = 4;
    public synchronized static Ccharstar grabOne(int size) {
        int nextPow2 = sizeNextPow2(size);
        if( nextPow2 > MAX_SIZE_OF_STACK )
            return new Ccharstar( new byte [size] );
        if(!stacks.containsKey(nextPow2)) {
            int numTemps = MAX_SIZE_OF_STACK / size;
            numTemps = Math.max(MIN_NUMBER_OF_TEMPS,numTemps);
            stacks.put(nextPow2,new CcharstarStack(numTemps,nextPow2));
        }
        return stacks.get(nextPow2).grabOne();
    }
    public static Ccharstar grabNew(long size) {
        return new Ccharstar( new byte [(int)size] );
    }
    public static Ccharstar getNull() {
        return new Ccharstar((byte[])null);
    }
    public static Ccharstar on(byte [] that, int initialOffset) {
        return new Ccharstar(that,initialOffset);
    }
    public int andAt(int value, int offset) {
        return (backingBuffer[internalOffset + offset] &= (byte)value)&0xFF;
    }
    public int and(int value, int offset) {
        return (backingBuffer[internalOffset + offset] & (byte)value)&0xFF;
    }
    public int orAt(int value, int offset) {
        return (backingBuffer[internalOffset + offset] |= (byte)value)&0xFF;
    }
    public int or(int value, int offset) {
        return (backingBuffer[internalOffset + offset] | (byte)value)&0xFF;
    }
    public int xorAt(int value, int offset) {
        return (backingBuffer[internalOffset + offset] ^= (byte)value)&0xFF;
    }
    public int xor(int value, int offset) {
        return (backingBuffer[internalOffset + offset] ^ (byte)value)&0xFF;
    }
    public int notAt(int offset) {
        int newOffset = internalOffset + offset;
        backingBuffer[ newOffset ] = (byte)~backingBuffer[ newOffset ];
        return backingBuffer[ newOffset ] & 0xFF;
    }
    public int not(int offset) {
        int newOffset = internalOffset + offset;
        return (~backingBuffer[ newOffset ]) & 0xFF;
    }
    private byte [] backingBuffer;
    private int internalOffset;
    private Ccharstar( byte [] backingBuffer ) {
        internalOffset = 0;
        this.backingBuffer = backingBuffer;
    }
    private Ccharstar( byte [] backingBuffer, int initialOffset ) {
        internalOffset = initialOffset;
        this.backingBuffer = backingBuffer;
    }
    private Ccharstar(Ccharstar other) {
        internalOffset = other.internalOffset;
        backingBuffer = other.backingBuffer;
    }
    public byte [] buff() { return backingBuffer; }
    public int left() { return backingBuffer.length - internalOffset;}
    public void reset() { internalOffset = 0; }
    public void add(int offset) { internalOffset += offset; }
    public int  get() { return ((int)backingBuffer[internalOffset++]&0xFF); }
    public long getl() { return ((long)backingBuffer[internalOffset++]&0xFF); }
    public int  get(int offset) { return ((int)backingBuffer[internalOffset + offset])&0xFF; }
    public void put(int offset, int what) { backingBuffer[internalOffset + offset] = (byte)(what&0xFF); }
    public void put(int offset, long what) {
        backingBuffer[internalOffset + offset] = (byte)(what&0xFF);
    }
    public void put(int offset, byte what) { backingBuffer[internalOffset + offset] = what; }
    public Ccharstar dup() {return new Ccharstar(this);}
    public void set(Ccharstar other) { backingBuffer = other.backingBuffer; internalOffset = other.internalOffset; }
    public void set(Ccharstar other,int offset) { backingBuffer = other.backingBuffer; internalOffset = other.internalOffset + offset; }
    public double toDouble() {
        if (backingBuffer.length < 8) throw new RuntimeException("Not enough bytes to make a double.");
        return ByteBuffer.wrap(backingBuffer).getDouble();
    }

    public void endianSwap(int swapLen ) {
        if( backingBuffer.length < swapLen ) throw new RuntimeException("Cannot swap an odd shaped buffer.");
        byte t;
        int swapTo = swapLen/2;
        for( int i = 0; i < swapTo; i++ ) {
            t = backingBuffer[ i ];
            backingBuffer[ i ] = backingBuffer[swapLen - 1 - i];
            backingBuffer[ swapLen - 1 - i ] = t;
        }
    }

    public void copyTo(Ccharstar out,int length) {
        System.arraycopy(backingBuffer,internalOffset,out.backingBuffer,out.internalOffset,length);
        add(length);out.add(length);
    }

    //This does not make any sense in big endian.
    public long grabRemaining64bitLongLittleEndian() {
        int left = left();
        if( left >= 8 ) return grab64bitLongLittleEndian();
        long result = 0;
        switch(left) {
            case 7: result = get();
            case 6: result |= get();
            case 5: result |= get();
            case 4: result |= get();
            case 3: result |= get();
            case 2: result |= get();
            case 1: result |= get();
        }
        return result;
    }

    public long grab64bitLongLittleEndian() {
        long result = 0;
        result = get(0);
        result |= get(1)<<8;
        result |= get(2)<<16;
        result |= get(3)<<24;
        result |= get(4)<<32;
        result |= get(5)<<40;
        result |= get(6)<<48;
        result |= get(7)<<56;
        internalOffset+=8;
        return result;
    }
    public long grab64byteLongBigEndian() {
        long result = 0;
        result = get(7);
        result |= get(6)<<8;
        result |= get(5)<<16;
        result |= get(4)<<24;
        result |= get(3)<<32;
        result |= get(2)<<40;
        result |= get(1)<<48;
        result |= get(0)<<56;
        internalOffset+=8;
        return result;
    }
}
