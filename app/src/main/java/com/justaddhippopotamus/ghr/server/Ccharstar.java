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
    private byte [] backingBuffer;
    private int internalOffset;
    private Ccharstar( byte [] backingBuffer ) {
        internalOffset = 0;
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
}
