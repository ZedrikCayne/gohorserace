package com.justaddhippopotamus.ghr.server.types;

import com.justaddhippopotamus.ghr.RESP.IRESP;
import com.justaddhippopotamus.ghr.RESP.RESPInteger;
import com.justaddhippopotamus.ghr.server.Server;
import com.justaddhippopotamus.ghr.server.WorkItem;

import javax.annotation.CheckForNull;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;

public abstract class RedisType {
    public static final int EXPIRY_SET = 1;
    public static final int EXPIRY_NOT_SET = 0;
    public boolean dirty;
    public boolean writeLocked;
    public long expireAtMilliseconds;

    public abstract String type();

    public static int realIndex(int index, int len) {
        return index<0?len+index:index;
    }

    public synchronized int setTTLMilliseconds( long milliseconds, boolean NX, boolean XX, boolean GT, boolean LT ) {
        return setExpireMilliseconds( milliseconds + System.currentTimeMillis(), NX, XX, GT, LT);
    }
    public synchronized int setTTLSeconds( long seconds, boolean NX, boolean XX, boolean GT, boolean LT ) {
        long milliseconds = seconds * 1000;
        return setTTLMilliseconds( milliseconds, NX, XX, GT, LT);
    }
    public synchronized int setExpireMilliseconds( long newExpireMilliseconds, boolean NX, boolean XX, boolean GT, boolean LT ) {
        boolean hasExpirySet = !isPersistent();
        if( ( NX && hasExpirySet )
          ||( XX && !hasExpirySet )
          ||( GT && (!hasExpirySet && newExpireMilliseconds >= expireAtMilliseconds))
          ||( LT && (!hasExpirySet && newExpireMilliseconds <= expireAtMilliseconds)) )
            return EXPIRY_NOT_SET;
        expireAtMilliseconds = newExpireMilliseconds;
        return EXPIRY_SET;
    }

    public synchronized boolean canBeReaped() {
        return false;
    }
    public synchronized long getTTLMilliseconds() {
        return System.currentTimeMillis() - expireAtMilliseconds;
    }
    public synchronized long getTTLSeconds() {
        return getTTLMilliseconds() / 1000L;
    }
    public synchronized long getExpireAtMilliseconds() {
        return expireAtMilliseconds;
    }
    public synchronized long persist() {
        long oldMilis = expireAtMilliseconds;
        expireAtMilliseconds = 0;
        return oldMilis;
    }
    public synchronized boolean isPersistent() {
        return expireAtMilliseconds == 0;
    }
    public synchronized boolean expired() {
        return expireAtMilliseconds == 0 ? false : System.currentTimeMillis() > expireAtMilliseconds;
    }
    //Synchronized is atomic 'enough' for our purposes. Hopefully?
    @SuppressWarnings(value="unchecked")
    public synchronized <T extends RedisType> void atomic(RedisGo<T> doThis ) {
        doThis.accept((T)this);
    }

    protected synchronized <T extends RedisType,R> R syncAll(int which, int len, List<T> all, Class<R> returnType, RedisGoMany<T,R> what) {
        if( which >= len ) {
            return what.accept(all);
        }
        for( int i = which; i < len; ++i ) {
            T next = all.get(i);
            if( next != null ) {
                return next.syncAll(i + 1,len,all,returnType,what);
            }
        }
        return what.accept(all);
    }

    public synchronized int size() { return 0; }
    public synchronized boolean isEmpty() { return size() != 0; }

    public synchronized <T extends RedisType,R> R atomicAll(List<T> all, Class<R> returnType, RedisGoMany<T,R> what) {
        return syncAll(0,all.size(),all,returnType,what);
    }

    public static <T extends RedisType,R> R atomicAllStatic(List<T> all, Class<R> returnType, RedisGoMany<T,R> what) {
        int len = all.size();
        for( int i = 0; i < len; ++i ) {
            T current = all.get(i);
            if( current != null ) {
                return current.syncAll(i + 1, len, all, returnType, what);
            }
        }
        return what.accept(all);
    }

    public abstract void writeTo(OutputStream os) throws IOException;
    public abstract void readFrom(InputStream is) throws IOException;

    public void readTTL(InputStream is) throws IOException {
        RESPInteger ri = RESPInteger.readFull(is);
        expireAtMilliseconds = ri.value;
    }
    public void writeTTL(OutputStream os) throws IOException {
        RESPInteger ri = new RESPInteger(expireAtMilliseconds);
        ri.publishTo(os);
    }

    public static boolean isNullOrExpired(@CheckForNull final RedisType rt) {
        return rt == null || rt.expired();
    }

    public static boolean exists(@CheckForNull final RedisType rt) {
        return rt!=null&&!rt.expired()&&!(rt.isEmpty());
    }

    public abstract IRESP wireType(IRESP.RESPVersion v);

    public void copyFrom(RedisType other) {
        this.expireAtMilliseconds = other.expireAtMilliseconds;
        this.dirty = other.dirty;
        this.writeLocked = other.writeLocked;
    }

    protected final LinkedHashSet<WorkItem> blocked = new LinkedHashSet<>();
    public synchronized void queue(WorkItem w) { blocked.add(w); }
    public synchronized void unqueue(WorkItem w) { blocked.remove(w); }
    public synchronized void unqueueAll(Server s) { blocked.forEach(s::execute); blocked.clear(); }


    public abstract <T extends RedisType> T copy(Class<T> type);
}

