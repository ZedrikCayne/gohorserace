package com.justaddhippopotamus.ghr.server;

import com.justaddhippopotamus.ghr.RESP.IRESP;
import com.justaddhippopotamus.ghr.RESP.RESPArray;
import com.justaddhippopotamus.ghr.RESP.RESPBulkString;
import com.justaddhippopotamus.ghr.RESP.RESPInteger;
import com.justaddhippopotamus.ghr.server.types.RedisHash;
import com.justaddhippopotamus.ghr.server.types.RedisSet;
import com.justaddhippopotamus.ghr.server.types.RedisSortedSet;
import com.justaddhippopotamus.ghr.server.types.RedisType;

import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.LinkedList;
import java.util.Objects;
import java.util.stream.Collectors;

public class RedisCursor {
    TypeStorage on;
    PathMatcher matcher;
    String currentMatch;
    RedisType cursorTarget;
    LinkedList<String> keys;
    long cursorId;

    public long getCursorId() { return cursorId; }

    private boolean matches(String key) {
        if( matcher == null ) return true;
        return matcher.matches(Path.of(key));
    }

    public boolean isDone() {
        return keys.isEmpty();
    }

    private RESPArray bundleReturnValue(RESPArray ra) {
        RESPArray returnValue = new RESPArray(2);
        returnValue.addRespElement(new RESPBulkString(isDone()?"0":String.valueOf(cursorId)));
        returnValue.addRespElement(ra);
        return returnValue;
    }
    public RESPArray scanTable(int count,String newMatch,Class newFilter) {
        RESPArray ra = new RESPArray(count);
        int taken = 0;
        resetMatcherIfNeeded(newMatch);
        for(int i = 0; !keys.isEmpty() && i < count * 2 && taken < count; ++i ) {
            String key = keys.removeFirst();
            if( matches(key) ) {
                RedisType rt = on.fetch(key);
                if (rt != null && ((newFilter == null) || (newFilter == rt.getClass()))) {
                    ra.addString(key);
                    ++taken;
                }
            }
        }
        return bundleReturnValue(ra);
    }


    public RESPArray scanHash(int count, String newMatch, boolean noValues) {
        RESPArray ra = new RESPArray(count);
        resetMatcherIfNeeded(newMatch);
        int [] taken = new int[1];
        for(int i = 0; !keys.isEmpty() && i < count * 2 && taken[0] < count; ++i ) {
            String key = keys.removeFirst();
            if( matches(key) ) {
                cursorTarget.atomic((RedisHash t) -> {
                    if( t.value.containsKey(key) ) {
                        ra.addString(key);
                        if( !noValues ) ra.addRedisString(t.value.get(key));
                        ++taken[0];
                    }
                });
            }
        }
        return bundleReturnValue(ra);
    }

    public RESPArray scanSet(int count, String newMatch) {
        RESPArray ra = new RESPArray(count);
        resetMatcherIfNeeded(newMatch);
        int [] taken = new int[1];
        for(int i = 0; !keys.isEmpty() && i < count * 2 && taken[0] < count; ++i ) {
            String key = keys.removeFirst();
            if( matches(key) ) {
                cursorTarget.atomic((RedisSet x) -> {
                    if( x.keys().contains(new RESPBulkString(key)) ) {
                        ++taken[0];
                        ra.addString(key);
                    }
                });
            }
        }

        return bundleReturnValue(ra);
    }

    public RESPArray scanSortedSet(int count, String newMatch, boolean noValues ) {
        RESPArray ra = new RESPArray(count);
        resetMatcherIfNeeded(newMatch);
        int [] taken = new int[1];
        for(int i = 0; !keys.isEmpty() && i < count * 2 && taken[0] < count; ++i ) {
            String key = keys.removeFirst();
            if( matches(key) ) {
                cursorTarget.atomic((RedisSortedSet x) -> {
                    if( x.keys().contains(key) ) {
                        ++taken[0];
                        ra.addString(key);
                        if( !noValues )ra.addString(Utils.doubleToStringRedisStyle(x.getScore(key)));
                    }
                });
            }
        }

        return bundleReturnValue(ra);
    }

    private void setBasics( long cursorId, String match, RedisType target ) {
        cursorTarget = target;
        this.cursorId = cursorId;
        currentMatch = null;
        matcher = null;
        resetMatcherIfNeeded(match);
    }
    private void resetMatcherIfNeeded(String newMatch) {
        if( !Objects.equals(currentMatch,newMatch) ) {
            currentMatch = newMatch;
            if( newMatch == null ) {
                matcher = null;
            } else {
                matcher = Utils.forGlob(newMatch);
            }
        }
    }

    public RedisCursor(TypeStorage on,long cursorId,String match) {
        this.on = on;
        keys = new LinkedList<>(on.keys());
        setBasics(cursorId,match,null);
    }

    public RedisCursor(RedisHash target, long cursorId, String match) {
        keys = new LinkedList<>(target.keys());
        setBasics(cursorId,match,target);
    }

    public RedisCursor(RedisSortedSet target, long cursorId, String match) {
        keys = new LinkedList<>(target.keys());
        setBasics(cursorId,match,target);
    }

    public RedisCursor(RedisSet target, long cursorId, String match) {
        keys = target.keys().stream().map(RESPBulkString::toString).collect(Collectors.toCollection(LinkedList::new));
        setBasics(cursorId,match,target);
    }
}
