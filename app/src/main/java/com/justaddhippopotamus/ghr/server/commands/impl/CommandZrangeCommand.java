
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArray;
import com.justaddhippopotamus.ghr.server.*;
import com.justaddhippopotamus.ghr.server.types.RedisSortedSet;

import java.util.List;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandZrangeCommand extends ICommandImplementation {
    //ZRANGE key start stop [BYSCORE | BYLEX] [REV] [LIMIT offset count]   [WITHSCORES]
    @Override
    public void runCommand(WorkItem item) {
        zrangeGenericCommandAndQueue(item.what, item.whoFor, item.order, 1,false,false, false, false, null);
    }
    public static void zrangeGenericCommandAndQueue(RESPArray commands, Client client, long order, int sourceKeyIndex, boolean byscore, boolean bylex, boolean rev, boolean withscores, String storeTo ) {
        int len = commands.size();
        boolean startInclusive = true;
        boolean stopInclusive = true;

        String key = commands.stringAt(sourceKeyIndex );
        String start = commands.stringAt(sourceKeyIndex + 1);
        String stop = commands.stringAt(sourceKeyIndex + 2);
        boolean WITHSCORES = withscores;
        boolean BYSCORE = byscore;
        boolean BYLEX = bylex;
        boolean REV = rev;
        boolean LIMIT = false;
        int offset = 0;
        int count = 0;
        for( int i = sourceKeyIndex + 3; i < len; ++i ) {
            String arg = commands.argAt(i);
            switch( arg ) {
                case "BYSCORE":
                    BYSCORE = true;
                    break;
                case "BYLEX":
                    BYLEX = true;
                    break;
                case "LIMIT":
                    LIMIT = true;
                    offset = commands.intAt(++i);
                    count = commands.intAt(++i);
                    break;
                case "WITHSCORES":
                    WITHSCORES = true;
                    break;
                case "REV":
                    REV = true;
                    break;
                default:
                    throw new RuntimeException("Bad args on Zrange.");
            }
        }
        RedisSortedSet ss = client.getMainStorage().fetchRO(key, RedisSortedSet.class);
        if (ss == null) {
            if( storeTo != null ) {
                client.getMainStorage().store(storeTo,new RedisSortedSet());
                client.queueInteger(0,order);
            } else {
                client.queue(new RESPArray(),order);
            }
        } else {
            List<String> returnValue = ss.range(start, stop, BYLEX, BYSCORE, REV, LIMIT, offset, count, WITHSCORES);
            if( storeTo != null ) {
                client.getMainStorage().store(storeTo, new RedisSortedSet(returnValue));
                client.queueInteger(returnValue.size()/2,order);
            } else {
                client.queue( returnValue,order );
            }
        }
    }
}
