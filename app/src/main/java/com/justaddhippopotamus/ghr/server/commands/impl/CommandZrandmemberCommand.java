
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArray;
import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
import com.justaddhippopotamus.ghr.RESP.RESPBulkString;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.Command;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisSortedSet;

import java.util.List;
import java.util.stream.Collectors;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandZrandmemberCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        RESPArrayScanner arguments = item.scanner();
        String key = arguments.key();
        int count = 1;
        boolean hasCount = false;
        boolean distinct = true;
        if( arguments.hasNext() ) {
            count = arguments.takeInt();
            if (count < 0) {
                count = -count;
                distinct = false;
            }
            hasCount = true;
        }
        boolean WITHSCORES = arguments.argIs("WITHSCORES");
        RedisSortedSet rss = item.getMainStorage().fetchRO(key,RedisSortedSet.class);
        if( rss == null ) {
            if( hasCount )
                item.whoFor.queueEmptyArray(item.order);
            else
                item.whoFor.queueNullBulkString(item.order);
        } else {
            List<RedisSortedSet.SetValue> sv = rss.rand(count,distinct);
            if( hasCount ) {
                if( WITHSCORES )
                    item.whoFor.queue(RESPArray.fromListOfSortedSetValues(sv),item.order);
                else
                    item.whoFor.queue(sv.stream().map(x->x.key).collect(Collectors.toList()), item.order);
            } else {
                item.whoFor.queue(new RESPBulkString(sv.get(0).key),item.order);
            }
        }
    }
}
