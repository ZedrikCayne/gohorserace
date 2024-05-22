
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
import com.justaddhippopotamus.ghr.RESP.RESPBulkString;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.Command;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisHash;

import java.util.List;
import java.util.Map;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandHrandfieldCommand extends ICommandImplementation {
    //HRANDFIELD key [count [WITHVALUES]]
    @Override
    public void runCommand(WorkItem item) {
        RESPArrayScanner commands = item.scanner();
        String key = commands.key();
        int count;
        boolean WITHVALUES;
        boolean hasCount;
        if( commands.hasNext() ) {
            hasCount = true;
            count = commands.takeInt();
            WITHVALUES = commands.argIs("WITHVALUES");
        } else {
            hasCount = false;
            count = 1;
            WITHVALUES = false;
        }
        RedisHash rh = item.getMainStorage().fetchRO(key, RedisHash.class);
        if( rh == null ) {
            if( hasCount )
                item.whoFor.queueEmptyArray(item.order);
            else
                item.whoFor.queueNullBulkString(item.order);
        } else {
            rh.atomic( (RedisHash h) -> {
                if( h.size() == 0 ) {
                    if( hasCount )
                        item.whoFor.queueEmptyArray(item.order);
                    else
                        item.whoFor.queueNullBulkString(item.order);
                }
                if (hasCount)
                    item.whoFor.queueStrings(h.rand(count, WITHVALUES), item.order);
                else
                    item.whoFor.queue(new RESPBulkString(h.rand(count, false).get(0)), item.order);
            });
        }
    }
}
