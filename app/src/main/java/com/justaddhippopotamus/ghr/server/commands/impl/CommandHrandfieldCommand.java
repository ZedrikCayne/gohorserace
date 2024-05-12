
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
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
        int count = 1;
        boolean WITHVALUES = false;
        boolean hasCount = false;
        if( commands.hasNext() ) {
            hasCount = true;
            count = commands.takeInt();
            WITHVALUES = commands.argIs("WITHVALUES");
        }
        RedisHash rh = item.getMainStorage().fetchRO(key, RedisHash.class);
        if( rh == null ) {
            if( hasCount == false )
                item.whoFor.queueNullBulkString(item.order);
            else
                item.whoFor.queueEmptyArray(item.order);
        } else {
            item.whoFor.queue(rh.rand(count,WITHVALUES),item.order);
        }
    }
}
