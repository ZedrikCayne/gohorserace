
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
import com.justaddhippopotamus.ghr.RESP.RESPBulkString;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisHash;

import java.util.List;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandHsetCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        //HSET key field value [field value ...]
        RESPArrayScanner commands = item.scanner();
        String key = commands.key();
        List<RESPBulkString> pairs = commands.remainingBulkStrings();
        RedisHash rh = item.getMainStorage().fetchRW(key, RedisHash.class, RedisHash::new);
        if( commands.commandIs("HSET") ) {
            item.whoFor.queueInteger(rh.addPairs(pairs, false), item.order);
        } else {
            rh.addPairs(pairs,false);
            item.whoFor.queueOK(item.order);
        }
    }
}
