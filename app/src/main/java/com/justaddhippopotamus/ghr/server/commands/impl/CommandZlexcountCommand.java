
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArray;
import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisSortedSet;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandZlexcountCommand extends ICommandImplementation {
    //ZLEXCOUNT key min max
    @Override
    public void runCommand(WorkItem item) {
        final RESPArrayScanner commands = item.scanner();
        String key = commands.key();
        String start = commands.string();
        String stop = commands.string();
        RedisSortedSet ss = item.getMainStorage().fetchRO(key, RedisSortedSet.class);
        if( ss == null ) {
            item.whoFor.queue(new RESPArray(),item.order);
        } else {
            item.whoFor.queueInteger(ss.range(start, stop, true, false, false, false, 0, 0, false).size(),item.order);
        }
    }
}
