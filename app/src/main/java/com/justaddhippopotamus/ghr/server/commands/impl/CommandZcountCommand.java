
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArray;
import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisSortedSet;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandZcountCommand extends ICommandImplementation {
    //ZCOUNT key min max
    @Override
    public void runCommand(WorkItem item) {
        final RESPArrayScanner commands = item.scanner();
        String key = commands.key();
        String min = commands.string();
        String max = commands.string();
        commands.errorOnRemains();
        RedisSortedSet ss = item.getMainStorage().fetchRO(key, RedisSortedSet.class);
        if(ss == null) {
            item.whoFor.queueSimpleError("No such key: " + key, item.order);
        } else {
            item.whoFor.queueInteger(ss.count(min, max), item.order);
        }
    }
}
