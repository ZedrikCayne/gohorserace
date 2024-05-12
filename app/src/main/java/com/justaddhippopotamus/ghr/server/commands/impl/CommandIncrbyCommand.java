
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArray;
import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisString;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandIncrbyCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        final RESPArrayScanner commands = item.scanner();
        String key = commands.key();
        long by = commands.takeLong();
        commands.errorOnRemains();
        RedisString rs = item.getMainStorage().fetchRW(key, RedisString.class, () -> new RedisString(0) );
        if( rs == null ) {
            throw new RuntimeException("Tried to increment a non-string");
        }
        item.whoFor.queueInteger(rs.increment(by),item.order);
    }
}
