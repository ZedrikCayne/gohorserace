
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisString;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandIncrCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        String key = item.what.stringAt(1);
        RedisString rs = item.getMainStorage().fetchRW(key, RedisString.class, () -> new RedisString(0) );
        if( rs == null ) {
            throw new RuntimeException("Tried to decrement a non-string");
        }
        item.whoFor.queueInteger(rs.increment(1),item.order);
    }
}
