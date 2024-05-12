
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArray;
import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisString;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandGetrangeCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        final RESPArrayScanner commands = item.scanner();
        String key = commands.key();
        int start = commands.takeInt();
        int end = commands.takeInt();
        commands.errorOnRemains();

        RedisString rs = item.getMainStorage().fetchRO(key, RedisString.class);
        if( rs == null ) {
            item.whoFor.queueNullBulkString(item.order);
        } else {
            item.whoFor.queue(rs.range(start,end),item.order);
        }
    }
}
