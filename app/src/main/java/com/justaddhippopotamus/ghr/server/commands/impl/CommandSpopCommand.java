
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPBulkString;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisSet;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandSpopCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        String key = item.what.stringAt(1);
        int count = 1;
        boolean hasCount=false;
        if( item.what.size() > 2 ) {
            count = item.what.intAt(2);
            hasCount = true;
        }
        RedisSet rs = item.getMainStorage().fetchRO(key, RedisSet.class);
        if( rs == null ) {
            item.whoFor.queueNullArray(item.order);
        } else {
            if( hasCount )
                item.whoFor.queue(rs.pop(count),item.order);
            else
                item.whoFor.queue(new RESPBulkString(rs.pop()),item.order);
        }
    }
}
