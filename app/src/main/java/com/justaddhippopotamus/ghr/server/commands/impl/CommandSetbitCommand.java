
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArray;
import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisString;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandSetbitCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        final RESPArrayScanner commands = item.scanner();
        String key = commands.key();
        int offset = commands.takeInt();
        int value = commands.takeInt();
        commands.errorOnRemains();
        if( value < 0 || value > 1 ) {
            item.whoFor.queueSimpleError("Value in set bit needs to be 1 or 0", item.order );
        } else {
            RedisString rs = item.getMainStorage().fetchRW(key, RedisString.class, RedisString::new);
            item.whoFor.queueInteger(rs.setbit(offset, value), item.order);
        }
    }
}
