
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.Command;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisString;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandSetrangeCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        //SETRANGE key offset value
        RESPArrayScanner commands = item.scanner();
        String key = commands.key();
        int offset = commands.takeInt();
        RedisString append = commands.redisString();
        RedisString appendTo = item.getMainStorage().fetchRW(key,RedisString.class,RedisString::new);
        item.whoFor.queueInteger(appendTo.setrange(offset,append),item.order);
    }
}
