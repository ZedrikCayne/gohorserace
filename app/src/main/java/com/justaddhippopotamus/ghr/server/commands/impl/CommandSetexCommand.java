
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
import com.justaddhippopotamus.ghr.RESP.RESPBulkString;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.Command;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisString;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandSetexCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        //SETEX key seconds value
        RESPArrayScanner commands = item.scanner();
        String key = commands.key();
        long milliseconds = (commands.takeLong() * 1000) + System.currentTimeMillis();
        RedisString value = commands.redisString();
        CommandSetCommand.genericSet( item.whoFor,key, item.order, value, milliseconds, false, false, false, false);
    }
}
