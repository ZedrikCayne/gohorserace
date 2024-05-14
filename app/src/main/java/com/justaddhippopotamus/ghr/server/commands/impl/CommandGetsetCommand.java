
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPBulkString;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.Command;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisString;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandGetsetCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        //GETSET key value
        String key = item.what.stringAt(1);
        RedisString rs = item.what.redisStringAt(2);
        CommandSetCommand.genericSet(item.whoFor,key,item.order,rs,0,false,false,true,false);
    }
}
