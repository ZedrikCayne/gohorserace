
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArray;
import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisHash;

import java.util.List;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandHdelCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        //HDEL key field [field ...]
        final RESPArrayScanner commands = item.scanner();
        String key = commands.key();
        List<String> fields = commands.remainingElementsRequired(0);
        RedisHash rh = item.getMainStorage().fetchRW(key,RedisHash.class);
        if( rh == null ) {
            item.whoFor.queueInteger(0,item.order);
        } else {
            item.whoFor.queueInteger(rh.del(fields),item.order);
        }
    }
}
