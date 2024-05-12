
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.Command;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisHash;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandHincrbyfloatCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        //HINCRBYFLOAT key field increment
        RESPArrayScanner commands = item.scanner();
        String key = commands.key();
        String element = commands.string();
        String by = commands.string();
        RedisHash rh = item.getMainStorage().fetchRW(key, RedisHash.class, RedisHash::new);
        item.whoFor.queue( rh.incrFloat(element,by), item.order );
    }
}
