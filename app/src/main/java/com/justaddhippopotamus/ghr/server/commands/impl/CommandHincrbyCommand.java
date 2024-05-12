
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.Command;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisHash;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandHincrbyCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        //HINCRBY key field increment
        RESPArrayScanner commands = item.scanner();
        String key = commands.key();
        String field = commands.string();
        long by = commands.takeLong();
        RedisHash rh = item.getMainStorage().fetchRW(key, RedisHash.class, RedisHash::new);
        item.whoFor.queueInteger( rh.incrBy(field,by), item.order );
    }
}
