
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.Command;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisHash;
import com.justaddhippopotamus.ghr.server.types.RedisSet;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandHexistsCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        RESPArrayScanner commands = item.scanner();
        String key = commands.key();
        String element = commands.string();
        commands.errorOnRemains();
        RedisHash rs = item.getMainStorage().fetchRO(key,RedisHash.class);
        if( rs == null ) {
            item.whoFor.queueInteger(0, item.order );
        } else {
            item.whoFor.queueInteger(rs.value.containsKey(element)?1:0,item.order);
        }
    }
}
