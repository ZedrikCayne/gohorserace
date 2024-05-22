
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisSet;

import java.util.List;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandSaddCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        //SADD key member [member ...]
        final RESPArrayScanner commands = item.scanner();
        String key = commands.key();
        var elements = commands.remainingBulkStringsRequired(0);
        RedisSet rs = item.getMainStorage().fetchRW(key, RedisSet.class, RedisSet::new);
        item.whoFor.queueInteger(rs.add(elements),item.order);
    }
}
