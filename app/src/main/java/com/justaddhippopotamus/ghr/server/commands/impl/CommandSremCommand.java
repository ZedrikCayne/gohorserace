
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.Command;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisSet;

import java.util.List;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandSremCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        var scanner = item.scanner();
        var key = scanner.key();
        var elements = scanner.remainingBulkStringsRequired(0);
        RedisSet rs = item.getMainStorage().fetchRO(key,RedisSet.class);
        if( rs == null ) {
            item.whoFor.queueInteger(0,item.order);
        } else {
            item.whoFor.queueInteger(rs.removeAll(elements),item.order);
        }

    }
}
