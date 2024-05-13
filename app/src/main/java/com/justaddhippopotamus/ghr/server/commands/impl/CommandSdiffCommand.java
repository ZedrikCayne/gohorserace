
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
public class CommandSdiffCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        //SDIFF key [key ...]
        RESPArrayScanner commands = item.scanner();
        List<String> keys = commands.remainingElementsRequired(0);
        List<RedisSet> sets = item.getMainStorage().fetchROMany(keys,RedisSet.class);
        RedisSet first = sets.remove(0);
        if( first == null ) {
            item.whoFor.queue(new RedisSet(),item.order);
        } else {
            item.whoFor.queue(first.diff(sets),item.order);
        }
    }
}
