
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
public class CommandSdiffstoreCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        //SDIFFSTORE key destination [key ...]
        RESPArrayScanner commands = item.scanner();
        String destination = commands.key();
        List<String> keys = commands.remainingElementsRequired(0);
        List<RedisSet> sets = item.getMainStorage().fetchROMany(keys,RedisSet.class);
        RedisSet first = sets.remove(0);
        RedisSet toStore;
        if( first == null ) {
            toStore = new RedisSet();
        } else {
            toStore = first.diff(sets);
        }
        item.getMainStorage().store(destination,toStore);
        item.whoFor.queueInteger(toStore.size(),item.order);
    }
}
