
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.Command;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisSet;
import com.justaddhippopotamus.ghr.server.types.RedisType;

import java.util.List;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandSunionCommand extends ICommandImplementation {
    public static void genericSetUnion(WorkItem item) {
        //SUNION key [key ...]
        //SUNIONSTORE destination key [key ...]
        RESPArrayScanner commands = item.scanner();
        String storeAt = commands.commandIs("SUNIONSTORE")?commands.key():null;
        List<RedisSet> sets = item.getMainStorage().fetchROMany(commands.remainingElementsRequired(0), RedisSet.class);
        RedisSet output = RedisType.atomicAllStatic(sets,RedisSet.class, all -> {
            RedisSet out = new RedisSet();
            for( RedisSet other : all ) out.add(other);
            return out;
        });
        if( storeAt != null ) {
            item.getMainStorage().store(storeAt,output,false,false,false);
            item.whoFor.queueInteger(output.size(),item.order);
        } else {
            item.whoFor.queue(output,item.order);
        }
    }
    @Override
    public void runCommand(WorkItem item) {
        genericSetUnion(item);
    }
}
