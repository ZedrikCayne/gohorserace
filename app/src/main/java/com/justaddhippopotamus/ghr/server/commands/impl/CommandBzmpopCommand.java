
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArray;
import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.Command;
import com.justaddhippopotamus.ghr.server.Utils;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisSortedSet;
import com.justaddhippopotamus.ghr.server.types.RedisType;

import java.util.List;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandBzmpopCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        //BZMPOP timeout numkeys key [key ...] <MIN | MAX> [COUNT count]
        RESPArrayScanner commands = item.scanner();
        double timeout = commands.takeDouble();
        List<String> keys = commands.getNumKeys();
        boolean MIN = commands.argIsRequired("MAX","MIN");
        int count = commands.argIs("COUNT")?commands.takeInt():1;
        List<RedisSortedSet> sets = item.getMainStorage().fetchRWMany(keys,RedisSortedSet.class,RedisSortedSet::new);
        RedisType.atomicAllStatic(sets,Boolean.class, all -> {
            if( item.timedOut() ) {
                item.whoFor.queueNullBulkString(item.order);
                Utils.setUnblocked(item,all);
                return false;
            }
            int index = 0;
            for( var set : all ) {
                if( !set.isEmpty() ) {
                    RESPArray returnValue = new RESPArray(2);
                    returnValue.addString(keys.get(index));
                    returnValue.addRespElement(RESPArray.arrayOfArraysOfSortedSetValues(MIN?set.popmin(count):set.popmax(count)));
                    item.whoFor.queue(returnValue,item.order);
                    Utils.setUnblocked(item,all);
                    return true;
                }
                ++index;
            }
            Utils.setBlocking(item,all,timeout);
            return true;
        });
    }
}
