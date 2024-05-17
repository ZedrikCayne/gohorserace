
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
public class CommandBzpopminCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        //BZPOPOMIN key [key ...] timeout
        RESPArrayScanner commands = item.scanner();
        List<String> keys = commands.remainingElementsRequired(-1);
        double timeout = commands.takeDouble();
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
                    List<RedisSortedSet.SetValue> popped = set.popmin(1);
                    returnValue.addString(popped.get(0).key);
                    returnValue.addString(Utils.doubleToStringRedisStyle(popped.get(0).score));
                    item.whoFor.queue(returnValue,item.order);
                    Utils.setUnblocked(item,all);
                    return true;
                }
            }
            Utils.setBlocking(item,all,timeout);
            return true;
        });
    }
}
