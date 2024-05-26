
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.Command;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisHyperLogLog;
import com.justaddhippopotamus.ghr.server.types.RedisList;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandPfcountCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        //PFCOUNT key [key ...]
        var keys = item.scanner().remainingElementsRequired(0);
        var elements = item.getMainStorage().fetchROMany(keys, RedisHyperLogLog.class);
        if( elements.size() > 1 ) {
            item.whoFor.queueInteger(RedisList.atomicAllStatic(elements,Long.class,all -> {
                byte [] max = null;
                int returnValue = 0;
                for( var hll : all ) {
                    if (hll == null) continue;
                    max = hll.mergeMax(max);
                }
                return RedisHyperLogLog.hllCount(RedisHyperLogLog.getHistogramOfBytes(max));
            }),item.order);
        } else {
            item.whoFor.queueInteger(elements.get(0).hllSelfCount(), item.order);
        }
    }
}
