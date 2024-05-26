
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.Command;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisHyperLogLog;
import com.justaddhippopotamus.ghr.server.types.RedisType;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandPfmergeCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        //PFMERGE destination [source [source ...]]
        var commands = item.scanner();
        var dest = commands.key();
        var sources = commands.remainingElementsOptional();
        var saveTo = item.getMainStorage().fetchRW(dest,RedisHyperLogLog.class, RedisHyperLogLog::new);
        if( !sources.isEmpty() )
        saveTo.atomic( (RedisHyperLogLog l) -> {
            var sourceBits = item.getMainStorage().fetchROMany(sources,RedisHyperLogLog.class);
            RedisType.atomicAllStatic(sourceBits, Boolean.class, all -> {
                byte [] max = null;
                for( var hll : all ) {
                    if( hll != null ) max = hll.mergeMax(max);
                }
                if( max != null ) l.setFromMax(max);
                return true;
            });
        });
        item.whoFor.queueOK(item.order);
    }
}
