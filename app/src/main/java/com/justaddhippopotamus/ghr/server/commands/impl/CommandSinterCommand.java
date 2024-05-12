
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
import com.justaddhippopotamus.ghr.server.Client;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.Command;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisSet;
import com.justaddhippopotamus.ghr.server.types.RedisSortedSet;
import com.justaddhippopotamus.ghr.server.types.RedisType;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandSinterCommand extends ICommandImplementation {
    public static RedisSet genericCommand(WorkItem item) {
        RESPArrayScanner commands = item.scanner();
        List<String> keys = commands.remainingElementsRequired(0);
        Client client = item.whoFor;
        List<RedisSet> allSets = client.getMainStorage().fetchROMany(keys,RedisSet.class);
        if( allSets.contains(null) )
            return new RedisSet();
        allSets.sort((a,b)->Integer.compare(a.getSet().size(),b.getSet().size()));
        return RedisType.atomicAllStatic(allSets, RedisSet.class, all -> {
            Set<String> output = new HashSet<>();
            output.addAll( all.get(0).getSet() );
            int len = all.size();
            Set<String> stuffToRemove  = new HashSet<>();
            for( int i = 1; i < len; ++i ) {
                Set<String> currentSet = all.get(i).getSet();
                for (String s : output) {
                    if (!currentSet.contains(s))
                        stuffToRemove.add(s);
                }
                for (String s : stuffToRemove)
                    output.remove(s);
                if (output.isEmpty())
                    break;
            }
            return new RedisSet(output);
        });
    }

    @Override
    public void runCommand(WorkItem item) {
        RedisSet rs = genericCommand(item);
        item.whoFor.queue(rs,item.order);
    }
}
