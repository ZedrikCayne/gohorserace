
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArray;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.Client;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisSortedSet;
import com.justaddhippopotamus.ghr.server.types.RedisType;

import java.util.ArrayList;
import java.util.List;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandZdiffCommand extends ICommandImplementation {
    public static RedisSortedSet genericZdiff(List<String> keys, Client client) {
        List<RedisSortedSet> redisSortedSetList = client.getMainStorage().fetchROMany(keys,RedisSortedSet.class);
        return RedisType.atomicAllStatic(redisSortedSetList,RedisSortedSet.class,
                fullList -> {
                    RedisSortedSet workSet = null;
                    for( RedisSortedSet s : fullList ) {
                        if( workSet == null ) {
                            if( s == null ) {
                                workSet = new RedisSortedSet();
                                break;
                            }
                            workSet = s.copy(RedisSortedSet.class);
                        } else {
                            if( s == null )
                                continue;
                            workSet.removeAll(s.getSet());
                        }
                    }
                    return workSet;
        });
    }

    public static void genericZdiffCommand(RESPArray commands,Client client,long order) {
        String storeAt = null;
        int firstKey = 2;
        int numKeys = 0;
        List<String> keys = new ArrayList<>(commands.size());
        switch(commands.argAt(0) ) {
            case "ZDIFF":
                numKeys = commands.intAt(1);
                firstKey = 2;
                break;
            case "ZDIFFSTORE":
                storeAt = commands.stringAt(1);
                numKeys = commands.intAt(2);
                firstKey = 3;
                break;
            default:
                throw new RuntimeException("Generic zdiff doesn't handle " + commands.argAt(0) );
        }
        for( int i = firstKey; i < firstKey + numKeys; ++i ) {
            keys.add(commands.stringAt(i));
        }
        boolean WITHSCORES = commands.argAtMaybeIs(firstKey + numKeys,"WITHSCORES");

        RedisSortedSet theDiff = genericZdiff(keys,client);
        if (storeAt != null) {
            client.getMainStorage().store(storeAt, theDiff);
            client.queueInteger(theDiff.size(),order);
        } else {
            client.queue(theDiff.toArray(WITHSCORES),order);
        }
    }
    @Override
    public void runCommand(WorkItem item) {
        genericZdiffCommand(item.what,item.whoFor,item.order);
    }
}
