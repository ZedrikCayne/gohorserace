
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArray;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisSortedSet;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandZpopmaxCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        String key = item.what.stringAt(1);
        int count = 1;
        if( item.what.size() > 2 )
            count = item.what.intAt(2);
        RedisSortedSet rss = item.getMainStorage().fetchRW(key, RedisSortedSet.class);
        if( rss == null ) {
            item.whoFor.queueEmptyArray(item.order);
        } else {
            item.whoFor.queue(RESPArray.fromListOfSortedSetValues(rss.popmax(count)),item.order);
        }
    }
}
