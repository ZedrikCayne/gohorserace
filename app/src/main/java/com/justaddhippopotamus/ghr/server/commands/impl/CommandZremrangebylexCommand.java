
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArray;
import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisSortedSet;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandZremrangebylexCommand extends ICommandImplementation {
    //ZREMRANGEBYLEX key min max
    @Override
    public void runCommand(WorkItem item) {
        final RESPArrayScanner commands = item.scanner();
        String key = commands.key();
        String min = commands.string();
        String max = commands.string();
        commands.errorOnRemains();

        RedisSortedSet rss = item.getMainStorage().fetchRW(key, RedisSortedSet.class);
        if( rss == null ) {
            item.whoFor.queueInteger(0, item.order);
        } else {
            rss.atomic( t ->
                item.whoFor.queueInteger( rss.removeAll(rss.range(min,max,true,false,false,false,0,0,false)),item.order));
        }
    }
}
