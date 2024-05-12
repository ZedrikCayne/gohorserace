
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArray;
import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
import com.justaddhippopotamus.ghr.RESP.RESPBulkString;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisSortedSet;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandZscoreCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        final RESPArrayScanner commands = item.scanner();
        String key = commands.key();
        String member = commands.key();
        commands.errorOnRemains();

        RedisSortedSet rss = item.getMainStorage().fetchRO(key, RedisSortedSet.class);
        if( rss != null ) {
            Double score = rss.getScore( member );
            if( score == null ) {
                item.whoFor.queueNullBulkString(item.order);
            } else {
                item.whoFor.queue( new RESPBulkString(score), item.order );
            }
        }
    }
}
