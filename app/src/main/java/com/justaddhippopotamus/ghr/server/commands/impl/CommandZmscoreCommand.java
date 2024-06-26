
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArray;
import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.Command;
import com.justaddhippopotamus.ghr.server.Utils;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisSortedSet;

import java.util.ArrayList;
import java.util.List;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandZmscoreCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        //ZMSCORE key member [member ...]
        RESPArrayScanner arguments = item.scanner();
        String key = arguments.key();
        List<String> members = arguments.remainingElementsRequired(0);
        List<String> values = new ArrayList<>(members.size());
        RedisSortedSet rss = item.getMainStorage().fetchRO(key, RedisSortedSet.class);
        if( rss == null ) {
            item.whoFor.queueNullBulkString(item.order);
        } else {
            for (String member : members) {
                Double score = rss.getScore(member);
                values.add(score == null ? null : Utils.doubleToStringRedisStyle(score));
            }
            item.whoFor.queue(RESPArray.RESPArrayWithNulls(values,item.whoFor.clientRESPVersion), item.order);
        }
    }
}
