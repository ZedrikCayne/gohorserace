
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArray;
import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.Utils;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisSortedSet;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandZincrbyCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        final RESPArrayScanner commands = item.scanner();
        String key = commands.key();
        double increment = commands.takeDouble();
        String member = commands.string();
        commands.errorOnRemains();

        RedisSortedSet ss = item.getMainStorage().fetchRW(key, RedisSortedSet.class, RedisSortedSet::new);
        item.whoFor.queueBulkString(Utils.doubleToStringRedisStyle(ss.incr(member, increment)), item.order);
    }
}
