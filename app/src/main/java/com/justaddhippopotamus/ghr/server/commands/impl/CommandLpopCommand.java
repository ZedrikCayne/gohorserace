
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisList;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandLpopCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        final RESPArrayScanner commands = item.scanner();
        String key = commands.key();
        int count;
        boolean hasCount;
        if( commands.hasNext() ) {
            hasCount = true;
            count = commands.takeInt();
        } else {
            count = 1;
            hasCount = false;
        }
        commands.errorOnRemains();
        RedisList xrl = item.getMainStorage().fetchRO(key, RedisList.class);
        xrl.atomic((RedisList rl) -> {
            if (RedisList.isNullOrEmpty(rl)) {
                item.whoFor.queueNullArray(item.order);
            } else {
                if (hasCount) {
                    item.whoFor.queue(rl.pop(count),item.order);
                } else {
                    item.whoFor.queue(rl.pop(),item.order);
                }
            }
        });
    }
}
