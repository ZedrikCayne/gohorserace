
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.Command;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisHyperLogLog;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandPfaddCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        //PFADD key [element [element ...]]
        var commands = item.scanner();
        String key = commands.key();
        var elements = commands.remainingBulkStrings();
        boolean [] bNew = {false};
        RedisHyperLogLog hll = item.getMainStorage().fetchRW(key,RedisHyperLogLog.class,() -> {
            bNew[0] = true;
            return new RedisHyperLogLog();
        });
        if( bNew[0] && elements.isEmpty() ) {
            item.whoFor.queueInteger(1,item.order);
        } else {
            if (elements.isEmpty()) {
                item.whoFor.queueInteger(0, item.order);
            } else {
                item.whoFor.queueInteger(hll.hllAdd(elements),item.order);
            }
        }
    }
}
