
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisList;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandLsetCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        //LSET key index element
        final RESPArrayScanner commands = item.scanner();
        String key = commands.key();
        int index = commands.takeInt();
        String element = commands.string();
        commands.errorOnRemains();

        RedisList rl = item.getMainStorage().fetchRO(key, RedisList.class);
        if( rl == null ) {
            item.whoFor.queueSimpleError("Key doesn't exist.",item.order);
        } else {
            if (rl.set(element, index)) {
                item.whoFor.queueSimpleError("Index " + index + " out of range on key " + key,item.order);
            } else {
                item.whoFor.queueOK(item.order);
            }
        }
    }
}
