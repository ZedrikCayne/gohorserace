
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisList;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandLremCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        final RESPArrayScanner commands = item.scanner();
        String key = commands.key();
        int count = commands.takeInt();
        String what = commands.string();
        commands.errorOnRemains();

        RedisList rl = item.getMainStorage().fetchRW(key, RedisList.class);
        if( rl != null ) {
            item.whoFor.queueInteger( rl.remove(what,count),item.order );
        } else {
            item.whoFor.queueInteger(0,item.order);
        }
    }
}
