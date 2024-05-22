
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArray;
import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisList;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandLinsertCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        //LINSERT key <BEFORE | AFTER> pivot element
        final RESPArrayScanner commands = item.scanner();
        String key = commands.key();
        boolean BEFORE = commands.argIsRequired("AFTER","BEFORE");
        var pivot = commands.bulkString();
        var what = commands.bulkString();
        commands.errorOnRemains();
        RedisList rl = item.getMainStorage().fetchRW(key,RedisList.class,RedisList::new);
        item.whoFor.queueInteger(rl.insert(BEFORE,pivot,what),item.order);
    }
}
