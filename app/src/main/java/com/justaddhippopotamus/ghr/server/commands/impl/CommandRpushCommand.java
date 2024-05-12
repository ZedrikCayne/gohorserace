
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisList;

import java.util.List;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandRpushCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        //RPUSH key element [element ...]
        final RESPArrayScanner commands = item.scanner();
        String key = commands.key();
        List<String> elements = commands.remainingElementsRequired(0);
        RedisList rl = item.getMainStorage().fetchRW(key,RedisList.class,RedisList::new);
        item.whoFor.queueInteger( rl.rpush(elements), item.order );
    }
}
