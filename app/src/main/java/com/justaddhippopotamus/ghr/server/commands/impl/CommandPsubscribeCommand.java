package com.justaddhippopotamus.ghr.server.commands.impl;

import java.util.List;

import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.Command;
import com.justaddhippopotamus.ghr.server.WorkItem;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandPsubscribeCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        RESPArrayScanner commands = item.scanner();
        List<String> patterns = commands.remainingElementsRequired(0);
        patterns.stream().forEach(pattern->item.whoFor.subscribeToPattern(item.getServer().channelManager().getPattern(pattern)));
        item.whoFor.queueNop(item.order);
    }
}
