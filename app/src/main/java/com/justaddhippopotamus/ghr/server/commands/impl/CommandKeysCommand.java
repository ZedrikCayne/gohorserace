
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.server.*;

import java.util.Set;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandKeysCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        String glob = item.what.stringAt(1);
        Set<String> matchedKeys = item.getMainStorage().keys(glob);
        item.whoFor.queue(matchedKeys,item.order);
    }
}
