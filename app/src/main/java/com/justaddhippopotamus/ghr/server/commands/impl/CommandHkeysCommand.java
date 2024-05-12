
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.Command;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisHash;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandHkeysCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        //HKEYS key
        RESPArrayScanner commands = item.scanner();
        String key = commands.key();
        RedisHash rh = item.getMainStorage().fetchRO(key, RedisHash.class);
        if( rh == null ) {
            item.whoFor.queueEmptyArray(item.order);
        } else {
            item.whoFor.queue(rh.keys(),item.order);
        }
    }
}
