
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.Command;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisType;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandTypeCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        RESPArrayScanner commands = item.scanner();
        String key = commands.key();
        commands.errorOnRemains();
        RedisType rt = item.getMainStorage().fetch( key );
        if( rt == null ) {
            item.whoFor.queueSimpleString("none",item.order);
        } else {
            //string, list, set, zset, hash and stream
            item.whoFor.queueSimpleString( rt.type(), item.order );
        }
        Command.BadDefaultCommandImplementation(item);
    }
}
