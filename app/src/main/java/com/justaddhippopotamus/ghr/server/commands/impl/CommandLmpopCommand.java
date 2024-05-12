
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArray;
import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
import com.justaddhippopotamus.ghr.RESP.RESPInteger;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisList;

import java.util.ArrayList;
import java.util.List;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandLmpopCommand extends ICommandImplementation {
    //LMPOP numkeys key [key ...] <LEFT | RIGHT> [COUNT count]
    @Override
    public void runCommand(WorkItem item) {
        final RESPArrayScanner commands = item.scanner();
        List<String> keys = commands.getNumKeys();
        boolean LEFT = commands.argIsRequired("RIGHT","LEFT");
        int count = 1;
        if( commands.argIs( "COUNT" ) ) {
            count = commands.takeInt();
        }
        for( String key : keys ) {
            RedisList rl = item.getMainStorage().fetchRO(key, RedisList.class);
            if( !RedisList.isNullOrEmpty(rl) ) {
                List<String> returnValue = LEFT?rl.pop(count):rl.rpop(count);
                if( returnValue == null || returnValue.isEmpty() )
                    continue;
                RESPArray ra = new RESPArray();
                ra.addRespElement(new RESPInteger(returnValue.size()));
                ra.addRespElement(new RESPArray(returnValue));
                item.whoFor.queue(ra, item.order);
                return;
            }
        }
        item.whoFor.queueNullBulkString(item.order);
    }
}
