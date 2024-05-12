
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArray;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.Client;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisString;

import java.util.HashMap;
import java.util.Map;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandMsetCommand extends ICommandImplementation {
    public static long genericMset(RESPArray commands, Client client, boolean NX ) {
        int len = commands.size();
        Map<String,RedisString> setPairs = new HashMap<>();
        for( int i = 1; i < len; i += 2 ) {
            setPairs.put(commands.stringAt(i), commands.redisStringAt(i+1) );
        }
        return client.getMainStorage().mset(setPairs,NX);
    }
    @Override
    public void runCommand(WorkItem item) {
        genericMset(item.what,item.whoFor,false);
        item.whoFor.queueOK(item.order);
    }
}
