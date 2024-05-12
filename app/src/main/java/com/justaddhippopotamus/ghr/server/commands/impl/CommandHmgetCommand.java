
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArray;
import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
import com.justaddhippopotamus.ghr.RESP.RESPBulkString;
import com.justaddhippopotamus.ghr.server.Client;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.Command;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisHash;
import com.justaddhippopotamus.ghr.server.types.RedisString;

import java.util.List;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandHmgetCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        final RESPArrayScanner commands = item.scanner();
        String key = commands.key();
        List<String> fields = commands.remainingElementsRequired(0);
        int len = fields.size();
        commands.errorOnRemains();
        RedisHash rh = item.getMainStorage().fetchRO(key, RedisHash.class);
        if( rh == null ) {
            RESPArray returnValue = new RESPArray(fields.size());
            for(int i = 0; i < len; ++i ) {
                returnValue.addRespElement(Client.NIL_ARRAY);
            }
            item.whoFor.queue(returnValue, item.order);
        } else {
            RESPArray returnValue = new RESPArray(fields.size());
            for(int i = 0; i < len; ++i ) {
                RedisString rs = rh.value.getOrDefault(fields.get(i),null);
                if( rs == null )
                    returnValue.addRespElement(Client.NIL_ARRAY);
                else
                    returnValue.addRespElement(new RESPBulkString(rs));
            }
            item.whoFor.queue( returnValue,item.order );
        }
    }
}
