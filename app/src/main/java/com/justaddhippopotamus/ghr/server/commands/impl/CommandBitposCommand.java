
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArray;
import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisString;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandBitposCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        final RESPArrayScanner commands = item.scanner();
        String key = commands.key();
        int bit = commands.takeInt();
        int start = 0;
        int end = -1;
        if( commands.hasNext() ) start = commands.takeInt();
        if( commands.hasNext() ) end = commands.takeInt();
        boolean BIT_INDEX = commands.argIs("BIT");
        RedisString rs = item.getMainStorage().fetchRO(key, RedisString.class);
        if( rs == null ) {
            item.whoFor.queueInteger(-1, item.order);
        } else {
            item.whoFor.queueInteger( rs.bitpos(bit,start,end,BIT_INDEX), item.order);
        }
    }
}
