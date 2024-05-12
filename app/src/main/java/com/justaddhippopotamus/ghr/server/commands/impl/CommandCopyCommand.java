
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArray;
import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.TypeStorage;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisType;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandCopyCommand extends ICommandImplementation {
    private static final int NOT_COPIED = 0;
    private static final int COPIED = 1;
    //COPY source destination [DB destination-db] [REPLACE]
    @Override
    public void runCommand(WorkItem item) {
        final RESPArrayScanner commands = item.scanner();
        String sourceKey = commands.key();
        String destinationKey = commands.key();
        String destinationDb = null;
        if( commands.argIs("DESTINATION") ) {
            destinationDb = commands.string();
        }
        boolean REPLACE = commands.argIs("REPLACE");
        commands.errorOnRemains();
        TypeStorage source = item.getMainStorage();
        TypeStorage destination;
        if( destinationDb != null ) {
            destination = item.whoFor.getStorage(destinationDb);
        } else {
            destination = source;
        }
        RedisType rt = source.fetchRO(sourceKey, RedisType.class);
        if( rt != null ) {
            RedisType rtCopy = rt.copy(RedisType.class);
            if( !destination.store(destinationKey,rtCopy,!REPLACE,false,false) ) {
                item.whoFor.queueInteger(COPIED,item.order);
                return;
            }
        }
        item.whoFor.queueInteger(NOT_COPIED,item.order);
    }
}
