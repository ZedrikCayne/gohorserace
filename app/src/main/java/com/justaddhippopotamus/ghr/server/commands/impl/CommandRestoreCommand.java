
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
import com.justaddhippopotamus.ghr.RESP.RESPBulkString;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.Command;
import com.justaddhippopotamus.ghr.server.RDB;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisType;

import java.io.ByteArrayInputStream;
import java.io.IOException;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandRestoreCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        //RESTORE key ttl serialized-value [REPLACE] [ABSTTL]
        //  [IDLETIME seconds] [FREQ frequency]
        RESPArrayScanner commands = item.scanner();
        String key = commands.key();
        long ttl = commands.takeLong();
        RESPBulkString sValue = commands.bulkString();
        boolean REPLACE = commands.argIs("REPLACE");
        boolean ABSTTL = commands.argIs("ABSTTL");
        boolean IDLETIME = commands.argIs("IDLETIME");
        long idleTime = IDLETIME?commands.takeLong():0;
        boolean FREQUENCY = commands.argIs("FREQ");
        long frequency = FREQUENCY?commands.takeLong():0;
        ByteArrayInputStream is = new ByteArrayInputStream(sValue.value);
        if( !REPLACE && item.getMainStorage().keyExists(key) ) {
            item.whoFor.queueSimpleError("Target key name is busy", item.order);
            return;
        }
        try {
            RedisType rt = RDB.loadObject(RDB.loadType(is),is);
            rt.expireAtMilliseconds = ABSTTL?ttl:ttl*1000;
            item.getMainStorage().store(key,rt);
        } catch (IOException e ) {
            throw new RuntimeException("Error deserializing a RESTORED object.");
        }
        item.whoFor.queueOK(item.order);
    }
}
