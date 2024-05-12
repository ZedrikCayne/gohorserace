
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArray;
import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.Client;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisString;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandSetCommand extends ICommandImplementation {
    public void genericSet(Client client, String key, long order, RedisString value, long milliseconds, boolean NX, boolean XX, boolean GET, boolean KEEPTTL) {
        if (milliseconds != 0) {
            value.setExpireMilliseconds(milliseconds,false, false, false, false);
        }
        if( GET ) {
            client.queue( client.getMainStorage().storeWithGet(key,value,KEEPTTL),order );
        }
        else {
            if (client.getMainStorage().store(key, value, NX, XX, KEEPTTL)) {
                client.queueNullBulkString(order);
            } else {
                client.queueOK(order);
            }
        }
    }
    //SET key value [NX | XX] [GET] [EX seconds | PX milliseconds | EXAT unix-time-seconds | PXAT unix-time-milliseconds | KEEPTTL]
    @Override
    public void runCommand(WorkItem item) {
        final RESPArrayScanner commands = item.scanner();
        String key = commands.key();
        RedisString value = commands.redisString();
        String NXorXX = commands.argOneOf("NX", "XX" );
        boolean NX = NXorXX.equals("NX"), XX = NXorXX.equals("XX");
        boolean GET = commands.argIs("GET");
        long milliseconds = 0;
        String expiry = commands.argOneOf("EX","PX","EXAT","PXAT");
        switch( expiry ) {
            case "EX":
                milliseconds = commands.takeLong() * 1000L + System.currentTimeMillis();
                break;
            case "PEX":
                milliseconds = commands.takeLong() + System.currentTimeMillis();
                break;
            case "EXAT":
                milliseconds = commands.takeLong() * 1000l;
                break;
            case "PXAT":
                milliseconds = commands.takeLong();
                break;
            default:
                break;
        }
        boolean KEEPTTL = commands.argIs("KEEPTTL");
        commands.errorOnRemains();
        genericSet(item.whoFor,key,item.order,value,milliseconds,NX,XX,GET,KEEPTTL);
    }
}
