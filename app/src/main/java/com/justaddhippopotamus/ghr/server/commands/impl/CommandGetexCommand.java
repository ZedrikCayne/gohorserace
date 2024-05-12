
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArray;
import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisString;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandGetexCommand extends ICommandImplementation {
    //GETEX key [EX seconds | PX milliseconds | EXAT unix-time-seconds |   PXAT unix-time-milliseconds | PERSIST]
    @Override
    public void runCommand(WorkItem item) {
        final RESPArrayScanner commands = item.scanner();
        String key = commands.string();
        String action = commands.argOneOf("EX","PEX","EXAT","PXAT","PERSIST" );
        long millisecondTimeout = -1;
        long multiplier = 1;
        long addMe = 0;
        boolean setExpiry = true;
        switch (action) {
            case "EX":
                multiplier = 1000L;
            case "PEX":
                addMe = System.currentTimeMillis();
                break;
            case "EXAT":
                multiplier = 1000L;
            case "PXAT":
                addMe = 0;
                break;
            case "PERSIST":
                multiplier = 0;
                millisecondTimeout = 0;
                break;
            default:
                multiplier = 0;
                setExpiry = false;
        }
        if( multiplier != 0 ) {
            millisecondTimeout = ( commands.takeLong() * multiplier ) + addMe;
        }
        commands.errorOnRemains();
        RedisString rs = item.getMainStorage().fetchRW(key, RedisString.class);
        if( rs != null && setExpiry ) {
            rs.setExpireMilliseconds(millisecondTimeout,false,false,false,false);
        }
        item.whoFor.queue(rs,item.order);
    }
}
