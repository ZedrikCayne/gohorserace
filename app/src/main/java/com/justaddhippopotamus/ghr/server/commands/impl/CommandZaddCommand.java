
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArray;
import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisSortedSet;

import java.util.HashMap;
import java.util.Map;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandZaddCommand extends ICommandImplementation {
    //ZADD key [ NX | XX ] [GT | LT] [CH] [INCR] score member [score member ...]
    @Override
    public void runCommand(WorkItem item) {
        final RESPArrayScanner commands = item.scanner();
        String key = commands.key();
        Map<String,Double> stuffToAdd = new HashMap<>(commands.size());
        boolean NX = false, XX = false, GT = false, LT = false, CH = false, INCR = false;
        while( commands.hasNext() ) {
            String arg = commands.takeArg();
            switch( arg ) {
                case "NX":
                    NX = true;
                    break;
                case "XX":
                    XX = true;
                    break;
                case "GT":
                    GT = true;
                    break;
                case "LT":
                    LT = true;
                    break;
                case "CH":
                    CH = true;
                    break;
                case "INCR":
                    INCR = true;
                    break;
                default:
                    //Once we're off the end, rumble the pairs off.
                    commands.backUp();
                    while( commands.hasNext() ) {
                        double value = commands.takeDouble();
                        stuffToAdd.put(commands.string(),value);
                    }
                    break;
            }
        }
        RedisSortedSet ss = item.getMainStorage().fetchRW(key,RedisSortedSet.class,() -> new RedisSortedSet() );
        if( ss == null ) {
            throw new RuntimeException("Trying to set a list into a non list.");
        }
        if( INCR ) {
            if( stuffToAdd.size() > 0 )
                throw new RuntimeException("Too many things to increment at once.");
            item.whoFor.queueDouble(ss.incr(stuffToAdd),item.order);
        } else {
            item.whoFor.queueInteger(ss.add(stuffToAdd,NX,XX,GT,LT,CH),item.order);
        }
    }
}
