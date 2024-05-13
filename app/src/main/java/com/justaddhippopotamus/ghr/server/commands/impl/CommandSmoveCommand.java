
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.Command;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisSet;

import java.nio.file.Paths;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandSmoveCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        //SMOVE source destination member
        RESPArrayScanner commands = item.scanner();
        String source = commands.key();
        String destination = commands.key();
        String member = commands.string();
        RedisSet src = item.getMainStorage().fetchRW(source,RedisSet.class);
        if( src == null ) {
            item.whoFor.queueInteger(0,item.order);
            return;
        }
        src.atomic( (RedisSet s) -> {
            if( !s.contains(member) ) {
                item.whoFor.queueInteger(0,item.order);
                return;
            }
            RedisSet dst = item.getMainStorage().fetchRW(destination,RedisSet.class);
            if( dst == null ) {
                item.whoFor.queueSimpleError("Destination does not exist.", item.order);
            } else {
                dst.atomic((RedisSet d) -> {
                    s.remove(member);
                    if( d.contains(member) ) {
                        item.whoFor.queueInteger(0,item.order);
                        return;
                    } else {
                        d.add(member);
                        item.whoFor.queueInteger(1,item.order);
                    }
                });
            }
        });
    }
}
