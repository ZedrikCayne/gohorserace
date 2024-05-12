
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArray;
import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.Command;
import com.justaddhippopotamus.ghr.server.RedisCursor;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisHash;
import com.justaddhippopotamus.ghr.server.types.RedisSet;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandSscanCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        //SSCAN key cursor [MATCH pattern] [COUNT count]
        RESPArrayScanner commands = item.scanner();
        String key = commands.key();
        long cursorId = commands.takeLong();
        String pattern = commands.argIs("MATCH")?commands.string():null;
        int count = commands.argIs("COUNT")?commands.takeInt():null;
        RedisSet rs = null;
        if( cursorId == 0 ) {
            rs = item.getMainStorage().fetchRO(key, RedisSet.class);
            if( rs == null ) {
                item.whoFor.queueSimpleError("No such hash.", item.order);
                return;
            }
        }
        RedisCursor rc = cursorId==0?new RedisCursor(rs,item.whoFor.getNewCursorId(),pattern):item.whoFor.getCursor(cursorId);
        if( rc == null ) {
            item.whoFor.queueSimpleError("No such cursor.", item.order);
            return;
        }
        if( cursorId == 0 ) {
            item.whoFor.populateCursor(rc);
        }

        RESPArray ra = rc.scanSet(count,pattern);
        if(rc.isDone()) item.whoFor.killCursor(rc.getCursorId());
        item.whoFor.queue(ra,item.order);
    }
}
