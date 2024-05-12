
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArray;
import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.Command;
import com.justaddhippopotamus.ghr.server.RedisCursor;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.*;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandScanCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        //SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]
        RESPArrayScanner commands = item.scanner();
        long cursorId = commands.takeLong();
        String match = commands.argIs("MATCH")?commands.string():null;
        int count = commands.argIs("COUNT")?commands.takeInt():10;
        String type = commands.argIs("TYPE")?commands.string():null;
        Class filterType = null;
        if( type != null ) {
            switch (type) {
                case "string":
                    filterType = RedisString.class;
                    break;
                case "list":
                    filterType = RedisList.class;
                    break;
                case "set":
                    filterType = RedisSet.class;
                    break;
                case "zset":
                    filterType = RedisSortedSet.class;
                    break;
                case "hash":
                    filterType = RedisHash.class;
                    break;
                case "stream":
                    filterType = RedisStream.class;
                    break;
            }
        }

        RedisCursor rc = cursorId==0?new RedisCursor(item.getMainStorage(),item.whoFor.getNewCursorId(),match):item.whoFor.getCursor(cursorId);
        if( rc == null ) {
            item.whoFor.queueSimpleError("No such cursor.", item.order);
            return;
        }
        if( cursorId == 0 )
            item.whoFor.populateCursor(rc);

        RESPArray ra = rc.scanTable(count,match,filterType);

        if( rc.isDone() ) item.whoFor.killCursor(rc.getCursorId());

        item.whoFor.queue(ra,item.order);
    }
}
