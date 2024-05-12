
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArray;
import com.justaddhippopotamus.ghr.RESP.RESPBulkString;
import com.justaddhippopotamus.ghr.RESP.RESPInteger;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.Client;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisSortedSet;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandZrankCommand extends ICommandImplementation {
    //ZRANK key member [WITHSCORE]
    @Override
    public void runCommand(WorkItem item) {
        genericZrank(item.what,item.whoFor,item.order, false);
    }
    public static void genericZrank(RESPArray commands, Client client, long order, boolean reverse) {
        String key = commands.stringAt(1);
        String member = commands.stringAt(2);
        boolean WITHSCORE = commands.argAtMaybeIs(3,"WITHSCORE");
        RedisSortedSet rss = client.getMainStorage().fetchRO(key, RedisSortedSet.class);
        if( rss == null ) {
            client.queueNullArray(order);
        } else {
            var vs = rss.rank(member,reverse);
            if( vs == null ) {
                client.queueNullArray(order);
            } else {
                if( WITHSCORE ) {
                    RESPArray ra = new RESPArray();
                    ra.addRespElement(new RESPInteger(vs.rank));
                    ra.addRespElement(new RESPBulkString(vs.score));
                    client.queue(ra,order);
                } else {
                    client.queueInteger(vs.rank,order);
                }
            }
        }
    }
}
