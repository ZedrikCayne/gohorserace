
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.IRESP;
import com.justaddhippopotamus.ghr.RESP.RESPArray;
import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.Command;
import com.justaddhippopotamus.ghr.server.WorkItem;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandHelloCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        //HELLO [protover [AUTH username password] [SETNAME clientname]]
        final RESPArrayScanner commands = item.scanner();
        IRESP.RESPVersion rv = IRESP.RESPVersion.RESP2;
        if( commands.hasNext() ) {
            int version = commands.takeInt();
            if( version != 2 && version != 3 ) {
                item.whoFor.queueSimpleError("No idea what RESP version you want.", item.order);
                return;
            }
            if( version == 3 ) {
                rv = IRESP.RESPVersion.RESP3;
            } else {
                rv = IRESP.RESPVersion.RESP2;
            }

            if( commands.argIs("AUTH") ) {
                String username = commands.string();
                String password = commands.string();
            }

            if( commands.argIs("SETNAME") ) {
                String name = commands.string();
            }
        }
        commands.errorOnRemains();
        item.whoFor.clientRESPVersion = rv;
        item.whoFor.queue(item.whoFor.helloResponse(),item.order);
    }
}
