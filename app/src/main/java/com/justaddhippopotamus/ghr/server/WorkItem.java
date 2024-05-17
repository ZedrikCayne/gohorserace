package com.justaddhippopotamus.ghr.server;

import com.justaddhippopotamus.ghr.RESP.RESPArray;
import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;

public class WorkItem {
    public Client whoFor;
    public RESPArray what;
    public long order;
    public long timeout;

    public TypeStorage getMainStorage() { return whoFor.getMainStorage(); }
    public Server getServer() { return whoFor.myServer; }

    public RESPArrayScanner scanner() {
        return new RESPArrayScanner(what);
    }

    public RESPArrayScanner scanner(boolean subcommand) {
        return new RESPArrayScanner(what,subcommand);
    }

    public WorkItem(RESPArray what, Client whoFor, long order) {
        this.what = what;
        this.whoFor = whoFor;
        this.order = order;
        this.timeout = -1;
    }

    public WorkItem() {
        this.what = null;
        this.whoFor = null;
        this.order = 0;
        this.timeout = -1;
    }

    public synchronized boolean alreadyCompleted() {
        return this.order < 0;
    }

    @Override
    public String toString() {
        if( what != null ) {
            StringBuilder sb = new StringBuilder();
            sb.append(whoFor.hashCode());
            sb.append(": ");
            if( what.argAtIs(0,"AUTH") ) {
                sb.append("AUTH ***** *****");
            } if ( what.argAtIs( 0, "HELLO") ) {
                sb.append("HELLO ?");
            } else {
                int len = what.size();
                for (int i = 0; i < len; ++i) {
                    sb.append(what.rbsAt(i).prettyString());
                    sb.append(" ");
                }
                sb.deleteCharAt(sb.length() - 1);
            }
            return sb.toString();
        }
        return "NOP";
    }

    public synchronized void setTimeout( double t ) {
        if( timeout < 0 ) {
            if (t == 0.0D) timeout = 0;
            else timeout = System.currentTimeMillis() + (long) (1000 * t);
        }
    }

    public synchronized boolean timedOut() {
        return timeout > 0 && timeout > System.currentTimeMillis();
    }
}
