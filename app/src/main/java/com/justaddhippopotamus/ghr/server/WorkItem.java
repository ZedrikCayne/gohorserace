package com.justaddhippopotamus.ghr.server;

import com.justaddhippopotamus.ghr.RESP.RESPArray;
import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
import com.justaddhippopotamus.ghr.server.types.RedisType;

import java.util.List;

public class WorkItem {
    public Client whoFor;
    public RESPArray what;
    public long order;

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
    }

    public WorkItem() {
        this.what = null;
        this.whoFor = null;
        this.order = 0;
    }

    @Override
    public String toString() {
        if( what != null ) {
            StringBuilder sb = new StringBuilder();
            sb.append(whoFor.hashCode());
            sb.append(": ");
            if( what.argAtIs(0,"AUTH") ) {
                sb.append("AUTH ***** *****");
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
    public List<RedisType> blockedOn = null;

}
