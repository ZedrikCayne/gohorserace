package com.justaddhippopotamus.ghr.server;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class ChannelManager {
    ConcurrentHashMap<String,PubSubChannel> channels = new ConcurrentHashMap<>();
    ConcurrentHashMap<String,PatternHolder> patterns = new ConcurrentHashMap<>();

    private static PatternHolder RESET = new PatternHolder("RESET");
    private static PubSubChannel RESET_PSC = new PubSubChannel("RESET",null);

    public synchronized int numPatterns() {
        int total = 0;
        for (PatternHolder patternHolder : patterns.values()) {
            total += patternHolder.numClients();
        }
        return total;
    }

    synchronized public PatternHolder getPattern(String pattern) {
        if( pattern.compareTo("RESET") == 0 ) return RESET;
        if( !patterns.containsKey(pattern) ) {
            PatternHolder ph = new PatternHolder(pattern);
            channels.values().stream().filter(c->ph.matches(c.getName())).forEach(c->ph.addChannel(c));
            patterns.put(pattern,ph);
        }
        return patterns.get(pattern);
    }

    synchronized public PubSubChannel getChannel(String name) {
        if( name.compareTo("RESET") == 0 ) return RESET_PSC;
        if( !channels.containsKey(name) ) {
            PubSubChannel psc = new PubSubChannel(name,this);
            channels.put(name,psc);
            for( PatternHolder ph : patterns.values() )
                if( ph.matches(name) )
                    ph.addChannel(psc);
            psc.goDogGo();
        }
        return channels.get(name);
    }

    synchronized public void removeChannel(String name) {
        if( channels.containsKey(name) ) {
            PubSubChannel psc = channels.remove(name);
            patterns.values().forEach(pat->pat.removeChannel(psc));
        }
    }

    public List<String> channelNames() {
        List<String> returnValue = new ArrayList<>();
        channels.forEach((k,v)-> { if(!v.hasNoClients()) returnValue.add(k);});
        return returnValue;
    }
}
