package com.justaddhippopotamus.ghr.server;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class ChannelManager {
    ConcurrentHashMap<String,PubSubChannel> channels = new ConcurrentHashMap<>();
    ConcurrentHashMap<String,PatternHolder> patterns = new ConcurrentHashMap<>();

    synchronized public PatternHolder getPattern(String pattern) {
        if( !patterns.containsKey(pattern) ) {
            PatternHolder ph = new PatternHolder(pattern);
            channels.values().stream().filter(c->ph.matches(c.getName())).forEach(c->ph.addChannel(c));
            patterns.put(pattern,ph);
        }
        return patterns.get(pattern);
    }

    synchronized public PubSubChannel getChannel(String name) {
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
        returnValue.addAll(channels.keySet());
        return returnValue;
    }
}
