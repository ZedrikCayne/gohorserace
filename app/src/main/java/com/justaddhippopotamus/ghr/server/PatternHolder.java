package com.justaddhippopotamus.ghr.server;

import com.justaddhippopotamus.ghr.RESP.RESPBulkString;
import com.justaddhippopotamus.ghr.RESP.RESPPush;

import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;

public class PatternHolder {
    String pattern;
    RESPBulkString bulkPattern;
    PathMatcher matcher;
    Set<Client> clients;
    Set<PubSubChannel> channels;

    public synchronized void addChannel(PubSubChannel psc) {
        channels.add(psc);
        psc.partOfPattern(this);
    }

    public synchronized int numClients() {
        return clients.size();
    }

    public synchronized void removeChannel(PubSubChannel psc) {
        channels.remove(psc);
        psc.removeFromPattern(this);
    }

    public synchronized void addClient(Client c) {
        clients.add(c);
        c.queuePubSubSubscribe(bulkPattern,true);
    }

    public synchronized void removeClient(Client c) {
        clients.remove(c);
        c.queuePubSubUnsubscribe(bulkPattern,true);
    }

    public synchronized void broadcast (RESPBulkString channel, RESPBulkString message) {
        clients.stream().forEach(c->c.queuePubSubMessage(channel,message));
    }

    public boolean matches(String name) {
        return matcher.matches(Paths.get(name));
    }

    public PatternHolder(String pattern) {
        this.pattern = pattern;
        bulkPattern = new RESPBulkString(pattern);
        matcher = Utils.forGlob(pattern);
        clients = new HashSet<>();
        channels = new HashSet<>();
    }
}
