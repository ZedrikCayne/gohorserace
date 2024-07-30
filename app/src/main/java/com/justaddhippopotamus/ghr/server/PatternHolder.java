package com.justaddhippopotamus.ghr.server;

import com.justaddhippopotamus.ghr.RESP.RESPBulkString;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

public class PatternHolder {
    String currentPattern;
    RESPBulkString bulkPattern;
    Pattern pattern;
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
        return pattern.matcher(name).matches();
    }

    public PatternHolder(String currentPattern) {
        this.currentPattern = currentPattern;
        bulkPattern = new RESPBulkString(currentPattern);
        pattern = Utils.forGlob(currentPattern);
        clients = new HashSet<>();
        channels = new HashSet<>();
    }
}
