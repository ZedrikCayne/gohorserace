package com.justaddhippopotamus.ghr.server;

import com.justaddhippopotamus.ghr.RESP.RESPBulkString;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class PubSubChannel extends GoDog {
    @Override
    public void run() {
        super.run();
        while(running) {
            try {
                RESPBulkString nextMessage = messages.take();
                Set<Client> needsToRemove = clients.keySet().stream().filter(c -> c.queuePubSubMessage(bulkname,nextMessage)).collect(Collectors.toSet());
                needsToRemove.stream().forEach(c->clients.remove(c));
                patterns.values().stream().forEach(p -> p.broadcast(bulkname,nextMessage));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        mainLoopCompleted();
    }

    @Override
    public boolean stop() {
        boolean wasRunning = super.stop();
        if( wasRunning ) {
            manager.removeChannel(getName());
        }
        return wasRunning;
    }

    public PubSubChannel(String name,ChannelManager manager) {
        this.name = name;
        this.bulkname = new RESPBulkString(name);
        this.manager = manager;
    }

    public int postMessage(RESPBulkString message) {
        try {
            messages.put(message);
            return clients.size();
        } catch (InterruptedException e) {

        }
        return 0;
    }

    public synchronized void subscribe(Client who) {
        clients.put(who,1);
    }

    public synchronized void unsubscribe(Client who) {
        clients.remove(who);
        if( canBeRemoved() )
            stop();
    }

    public synchronized boolean canBeRemoved() {
        return( clients.size() == 0 && patterns.size() == 0 );
    }

    public synchronized boolean hasNoClients() {
        return clients.isEmpty();
    }

    public synchronized int numClients() {
        return clients.size();
    }

    public synchronized void partOfPattern(PatternHolder ph) {
        patterns.put(ph.pattern,ph);
    }

    public synchronized void removeFromPattern(PatternHolder ph) {
        patterns.remove(ph.pattern);
    }

    public String getName() {return name;}
    public RESPBulkString getBulkname() {return bulkname;}
    ChannelManager manager;
    private String name;
    private RESPBulkString bulkname;
    BlockingQueue<RESPBulkString> messages = new LinkedBlockingQueue<>();
    ConcurrentHashMap<Client,Integer> clients = new ConcurrentHashMap<>();
    ConcurrentHashMap<String,PatternHolder> patterns = new ConcurrentHashMap<>();

}
