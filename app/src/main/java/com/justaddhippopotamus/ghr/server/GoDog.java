package com.justaddhippopotamus.ghr.server;


public abstract class GoDog implements Runnable {
    public Thread goDogGo() {
        if( myThread == null ) {
            myThread = new Thread(this);
            myThread.start();
        }
        else {
            System.out.println("Tried to start a " + getClass().getSimpleName() + " twice.");
        }
        return myThread;
    }

    public void run() {
        running = true;
    }
    
    public boolean mainLoopCompleted() {
        boolean wasLoopDone = mainLoopDone;
        mainLoopDone = true;
        if( running ) {
            System.out.println("Main loop of " + getClass().getSimpleName() + " thinks it is still running when it got to the end of main loop.");
        }
        if( wasLoopDone ) {
            System.out.println("Main loop already said it was done.");
        }
        return wasLoopDone;
    }

    public void stopIfNecessary() {
        if( running ) stop();
    }

    public boolean stop() {
        boolean returnValue = running;
        if( running ) {
            running = false;
        }
        return returnValue;
    }
    public Thread myThread = null;
    public boolean running = false;
    public boolean mainLoopDone = false;
}
