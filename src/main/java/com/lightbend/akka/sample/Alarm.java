package com.lightbend.akka.sample;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.pf.ReceiveBuilder;

import java.util.concurrent.atomic.AtomicBoolean;

// Simple example of context switching logic between states of the alarm.
@SuppressWarnings("unused")
public class Alarm extends AbstractActor {
    private final LoggingAdapter logger = Logging.getLogger(getContext().getSystem(), this);

    private final AtomicBoolean enabled = new AtomicBoolean(false);

    public static Props props(String password){
        return Props.create(Alarm.class, password);
    }

    // Messages
    private static final class Activity{}
    static final class Disable{
        private final String password;

        public Disable(String password) {
            this.password = password;
        }
    }
    static final class Enable{
        private final String password;

        public Enable(String password) {
            this.password = password;
        }
    }

    private final String password;

    private Alarm(String password) {
        this.password = password;
    }

    @Override
    public Receive createReceive() {
        return disabledReceive();
    }

    private Receive enabledReceive() {
        return new ReceiveBuilder()
                .match(Disable.class, this::onDisable)
                .match(Activity.class, this::onActivity)
                .build();
    }

    private Receive disabledReceive(){
        return new ReceiveBuilder()
                .match(Enable.class, this::onEnable)
                .build();
    }

    private void onEnable(Enable msg) {
        if(msg.password.equals(password)){
            enabled.compareAndExchange(false, true);
            getContext().become(enabledReceive());
            logger.info("Enabling alarm");
        }else{
            logger.warning("Password incorrect. Cannot enable");
        }
    }

    private void onActivity(Activity msg) {
        if(enabled.get()){
            logger.warning("Activity detected...initiating self destructing in 60 seconds unless alarm is disabled");
            // do something really cool!
        }else{
            logger.warning("Sorry, activity detected, but you forgot to set the alarm.  Not my fault!");
            // kiss your beholdings goodbye
        }

    }

    private void onDisable(Disable msg) {
        if(msg.password.equals(password)){
            enabled.compareAndExchange(true, false);
            getContext().become(disabledReceive());
            logger.info("Disabling alarm");
        }else{
            logger.warning("Password incorrect. Cannot disable");
        }
    }
}
