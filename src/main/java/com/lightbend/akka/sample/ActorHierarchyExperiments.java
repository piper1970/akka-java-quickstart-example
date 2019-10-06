package com.lightbend.akka.sample;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;

import java.io.IOException;

class PrintMyActorRefActor extends AbstractActor {
    static Props props() {
        return Props.create(PrintMyActorRefActor.class, PrintMyActorRefActor::new);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .matchEquals("printit", p -> {
                    ActorRef secondRef = getContext().actorOf(Props.empty(), "second-actor");
                    System.out.println("Second: " + secondRef);
                })
                .build();
    }
}

class StartStopActor1 extends AbstractActor {
    static Props props() {
        return Props.create(StartStopActor1.class, StartStopActor1::new);
    }

    @Override
    public void preStart() {
        System.out.println("first started");
        getContext().actorOf(StartStopActor2.props(), "second");
    }

    @Override
    public void postStop() {
        System.out.println("first stopped");
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .matchEquals("stop", s ->
                        getContext().stop(getSelf()))
                .build();
    }
}

class StartStopActor2 extends AbstractActor {
    static Props props() {
        return Props.create(StartStopActor2.class, StartStopActor2::new);
    }

    @Override
    public void preStart() {
        System.out.println("second started");
    }

    @Override
    public void postStop() {
        System.out.println("second stopped");
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder().build();
    }
}

class SupervisingActor extends AbstractActor {
    static Props props() {
        return Props.create(SupervisingActor.class, SupervisingActor::new);
    }

    private ActorRef child = getContext().actorOf(SupervisedActor.props(), "supervised-actor");

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .matchEquals("failChild", f ->
                        child.tell("fail", getSelf()))
                .build();
    }
}

class SupervisedActor extends AbstractActor {
    static Props props() {
        return Props.create(SupervisedActor.class, SupervisedActor::new);
    }

    @Override
    public void preStart() {
        System.out.println("supervised actor started");
    }

    @Override
    public void postStop() {
        System.out.println("supervised actor stopped");
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .matchEquals("fail", f -> {
                    System.out.println("supervised actor fails now");
                    throw new Exception("I failed!");
                })
                .build();
    }
}

@SuppressWarnings("all")
public class ActorHierarchyExperiments {
    public static void main(String[] args) throws IOException {
        ActorSystem system = ActorSystem.create("testSystem");

        ActorRef firstRef = system.actorOf(PrintMyActorRefActor.props(), "first-actor");
        ActorRef firstStopperRef = system.actorOf(StartStopActor1.props(), "first");

        ActorRef supervisingActor = system.actorOf(SupervisingActor.props(), "supervising-actor");

        System.out.println("First: " + firstRef);
        firstRef.tell("printit", ActorRef.noSender());

        // prestart,postStop, and child handling
        firstStopperRef.tell("stop", ActorRef.noSender());

        supervisingActor.tell("failChild", ActorRef.noSender());

        System.out.println(">>> Press ENTER to exit <<<");
        try {
            System.in.read();
        } finally {
            system.terminate();
        }
    }
}
