package com.lightbend.akka.sample;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.util.Timeout;
import scala.concurrent.Await;
import scala.concurrent.Future;

import java.time.Duration;

import static akka.pattern.Patterns.ask;

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
                .matchEquals("Show me the money", msg -> child.forward(msg, getContext()))
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
                .matchEquals("Show me the money", msg -> sender().tell(1_000_000, getSelf()))
                .matchEquals("fail", f -> {
                    System.out.println("supervised actor fails now");
                    throw new Exception("I failed!");
                })
                .build();
    }
}

@SuppressWarnings("ResultOfMethodCallIgnored")
public class ActorHierarchyExperiments {
    public static void main(String[] args) throws Exception {
        ActorSystem system = ActorSystem.create("testSystem");

        ActorRef firstRef = system.actorOf(PrintMyActorRefActor.props(), "first-actor");
        ActorRef firstStopperRef = system.actorOf(StartStopActor1.props(), "first");

        ActorRef supervisingActor = system.actorOf(SupervisingActor.props(), "supervising-actor");

        System.out.println("First: " + firstRef);
        firstRef.tell("printit", ActorRef.noSender());

        // TODO: NOTE THIS SHOWS HOW TO TALK TO ACTOR FROM OUTSIDE.. at least one way...
        Future<Object> future = ask(supervisingActor, "Show me the money", Timeout.create(Duration.ofSeconds(300)));
        Integer result = (Integer) Await.result(future, Timeout.create(Duration.ofSeconds(300)).duration());
        System.out.println("My current value is: " + result);


        // prestart,postStop, and child handling
        firstStopperRef.tell("stop", ActorRef.noSender());

        supervisingActor.tell("failChild", ActorRef.noSender());

        System.out.println(">>> Press ENTER to exit <<<");
        try {
            System.in.read();
        } finally {
            System.out.println("Terminating...");
            system.terminate();
        }
    }
}
