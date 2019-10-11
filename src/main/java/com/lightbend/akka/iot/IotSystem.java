package com.lightbend.akka.iot;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import java.io.IOException;

@SuppressWarnings("ResultOfMethodCallIgnored")
public class IotSystem {

    private final ActorSystem system;
    private final ActorRef supervisor;

    public static void main(String[] args) throws IOException {
        IotSystem system = new IotSystem("iotSystem");
        try {
            // Do something meaningful....
            System.out.println("Press Enter to exit the system");
            System.in.read();
        } finally {
            system.terminate();
        }

    }

    private IotSystem(String systemName) {
        String supervisorName = systemName + "-supervisor";
        system = ActorSystem.create(systemName);
        supervisor = system.actorOf(IotSupervisor.props(), supervisorName);
    }

    private void terminate() {
        system.terminate();
    }
}
