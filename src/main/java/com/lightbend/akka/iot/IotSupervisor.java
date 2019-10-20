package com.lightbend.akka.iot;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.Terminated;
import akka.event.Logging;
import akka.event.LoggingAdapter;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class IotSupervisor extends AbstractActor {

    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
    private final Map<String, ActorRef> managerIdToActor = new HashMap<>();
    private final Map<ActorRef, String> actorToManagerId = new HashMap<>();

    public static Props props() {
        return Props.create(IotSupervisor.class, IotSupervisor::new);
    }

    @Override
    public void preStart() {
        log.info("Iot Supervisor started");
    }

    @Override
    public void postStop() {
        log.info("Iot Supervisor stopped");
    }

    // Do something meaningful....
    // get device manager list
    // get device manager by list

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(RequestDeviceManagerList.class, this::onRequestDeviceManagerList)
                .match(TrackDeviceManager.class, this::onTrackDeviceManager)
                .match(RequestDeviceManagerById.class, this::onRequestDeviceManagerById)
                .match(Terminated.class, this::onTerminated)
                .build();
    }

    private void onRequestDeviceManagerById(RequestDeviceManagerById msg) {
        String deviceManagerById = msg.deviceManagerId;
        Optional.ofNullable(managerIdToActor.get(deviceManagerById))
                .ifPresentOrElse(actorRef -> {
                    log.info("Device manager {} requested", deviceManagerById);
                    getSender().tell(new ResponseDeviceManagerById(msg.requestId, actorRef), getSelf());
                }, () -> log.warning("Device manager {} not managed by this supervisor", deviceManagerById));
    }

    private void onTerminated(Terminated terminated) {
        ActorRef deviceManagerRef = terminated.getActor();
        Optional.ofNullable(actorToManagerId.get(deviceManagerRef))
                .ifPresent(id -> {
                    log.info("Terminating device manager with id {}", id);
                    managerIdToActor.remove(id);
                    actorToManagerId.remove(deviceManagerRef);
                    getContext().unwatch(deviceManagerRef);
                });
    }


    private void onTrackDeviceManager(TrackDeviceManager trackDeviceManager) {
        String deviceManagerId = trackDeviceManager.deviceManagerId;
        Optional.ofNullable(managerIdToActor.get(deviceManagerId))
                .ifPresentOrElse(ref -> ref.forward(trackDeviceManager, getContext()),
                        () -> trackNewDeviceManager(trackDeviceManager, deviceManagerId));
    }

    private void trackNewDeviceManager(TrackDeviceManager trackDeviceManager, String deviceManagerId) {
        log.info("Creating device manager for {}", deviceManagerId);
        ActorRef managerActor = getContext().actorOf(IotDeviceManager.props(deviceManagerId), "iotDeviceManager-" + deviceManagerId);
        getContext().watch(managerActor);
        managerActor.forward(trackDeviceManager, getContext());
        managerIdToActor.put(deviceManagerId, managerActor);
        actorToManagerId.put(managerActor, deviceManagerId);
    }

    private void onRequestDeviceManagerList(RequestDeviceManagerList requestDeviceManagerList) {
        getSender().tell(new ReplyDeviceManagerList(requestDeviceManagerList.requestId,
                managerIdToActor.keySet()), getSelf());
    }

    static final class RequestDeviceManagerList {
        final long requestId;

        RequestDeviceManagerList(long requestId) {
            this.requestId = requestId;
        }
    }

    static final class ReplyDeviceManagerList {
        final long requestId;
        final Set<String> ids;

        ReplyDeviceManagerList(long requestId, Set<String> ids) {
            this.requestId = requestId;
            this.ids = ids;
        }
    }

    static final class TrackDeviceManager {
        final long requestId;
        final String deviceManagerId;

        TrackDeviceManager(long requestId, String deviceManagerId) {
            this.requestId = requestId;
            this.deviceManagerId = deviceManagerId;
        }
    }

    static final class RequestDeviceManagerById {
        final long requestId;
        final String deviceManagerId;

        RequestDeviceManagerById(long requestId, String deviceManagerId) {
            this.requestId = requestId;
            this.deviceManagerId = deviceManagerId;
        }
    }

    static final class ResponseDeviceManagerById {
        final long requestId;
        final ActorRef deviceManager;

        ResponseDeviceManagerById(long requestId, ActorRef deviceManager) {
            this.requestId = requestId;
            this.deviceManager = deviceManager;
        }
    }

    static final class DeviceManagerRegistered {
        final long requestId;

        DeviceManagerRegistered(long requestId) {
            this.requestId = requestId;
        }
    }
}
