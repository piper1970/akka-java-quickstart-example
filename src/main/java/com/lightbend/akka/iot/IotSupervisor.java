package com.lightbend.akka.iot;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class IotSupervisor extends AbstractActor {

    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

    private final String supervisorId;
    private final Map<String, ActorRef> managerIdToActor = new HashMap<>();
    private final Map<ActorRef, String> actorToManagerId = new HashMap<>();

    private IotSupervisor(String supervisorId) {
        this.supervisorId = supervisorId;
    }

    public static Props props(String supervisorId) {
        return Props.create(IotSupervisor.class, () -> new IotSupervisor(supervisorId) );
    }

    @Override
    public void preStart() {
        log.info("Iot Supervisor started");
    }

    @Override
    public void postStop() {
        log.info("Iot Supervisor stopped");
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(RequestDeviceManagerList.class, this::onRequestDeviceManagerList)
                .match(TrackDeviceManager.class, this::onTrackDeviceManager)
                .build();
    }

    private void onTrackDeviceManager(TrackDeviceManager trackDeviceManager) {
        String deviceManagerId = trackDeviceManager.deviceManagerId;
        Optional.ofNullable(managerIdToActor.get(deviceManagerId))
                .ifPresentOrElse(ref -> ref.forward(trackDeviceManager, getContext()),
                        () -> trackNewDeviceManager(trackDeviceManager, deviceManagerId));
    }

    private void trackNewDeviceManager(TrackDeviceManager trackDeviceManager, String deviceManagerId) {
        log.info("Creating device manager for {}", deviceManagerId);
        ActorRef managerActor = getContext().actorOf(IotDeviceManager.props(supervisorId, deviceManagerId), "iotDeviceManager-" + deviceManagerId);
        getContext().watch(managerActor);
        managerActor.forward(trackDeviceManager, getContext());
        managerIdToActor.put(deviceManagerId, managerActor);
        actorToManagerId.put(managerActor, deviceManagerId);
    }

    private void onRequestDeviceManagerList(RequestDeviceManagerList requestDeviceManagerList) {
        getSender().tell(new ReplyDeviceManagerList(requestDeviceManagerList.requestId,
                managerIdToActor.keySet()), getSelf());
    }

    public static final class RequestDeviceManagerList{
        final long requestId;

        public RequestDeviceManagerList(long requestId) {
            this.requestId = requestId;
        }
    }

    public static final class ReplyDeviceManagerList{
        final long requestId;
        final Set<String> ids;

        public ReplyDeviceManagerList(long requestId, Set<String> ids) {
            this.requestId = requestId;
            this.ids = ids;
        }
    }

    public static final class TrackDeviceManager{
        final long requestId;
        final String deviceManagerId;

        public TrackDeviceManager(long requestId, String deviceManagerId) {
            this.requestId = requestId;
            this.deviceManagerId = deviceManagerId;
        }
    }

    public static final class DeviceManagerRegistered{
        final long requestId;

        public DeviceManagerRegistered(long requestId) {
            this.requestId = requestId;
        }
    }
}
