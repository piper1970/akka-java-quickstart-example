package com.lightbend.akka.sample;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.Terminated;
import akka.event.Logging;
import akka.event.LoggingAdapter;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class IotDeviceManager extends AbstractActor {
    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

    public static Props props() {
        return Props.create(IotDeviceManager.class, IotDeviceManager::new);
    }

    public static final class RequestTrackDevice {
        final String groupId;
        final String deviceId;

        public RequestTrackDevice(String groupId, String deviceId) {
            this.groupId = groupId;
            this.deviceId = deviceId;
        }
    }

    public static final class DeviceRegistered {
    }

    private final Map<String, ActorRef> groupIdToActor = new HashMap<>();
    private final Map<ActorRef, String> actorToGroupId = new HashMap<>();

    @Override
    public void preStart() {
        log.info("IotDeviceManager started.");
    }

    @Override
    public void postStop() {
        log.info("IotDeviceManager stopped.");
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(RequestTrackDevice.class, this::onTrackDevice)
                .match(Terminated.class, this::onTerminated)
                .build();
    }

    private void onTrackDevice(RequestTrackDevice trackMsg) {
        String groupId = trackMsg.groupId;
        Optional.ofNullable(groupIdToActor.getOrDefault(groupId, null))
                .ifPresentOrElse(ref -> ref.forward(trackMsg, getContext()), () -> {
                    log.info("Creating device group actor for {}", groupId);
                    ActorRef groupActor = this.getContext().actorOf(IotDeviceGroup.props(groupId), "iotGroup-" + groupId);
                    getContext().watch(groupActor);
                    groupActor.forward(trackMsg, this.getContext());
                    groupIdToActor.put(groupId, groupActor);
                    actorToGroupId.put(groupActor, groupId);
                });
    }

    private void onTerminated(Terminated t) {
        ActorRef groupActor = t.getActor();
        Optional.ofNullable(actorToGroupId.getOrDefault(groupActor, null))
                .ifPresent(groupId -> {
                    log.info("Device group actor for {} has been terminated", groupId);
                    actorToGroupId.remove(groupActor);
                    groupIdToActor.remove(groupId);
                });
    }

}
