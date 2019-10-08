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

public class IotDeviceManager extends AbstractActor {
    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
    private final String supervisorId;
    private final String managerId;
    private final Map<String, ActorRef> groupIdToActor = new HashMap<>();
    private final Map<ActorRef, String> actorToGroupId = new HashMap<>();

    private IotDeviceManager(String supervisorId, String managerId) {
        this.managerId = managerId;
        this.supervisorId = supervisorId;
    }

    public static Props props(String supervisorId, String managerId) {
        return Props.create(IotDeviceManager.class, () -> new IotDeviceManager(supervisorId, managerId));
    }

    // TODO: need to add getGroupList functionality, like in IotDeviceGroup getDeviceList

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
                .match(IotSupervisor.TrackDeviceManager.class, this::onTrackDeviceManager)
                .match(Terminated.class, this::onTerminated)
                .build();
    }

    private void onTrackDeviceManager(IotSupervisor.TrackDeviceManager trackDeviceManager) {
        if(managerId.equals(trackDeviceManager.deviceManagerId)){
            getSender().tell(new IotSupervisor.DeviceManagerRegistered(trackDeviceManager.requestId), getSelf());
        }else{
            log.warning("Ignoring TrackDeviceManager call for {}.  This DeviceManager handles calls for {}",
                    trackDeviceManager.deviceManagerId, managerId);
        }
    }

    private void onTrackDevice(RequestTrackDevice trackMsg) {
        String groupId = trackMsg.groupId;
        Optional.ofNullable(groupIdToActor.get(groupId))
                .ifPresentOrElse(ref -> ref.forward(trackMsg, getContext()), () -> {
                    log.info("Creating device group actor for {}", groupId);
                    ActorRef groupActor = getContext().actorOf(IotDeviceGroup.props(groupId), "iotGroup-" + groupId);
                    getContext().watch(groupActor);
                    groupActor.forward(trackMsg, getContext());
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
