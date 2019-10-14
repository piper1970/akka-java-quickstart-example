package com.lightbend.akka.iot;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.Terminated;
import akka.event.Logging;
import akka.event.LoggingAdapter;

import java.util.*;

public class IotDeviceManager extends AbstractActor {
    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
    private final String managerId;
    private final Map<String, ActorRef> groupIdToActor = new HashMap<>();
    private final Map<ActorRef, String> actorToGroupId = new HashMap<>();

    private IotDeviceManager(String managerId) {
        this.managerId = managerId;
    }

    public static Props props(String managerId) {
        return Props.create(IotDeviceManager.class, () -> new IotDeviceManager(managerId));
    }

    // TODO: need to add getGroupList functionality, like in IotDeviceGroup getDeviceList
    public static final class RequestGroupList {
        final long requestId;
        final String deviceManagerId;

        public RequestGroupList(long requestId, String deviceManagerId) {
            this.requestId = requestId;
            this.deviceManagerId = deviceManagerId;
        }
    }

    public static final class ReplyGroupList {
        final long requestId;
        final Set<String> groupList;

        public ReplyGroupList(long requestId, Set<String> groupList) {
            this.requestId = requestId;
            this.groupList = groupList;
        }
    }

    public static final class RequestTrackDevice {
        final String groupId;
        final String deviceId;

        public RequestTrackDevice(String groupId, String deviceId) {
            this.groupId = groupId;
            this.deviceId = deviceId;
        }
    }

    public static final class RequestDeviceGroupById{
        public final long requestId;
        public final String deviceGroupId;

        public RequestDeviceGroupById(long requestId, String deviceGroupId) {
            this.requestId = requestId;
            this.deviceGroupId = deviceGroupId;
        }
    }

    public static final class RespondDeviceGroupById{
        public final long requestId;
        public final ActorRef deviceGroupActor;

        public RespondDeviceGroupById(long requestId, ActorRef deviceGroupActor) {
            this.requestId = requestId;
            this.deviceGroupActor = deviceGroupActor;
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
                .match(RequestDeviceGroupById.class, this::onRequestDeviceGroupById)
                .match(Terminated.class, this::onTerminated)
                .match(RequestGroupList.class, this::onRequestGroupList)
                .build();
    }

    private <P> void onRequestDeviceGroupById(RequestDeviceGroupById msg) {
        Optional.ofNullable(groupIdToActor.get(msg.deviceGroupId))
                .ifPresentOrElse(actor -> {
                    log.info("Getting device group {} for sender with request", msg.deviceGroupId, msg.requestId);
                    getSender().tell(new RespondDeviceGroupById(msg.requestId, actor), getSelf());
                }, () -> log.warning("Device group {} not managed by this actor", msg.deviceGroupId));
    }

    private void onRequestGroupList(RequestGroupList requestGroupList) {
        if(managerId.equals(requestGroupList.deviceManagerId)){
            getSender().tell(new ReplyGroupList(requestGroupList.requestId, groupIdToActor.keySet()), getSelf());
        }else{
            log.warning("Ignoring RequestGroupList call for {}.  This device manager handles calls for {}",
                    requestGroupList.deviceManagerId, managerId);
        }
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
                    getContext().unwatch(groupActor);
                });
    }

}
