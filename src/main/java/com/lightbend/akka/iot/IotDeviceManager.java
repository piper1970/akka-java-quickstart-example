package com.lightbend.akka.iot;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.Terminated;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import scala.concurrent.duration.FiniteDuration;

import java.util.*;
import java.util.concurrent.TimeUnit;

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

    public interface DeviceGroupTemperatureReading{}

    public enum DeviceGroupNotAvailable implements DeviceGroupTemperatureReading{
        INSTANCE
    }

    public enum DeviceGroupTimedOut implements DeviceGroupTemperatureReading{
        INSTANCE
    }

    public static final class DeviceGroupTemperatures implements DeviceGroupTemperatureReading{
        public final long requestId;
        public final Map<String, IotDeviceGroup.TemperatureReading> groupTemperatureReading;

        public DeviceGroupTemperatures(long requestId, Map<String, IotDeviceGroup.TemperatureReading> groupTemperatureReading) {
            this.requestId = requestId;
            this.groupTemperatureReading = groupTemperatureReading;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DeviceGroupTemperatures that = (DeviceGroupTemperatures) o;
            return requestId == that.requestId &&
                    Objects.equals(groupTemperatureReading, that.groupTemperatureReading);
        }

        @Override
        public int hashCode() {
            return Objects.hash(requestId, groupTemperatureReading);
        }

        @Override
        public String toString() {
            return "DeviceGroupTemperatures{" +
                    "requestId=" + requestId +
                    ", groupTemperatureReading=" + groupTemperatureReading +
                    '}';
        }
    }

    public static final class RequestAllGroupTemperatures{
        final long requestId;

        public RequestAllGroupTemperatures(long requestId) {
            this.requestId = requestId;
        }
    }

    public static final class RespondAllGroupTemperatures{
        final long requestId;
        final Map<String, DeviceGroupTemperatureReading> groupTemperatures;

        public RespondAllGroupTemperatures(long requestId, Map<String, DeviceGroupTemperatureReading> groupTemperatures) {
            this.requestId = requestId;
            this.groupTemperatures = groupTemperatures;
        }
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
                .match(RequestAllGroupTemperatures.class, this::onRequestAllGroupTemperatures)
                .build();
    }

    private void onRequestAllGroupTemperatures(RequestAllGroupTemperatures msg) {
        log.info("Requesting all group temperatures for request {}", msg.requestId);
        Map<ActorRef, String> newActorToGroupId = new HashMap<>(actorToGroupId);
        getContext().actorOf(IotDeviceManagerQuery.props(newActorToGroupId, msg.requestId,
                getSender(), new FiniteDuration(30, TimeUnit.SECONDS)));
    }

    private void onRequestDeviceGroupById(RequestDeviceGroupById msg) {
        log.info("Requestiong device group by id for device {} on request {}", msg.deviceGroupId, msg.requestId);
        Optional.ofNullable(groupIdToActor.get(msg.deviceGroupId))
                .ifPresentOrElse(actor -> {
                    log.info("Getting device group {} for sender with request", msg.deviceGroupId, msg.requestId);
                    getSender().tell(new RespondDeviceGroupById(msg.requestId, actor), getSelf());
                }, () -> log.warning("Device group {} not managed by this actor", msg.deviceGroupId));
    }

    private void onRequestGroupList(RequestGroupList requestGroupList) {
        log.info("Requesting group list for device manager {} on request id {}", requestGroupList.deviceManagerId, requestGroupList.requestId);
        if(managerId.equals(requestGroupList.deviceManagerId)){
            getSender().tell(new ReplyGroupList(requestGroupList.requestId, groupIdToActor.keySet()), getSelf());
        }else{
            log.warning("Ignoring RequestGroupList call for {}.  This device manager handles calls for {}",
                    requestGroupList.deviceManagerId, managerId);
        }
    }

    private void onTrackDeviceManager(IotSupervisor.TrackDeviceManager trackDeviceManager) {
        log.info("Requesting to track device manager {} on request {}", trackDeviceManager.deviceManagerId, trackDeviceManager.requestId);
        if(managerId.equals(trackDeviceManager.deviceManagerId)){
            getSender().tell(new IotSupervisor.DeviceManagerRegistered(trackDeviceManager.requestId), getSelf());
        }else{
            log.warning("Ignoring TrackDeviceManager call for {}.  This DeviceManager handles calls for {}",
                    trackDeviceManager.deviceManagerId, managerId);
        }
    }

    private void onTrackDevice(RequestTrackDevice trackMsg) {
        log.info("Requesting to track device {} of group {} from device manager", trackMsg.deviceId, trackMsg.groupId);
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
