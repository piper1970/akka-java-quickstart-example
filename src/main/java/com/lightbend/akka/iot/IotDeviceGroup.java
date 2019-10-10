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

public class IotDeviceGroup extends AbstractActor {

    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

    public static Props props(String groupId) {
        return Props.create(IotDeviceGroup.class, () -> new IotDeviceGroup(groupId));
    }

    private final String groupId;

    private IotDeviceGroup(String groupId) {
        this.groupId = groupId;
    }

    public static final class RequestDeviceList {
        final long requestId;

        public RequestDeviceList(long requestId) {
            this.requestId = requestId;
        }
    }

    public static final class ReplyDeviceList {
        final long requestId;
        final Set<String> ids;

        ReplyDeviceList(long requestId, Set<String> ids) {
            this.requestId = requestId;
            this.ids = ids;
        }
    }

    public static final class RequestAllTemperatures {
        final long requestId;

        public RequestAllTemperatures(long requestId) {
            this.requestId = requestId;
        }
    }

    public static final class RespondAllTemperatures {
        final long requestId;
        final Map<String, TemperatureReading> temperatures;

        RespondAllTemperatures(long requestId, Map<String, TemperatureReading> temperatures) {
            this.requestId = requestId;
            this.temperatures = temperatures;
        }
    }

    public interface TemperatureReading {
    }

    public static final class Temperature implements TemperatureReading {
        final double value;

        public Temperature(double value) {
            this.value = value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Temperature that = (Temperature) o;
            return Double.compare(that.value, value) == 0;
        }

        @Override
        public int hashCode() {
            return Objects.hash(value);
        }

        @Override
        public String toString() {
            return "Temperature{" +
                    "value=" + value +
                    '}';
        }
    }

    public enum TemperatureNotAvailable implements TemperatureReading {
        INSTANCE
    }

    public enum DeviceNotAvailable implements TemperatureReading {
        INSTANCE
    }

    public enum DeviceTimedOut implements TemperatureReading {
        INSTANCE
    }

    private final Map<String, ActorRef> deviceIdToActor = new HashMap<>();
    private final Map<ActorRef, String> actorToDeviceId = new HashMap<>();

    @Override
    public void preStart() {
        log.info("IotDeviceGroup-{} started", groupId);
    }

    @Override
    public void postStop() {
        log.info("IotDeviceGroup-{} stopped", groupId);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(IotDeviceManager.RequestTrackDevice.class, this::onTrackDevice)
                .match(RequestDeviceList.class, this::onDeviceList)
                .match(Terminated.class, this::onTerminated)
                .match(RequestAllTemperatures.class, this::onAllTemperatures)
                .build();
    }

    private void onAllTemperatures(RequestAllTemperatures r) {
        Map<ActorRef, String> actorToDeviceIdCopy = new HashMap<>(actorToDeviceId);
        getContext().actorOf(IotDeviceGroupQuery.props(
                actorToDeviceIdCopy, r.requestId, getSender(), new FiniteDuration(3, TimeUnit.SECONDS)
        ));
    }

    private void onTrackDevice(IotDeviceManager.RequestTrackDevice trackMsg) {
        if (groupId.equals(trackMsg.groupId)) {
            Optional.ofNullable(deviceIdToActor.getOrDefault(trackMsg.deviceId, null))
                    .ifPresentOrElse(deviceActor -> deviceActor.forward(trackMsg, getContext()), () -> {
                        log.info("Creating IotDevice actor for {}", trackMsg.deviceId);
                        ActorRef deviceActor = getContext().actorOf(IotDevice.props(groupId, trackMsg.deviceId), "iotDevice-" + trackMsg.deviceId);
                        getContext().watch(deviceActor);
                        actorToDeviceId.put(deviceActor, trackMsg.deviceId);
                        deviceIdToActor.put(trackMsg.deviceId, deviceActor);
                        deviceActor.forward(trackMsg, getContext());
                    });
        } else {
            log.warning("Ignoring TrackDevice request for {}.  This actor is responsible for {}.",
                    groupId, this.groupId);
        }
    }

    private void onDeviceList(RequestDeviceList r) {
        getSender().tell(new ReplyDeviceList(r.requestId, deviceIdToActor.keySet()), getSelf());
    }

    private void onTerminated(Terminated t) {
        ActorRef ref = t.getActor();
        Optional.ofNullable(actorToDeviceId.get(ref))
                .ifPresent(deviceId -> {
                    log.info("Iot Device actor for {} has been terminated", deviceId);
                    actorToDeviceId.remove(ref);
                    deviceIdToActor.remove(deviceId);
                });
    }
}
