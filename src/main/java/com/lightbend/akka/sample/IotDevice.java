package com.lightbend.akka.sample;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;

import java.util.Optional;

public class IotDevice extends AbstractActor {
    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

    public static Props props(String groupId, String deviceId) {
        return Props.create(IotDevice.class, () -> new IotDevice(groupId, deviceId));
    }

    private final String groupId;

    private final String deviceId;

    private IotDevice(String groupId, String deviceId) {
        this.groupId = groupId;
        this.deviceId = deviceId;
    }

    public static final class RecordTemperature {
        final long requestId;
        final double value;

        public RecordTemperature(long requestId, double value) {
            this.requestId = requestId;
            this.value = value;
        }
    }

    public static final class TemperatureRecorded {
        final long requestId;

        TemperatureRecorded(long requestId) {
            this.requestId = requestId;
        }
    }

    public static final class ReadTemperature {
        final long requestId;

        public ReadTemperature(long requestId) {
            this.requestId = requestId;
        }
    }

    public static final class RespondTemperature {
        final long requestId;
        final Optional<Double> value;

        RespondTemperature(long requestId, Optional<Double> value) {
            this.requestId = requestId;
            this.value = value;
        }
    }

    Optional<Double> lastTemperatureReading = Optional.empty();

    @Override
    public void preStart() {
        log.info("IotDevice actor {}-{} started", groupId, deviceId);
    }

    @Override
    public void postStop() {
        log.info("IotDevice actor {}-{} stopped", groupId, deviceId);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(IotDeviceManager.RequestTrackDevice.class, this::onRequestTrackDevice)
                .match(ReadTemperature.class, r ->
                        getSender()
                                .tell(new RespondTemperature(r.requestId, lastTemperatureReading), getSelf()))
                .match(RecordTemperature.class, r -> {
                    log.info("Recorded temperature reading {} with {}", r.value, r.requestId);
                    lastTemperatureReading = Optional.of(r.value);
                    getSender().tell(new TemperatureRecorded(r.requestId), getSelf());
                })
                .build();
    }

    private void onRequestTrackDevice(IotDeviceManager.RequestTrackDevice r) {
        if (this.groupId.equals(r.groupId) && this.deviceId.equals(r.deviceId)) {
            getSender().tell(new IotDeviceManager.DeviceRegistered(), getSelf());
        } else {
            log.warning("Ignoring TrackDevice request for {}-{}.  This actor is responsible for {}-{}",
                    r.groupId, r.deviceId, this.groupId, this.deviceId);
        }
    }
}
