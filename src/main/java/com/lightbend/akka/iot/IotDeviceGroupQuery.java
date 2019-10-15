package com.lightbend.akka.iot;

import akka.actor.*;
import scala.concurrent.duration.FiniteDuration;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class IotDeviceGroupQuery extends AbstractActor {
    private static final class CollectionTimeout {
    }

    private final Map<ActorRef, String> actorToDeviceId;
    private final long requestId;
    private final ActorRef requester;
    private final Cancellable queryTimeoutTimer;

    private IotDeviceGroupQuery(Map<ActorRef, String> actorToDeviceId,
                                long requestId,
                                ActorRef requester,
                                FiniteDuration timout) {
        this.actorToDeviceId = actorToDeviceId;
        this.requestId = requestId;
        this.requester = requester;
        queryTimeoutTimer = getContext()
                .getSystem()
                .scheduler()
                .scheduleOnce(timout, getSelf(), new CollectionTimeout(),
                        getContext().getDispatcher(),
                        getSelf());
    }

    public static Props props(Map<ActorRef, String> actorToDeviceId,
                              long requestId,
                              ActorRef requester,
                              FiniteDuration timeout) {
        return Props.create(IotDeviceGroupQuery.class, () -> new IotDeviceGroupQuery(actorToDeviceId, requestId, requester, timeout));
    }

    @Override
    public void preStart() {
        actorToDeviceId.keySet().forEach(deviceActor -> {
            getContext().watch(deviceActor);
            deviceActor.tell(new IotDevice.ReadTemperature(0L), getSelf());
        });
    }

    @Override
    public void postStop() {
        queryTimeoutTimer.cancel();
    }

    @Override
    public Receive createReceive() {
        return waitingForReplies(new HashMap<>(), actorToDeviceId.keySet());
    }

    private Receive waitingForReplies(
            Map<String, IotDeviceGroup.TemperatureReading> repliesSoFar, Set<ActorRef> stillWaiting
    ) {
        return receiveBuilder()
                .match(IotDevice.RespondTemperature.class, rt -> onRespondTemperature(rt, repliesSoFar, stillWaiting))
                .match(Terminated.class, t -> onTerminated(t, repliesSoFar, stillWaiting))
                .match(CollectionTimeout.class, ignored -> this.onCollectionTimeout(repliesSoFar, stillWaiting))
                .build();
    }

    private void onTerminated(Terminated t, Map<String, IotDeviceGroup.TemperatureReading> repliesSoFar, Set<ActorRef> stillWaiting) {
        receivedResponse(t.getActor(), IotDeviceGroup.DeviceNotAvailable.INSTANCE,
                stillWaiting,
                repliesSoFar);
    }

    @SuppressWarnings("unused")
    private void onCollectionTimeout(
                                     Map<String, IotDeviceGroup.TemperatureReading> repliesSoFar,
                                     Set<ActorRef> stillWaiting) {
        Map<String, IotDeviceGroup.TemperatureReading> replies = new HashMap<>(repliesSoFar);
        stillWaiting.forEach(deviceActor -> {
            String deviceId = actorToDeviceId.get(deviceActor);
            replies.put(deviceId, IotDeviceGroup.DeviceTimedOut.INSTANCE);
        });
        requester.tell(new IotDeviceGroup.RespondAllTemperatures(requestId, replies), getSelf());
        getContext().stop(getSelf());
    }

    private void onRespondTemperature(IotDevice.RespondTemperature r,
                                      Map<String, IotDeviceGroup.TemperatureReading> repliesSoFar,
                                      Set<ActorRef> stillWaiting) {
        ActorRef deviceActor = getSender();
        IotDeviceGroup.TemperatureReading reading = r.getValue().map(v -> (IotDeviceGroup.TemperatureReading) new IotDeviceGroup.Temperature(v))
                .orElse(IotDeviceGroup.TemperatureNotAvailable.INSTANCE);
        receivedResponse(deviceActor, reading, stillWaiting, repliesSoFar);


    }

    private void receivedResponse(ActorRef deviceActor,
                                  IotDeviceGroup.TemperatureReading reading,
                                  Set<ActorRef> stillWaiting,
                                  Map<String, IotDeviceGroup.TemperatureReading> repliesSoFar) {
        getContext().unwatch(deviceActor);
        String deviceId = actorToDeviceId.get(deviceActor);

        Set<ActorRef> newStillWaiting = new HashSet<>(stillWaiting);
        newStillWaiting.remove(deviceActor);

        Map<String, IotDeviceGroup.TemperatureReading> newRepliesSoFar = new HashMap<>(repliesSoFar);
        newRepliesSoFar.put(deviceId, reading);
        if (newStillWaiting.isEmpty()) {
            requester.tell(new IotDeviceGroup.RespondAllTemperatures(requestId, newRepliesSoFar), getSelf());
            getContext().stop(getSelf());
        } else {
            getContext().become(waitingForReplies(newRepliesSoFar, newStillWaiting));
        }
    }
}
