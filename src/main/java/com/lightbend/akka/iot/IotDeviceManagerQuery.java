package com.lightbend.akka.iot;

import akka.actor.*;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import scala.concurrent.duration.FiniteDuration;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class IotDeviceManagerQuery extends AbstractActor {
    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
    private static final class CollectionTimeout {
    }

    private final Map<ActorRef, String> actorToDeviceGroupId;
    private final long requestId;
    private final ActorRef requester;
    private final Cancellable queryTimeoutTimer;

    private IotDeviceManagerQuery(Map<ActorRef, String> actorToDeviceGroupId, long requestId,
                                  ActorRef requester, FiniteDuration timeout) {
        this.actorToDeviceGroupId = actorToDeviceGroupId;
        this.requestId = requestId;
        this.requester = requester;

        queryTimeoutTimer = getContext()
                .getSystem()
                .scheduler()
                .scheduleOnce(timeout, getSelf(), new CollectionTimeout(), getContext().getDispatcher(), getSelf());
    }

    public static Props props(Map<ActorRef, String> actorToDeviceGroupId, long requestId,
                              ActorRef requester, FiniteDuration timeout) {
        return Props.create(IotDeviceManagerQuery.class, () ->
                new IotDeviceManagerQuery(actorToDeviceGroupId, requestId, requester, timeout));
    }

    @Override
    public void preStart() {
        actorToDeviceGroupId.keySet().forEach(actorRef -> {
            getContext().watch(actorRef);
            actorRef.tell(new IotDeviceGroup.RequestAllTemperatures(1L), getSelf());
        });
    }

    @Override
    public void postStop() {
        queryTimeoutTimer.cancel();
    }

    @Override
    public Receive createReceive() {
        return waitingForReplies(new HashMap<>(), actorToDeviceGroupId.keySet());
    }

    private Receive waitingForReplies(Map<String, IotDeviceManager.DeviceGroupTemperatureReading> repliesSoFar,
                                      Set<ActorRef> stillWaiting) {
        return receiveBuilder()
                .match(IotDeviceGroup.RespondAllTemperatures.class, msg -> this.onRespondAllTemperatures(msg, repliesSoFar, stillWaiting))
                .match(CollectionTimeout.class, ignored -> this.onCollectionTimeout(repliesSoFar, stillWaiting))
                .match(Terminated.class, msg -> this.onTerminated(msg, repliesSoFar, stillWaiting))
                .build();
    }

    private void onTerminated(Terminated msg, Map<String, IotDeviceManager.DeviceGroupTemperatureReading> repliesSoFar, Set<ActorRef> stillWaiting) {
        receivedResponse(msg.getActor(), IotDeviceManager.DeviceGroupNotAvailable.INSTANCE, repliesSoFar, stillWaiting);
    }

    private void onCollectionTimeout(Map<String, IotDeviceManager.DeviceGroupTemperatureReading> repliesSoFar, Set<ActorRef> stillWaiting) {
        Map<String, IotDeviceManager.DeviceGroupTemperatureReading> replies = new HashMap<>(repliesSoFar);
        stillWaiting.forEach(actorRef -> {
            String id = actorToDeviceGroupId.get(actorRef);
            replies.put(id, IotDeviceManager.DeviceGroupTimedOut.INSTANCE);
        });
        // get sender and tell
        requester.tell(new IotDeviceManager.RespondAllGroupTemperatures(requestId, replies), getSelf());
        getContext().stop(getSelf());

    }

    private void onRespondAllTemperatures(IotDeviceGroup.RespondAllTemperatures msg, Map<String, IotDeviceManager.DeviceGroupTemperatureReading> repliesSoFar, Set<ActorRef> stillWaiting) {
        log.info("Responding to IotDeviceGroup.RespondAllTemperatures");
        receivedResponse(getSender(), new IotDeviceManager.DeviceGroupTemperatures(msg.requestId, msg.temperatures), repliesSoFar, stillWaiting);
    }

    private void receivedResponse(ActorRef actorRef, IotDeviceManager.DeviceGroupTemperatureReading reading,
                                  Map<String, IotDeviceManager.DeviceGroupTemperatureReading> repliesSoFar, Set<ActorRef> stillWating) {

        getContext().unwatch(actorRef);
        String groupId = actorToDeviceGroupId.get(actorRef);
        Set<ActorRef> newStillWaiting = new HashSet<>(stillWating);
        newStillWaiting.remove(actorRef);
        Map<String, IotDeviceManager.DeviceGroupTemperatureReading> newRepliesSoFar = new HashMap<>(repliesSoFar);
        newRepliesSoFar.put(groupId, reading);
        if (newStillWaiting.isEmpty()) {
            log.info("Sending IotDeviceManager.RespondAllGroupTemperatures response to sender with path {}", requester.path());
            requester.tell(new IotDeviceManager.RespondAllGroupTemperatures(requestId, newRepliesSoFar), getContext().getParent());
            getContext().stop(getSelf());
        } else {
            getContext().become(waitingForReplies(newRepliesSoFar, newStillWaiting));
        }

    }
}
