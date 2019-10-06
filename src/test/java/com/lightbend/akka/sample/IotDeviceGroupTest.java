package com.lightbend.akka.sample;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.PoisonPill;
import akka.testkit.javadsl.TestKit;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

public class IotDeviceGroupTest {

    private static ActorSystem system;

    private TestKit probe;

    @Before
    public void setup() {
        probe = new TestKit(system);
    }

    @BeforeClass
    public static void classSetup() {
        system = ActorSystem.create();
    }

    @AfterClass
    public static void classTeardown() {
        system.terminate();
    }

    @Test
    public void testRegisterDeviceActor() {
        ActorRef groupActor = system.actorOf(IotDeviceGroup.props("group"));
        groupActor.tell(new IotDeviceManager.RequestTrackDevice("group", "device1"), probe.getRef());
        probe.expectMsgClass(IotDeviceManager.DeviceRegistered.class);
        ActorRef deviceActor1 = probe.getLastSender();

        groupActor.tell(new IotDeviceManager.RequestTrackDevice("group", "device2"), probe.getRef());
        probe.expectMsgClass(IotDeviceManager.DeviceRegistered.class);
        ActorRef deviceActor2 = probe.getLastSender();

        assertNotEquals(deviceActor1, deviceActor2);

        // test functionality
        deviceActor1.tell(new IotDevice.RecordTemperature(0L, 1.0), probe.getRef());
        assertEquals(0L, probe.expectMsgClass(IotDevice.TemperatureRecorded.class).requestId);

        deviceActor2.tell(new IotDevice.RecordTemperature(1L, 2.0), probe.getRef());
        assertEquals(1L, probe.expectMsgClass(IotDevice.TemperatureRecorded.class).requestId);
    }

    @Test
    public void testIgnoreRequestsForWrongGroupId() {
        ActorRef groupActor = system.actorOf(IotDeviceGroup.props("group"));
        groupActor.tell(new IotDeviceManager.RequestTrackDevice("wrong", "device1"), probe.getRef());
        probe.expectNoMessage();
    }

    @Test
    public void testReturnSameActorForSameDeviceId() {
        ActorRef groupActor = system.actorOf(IotDeviceGroup.props("group"));

        groupActor.tell(new IotDeviceManager.RequestTrackDevice("group", "device1"), probe.getRef());
        probe.expectMsgClass(IotDeviceManager.DeviceRegistered.class);
        ActorRef deviceActor1 = probe.getLastSender();

        groupActor.tell(new IotDeviceManager.RequestTrackDevice("group", "device1"), probe.getRef());
        probe.expectMsgClass(IotDeviceManager.DeviceRegistered.class);
        ActorRef deviceActor2 = probe.getLastSender();
        assertEquals(deviceActor1, deviceActor2);
    }

    @Test
    public void testListActiveDevices() {
        ActorRef groupActor = system.actorOf(IotDeviceGroup.props("group"));

        groupActor.tell(new IotDeviceManager.RequestTrackDevice("group", "device1"), probe.getRef());
        probe.expectMsgClass(IotDeviceManager.DeviceRegistered.class);

        groupActor.tell(new IotDeviceManager.RequestTrackDevice("group", "device2"), probe.getRef());
        probe.expectMsgClass(IotDeviceManager.DeviceRegistered.class);

        groupActor.tell(new IotDeviceGroup.RequestDeviceList(0L), probe.getRef());
        IotDeviceGroup.ReplyDeviceList reply = probe.expectMsgClass(IotDeviceGroup.ReplyDeviceList.class);
        assertNotNull(reply);
        assertEquals(0L, reply.requestId);
        assertEquals(Set.of("device1", "device2"), reply.ids);
    }

    @Test
    public void testListAcdtiveDevicesAfterOneShutsDown() {
        ActorRef groupActor = system.actorOf(IotDeviceGroup.props("group"));

        groupActor.tell(new IotDeviceManager.RequestTrackDevice("group", "device1"), probe.getRef());
        probe.expectMsgClass(IotDeviceManager.DeviceRegistered.class);
        ActorRef toShutDown = probe.getLastSender();

        groupActor.tell(new IotDeviceManager.RequestTrackDevice("group", "device2"), probe.getRef());
        probe.expectMsgClass(IotDeviceManager.DeviceRegistered.class);

        groupActor.tell(new IotDeviceGroup.RequestDeviceList(0L), probe.getRef());
        IotDeviceGroup.ReplyDeviceList reply1 = probe.expectMsgClass(IotDeviceGroup.ReplyDeviceList.class);
        assertEquals(0L, reply1.requestId);
        assertEquals(Set.of("device1", "device2"), reply1.ids);

        probe.watch(toShutDown);
        toShutDown.tell(PoisonPill.getInstance(), ActorRef.noSender());
        probe.expectTerminated(toShutDown);

        probe.awaitAssert(() -> {
            groupActor.tell(new IotDeviceGroup.RequestDeviceList(1L), probe.getRef());
            IotDeviceGroup.ReplyDeviceList reply2 = probe.expectMsgClass(IotDeviceGroup.ReplyDeviceList.class);
            assertEquals(1L, reply2.requestId);
            assertEquals(Set.of("device2"), reply2.ids);
            return null;
        });
    }

    @Test
    public void testCollectTemperaturesFrolAllActiveDevices() {
        ActorRef groupActor = system.actorOf(IotDeviceGroup.props("group"));

        groupActor.tell(new IotDeviceManager.RequestTrackDevice("group", "device1"), probe.getRef());
        probe.expectMsgClass(IotDeviceManager.DeviceRegistered.class);
        ActorRef device1Actor = probe.getLastSender();

        groupActor.tell(new IotDeviceManager.RequestTrackDevice("group", "device2"), probe.getRef());
        probe.expectMsgClass(IotDeviceManager.DeviceRegistered.class);
        ActorRef device2Actor = probe.getLastSender();

        groupActor.tell(new IotDeviceManager.RequestTrackDevice("group", "device3"), probe.getRef());
        probe.expectMsgClass(IotDeviceManager.DeviceRegistered.class);

        device1Actor.tell(new IotDevice.RecordTemperature(0L, 1.0), probe.getRef());
        assertEquals(0L, probe.expectMsgClass(IotDevice.TemperatureRecorded.class).requestId);

        device2Actor.tell(new IotDevice.RecordTemperature(1L, 2.0), probe.getRef());
        assertEquals(1L, probe.expectMsgClass(IotDevice.TemperatureRecorded.class).requestId);

        groupActor.tell(new IotDeviceGroup.RequestAllTemperatures(0L), probe.getRef());
        IotDeviceGroup.RespondAllTemperatures response = probe.expectMsgClass(IotDeviceGroup.RespondAllTemperatures.class);
        assertEquals(0L, response.requestId);

        Map<String, IotDeviceGroup.TemperatureReading> expectedTemperatures = Map.of("device1",
                new IotDeviceGroup.Temperature(1.0), "device2", new IotDeviceGroup.Temperature(2.0),
                "device3", IotDeviceGroup.TemperatureNotAvailable.INSTANCE);

        assertEquals(expectedTemperatures, response.temperatures);
    }
}