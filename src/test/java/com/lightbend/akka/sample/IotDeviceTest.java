package com.lightbend.akka.sample;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.javadsl.TestKit;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.assertEquals;

public class IotDeviceTest {

    private static ActorSystem system;

    private TestKit probe;

    private String groupId = "group";
    private String deviceId = "device";

    @Before
    public void setup() {
        probe = new TestKit(system);
    }

    @BeforeClass
    public static void classSetup() {
        system = ActorSystem.create();
    }

    @AfterClass
    public static void tearDown() {
        system.terminate();
    }

    @Test
    public void testReplyWithEmptyReadingIfNoTemperatureIsKnown() {
        ActorRef deviceActor = system.actorOf(IotDevice.props(groupId, deviceId));
        deviceActor.tell(new IotDevice.ReadTemperature(42L), probe.getRef());
        IotDevice.RespondTemperature response = probe.expectMsgClass(IotDevice.RespondTemperature.class);
        assertEquals(42L, response.requestId);
        assertEquals(Optional.empty(), response.value);
    }

    @Test
    public void testReplyWithLatestTemperatureReading() {
        ActorRef deviceActor = system.actorOf(IotDevice.props(groupId, deviceId));
        deviceActor.tell(new IotDevice.RecordTemperature(1L, 24.0), probe.getRef());
        assertEquals(1L, probe.expectMsgClass(IotDevice.TemperatureRecorded.class).requestId);

        deviceActor.tell(new IotDevice.ReadTemperature(2L), probe.getRef());
        IotDevice.RespondTemperature response1 = probe.expectMsgClass(IotDevice.RespondTemperature.class);
        assertEquals(2L, response1.requestId);
        assertEquals(Optional.of(24.0), response1.value);

        deviceActor.tell(new IotDevice.RecordTemperature(3L, 55.0), probe.getRef());
        assertEquals(3L, probe.expectMsgClass(IotDevice.TemperatureRecorded.class).requestId);

        deviceActor.tell(new IotDevice.ReadTemperature(4L), probe.getRef());
        IotDevice.RespondTemperature response2 = probe.expectMsgClass(IotDevice.RespondTemperature.class);
        assertEquals(4L, response2.requestId);
        assertEquals(Optional.of(55.0), response2.value);
    }

    @Test
    public void testReplyToRegistrationRequests() {
        ActorRef deviceActor = system.actorOf(IotDevice.props(groupId, deviceId));
        deviceActor.tell(new IotDeviceManager.RequestTrackDevice(groupId, deviceId), probe.getRef());
        probe.expectMsgClass(IotDeviceManager.DeviceRegistered.class);
        assertEquals(deviceActor, probe.getLastSender());
    }

    @Test
    public void testIgnoreWrongRegistrationRequests() {
        ActorRef deviceActor = system.actorOf(IotDevice.props(groupId, deviceId));
        deviceActor.tell(new IotDeviceManager.RequestTrackDevice(groupId, "bad"), probe.getRef());
        probe.expectNoMessage();

        deviceActor.tell(new IotDeviceManager.RequestTrackDevice("wrong", deviceId), probe.getRef());
        probe.expectNoMessage();
    }

}