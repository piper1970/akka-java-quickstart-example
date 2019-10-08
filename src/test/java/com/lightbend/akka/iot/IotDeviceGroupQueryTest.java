package com.lightbend.akka.iot;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.PoisonPill;
import akka.testkit.javadsl.TestKit;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.concurrent.duration.FiniteDuration;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class IotDeviceGroupQueryTest {

    private static ActorSystem system;

    private TestKit requester;
    private TestKit device1;
    private TestKit device2;
    private String device1Name = "device1";
    private String device2Name = "device2";

    private Map<ActorRef, String> actorToDeviceId;

    @BeforeClass
    public static void setupClass() {
        system = ActorSystem.create();
    }

    @AfterClass
    public static void teardownClass() {
        system.terminate();
    }

    @Before
    public void setUp() {
        requester = new TestKit(system);
        device1 = new TestKit(system);
        device2 = new TestKit(system);
        actorToDeviceId = new HashMap<>();
        actorToDeviceId.put(device1.getRef(), device1Name);
        actorToDeviceId.put(device2.getRef(), device2Name);
    }

    @Test
    public void testReturnTemperatureValueForWorkingDevices() {
        ActorRef queryActor = system.actorOf(IotDeviceGroupQuery.props(actorToDeviceId, 1L, requester.getRef(),
                new FiniteDuration(3, TimeUnit.SECONDS)));
        assertEquals(0L, device1.expectMsgClass(IotDevice.ReadTemperature.class).requestId);
        assertEquals(0L, device2.expectMsgClass(IotDevice.ReadTemperature.class).requestId);

        queryActor.tell(new IotDevice.RespondTemperature(0L, 1.0), device1.getRef());
        queryActor.tell(new IotDevice.RespondTemperature(0L, 2.0), device2.getRef());

        IotDeviceGroup.RespondAllTemperatures response =
                requester.expectMsgClass(IotDeviceGroup.RespondAllTemperatures.class);
        assertEquals(1L, response.requestId);

        Map<String, IotDeviceGroup.TemperatureReading> expectedTemperatures = Map.of(device1Name, new IotDeviceGroup.Temperature(1.0),
                device2Name, new IotDeviceGroup.Temperature(2.0));

        assertEquals(expectedTemperatures, response.temperatures);
    }

    @Test
    public void testReturnTemperatureNotAvailableForDeviscesWithNoReadings() {
        ActorRef queryActor = system.actorOf(IotDeviceGroupQuery.props(actorToDeviceId, 1L, requester.getRef(),
                new FiniteDuration(3, TimeUnit.SECONDS)));
        assertEquals(0L, device1.expectMsgClass(IotDevice.ReadTemperature.class).requestId);
        assertEquals(0L, device2.expectMsgClass(IotDevice.ReadTemperature.class).requestId);

        queryActor.tell(new IotDevice.RespondTemperature(0L, null), device1.getRef());
        queryActor.tell(new IotDevice.RespondTemperature(0L, 2.0), device2.getRef());

        IotDeviceGroup.RespondAllTemperatures response =
                requester.expectMsgClass(IotDeviceGroup.RespondAllTemperatures.class);
        assertEquals(1L, response.requestId);

        Map<String, IotDeviceGroup.TemperatureReading> expectedTemperatures = Map.of(device1Name, IotDeviceGroup.TemperatureNotAvailable.INSTANCE,
                device2Name, new IotDeviceGroup.Temperature(2.0));

        assertEquals(expectedTemperatures, response.temperatures);
    }

    @Test
    public void testReturnDeviceNotAvailableIfDeviceStopsBeforeAnswering() {
        ActorRef queryActor = system.actorOf(IotDeviceGroupQuery.props(actorToDeviceId, 1L, requester.getRef(),
                new FiniteDuration(3, TimeUnit.SECONDS)));
        assertEquals(0L, device1.expectMsgClass(IotDevice.ReadTemperature.class).requestId);
        assertEquals(0L, device2.expectMsgClass(IotDevice.ReadTemperature.class).requestId);

        queryActor.tell(new IotDevice.RespondTemperature(0L, 1.0), device1.getRef());
        device2.getRef().tell(PoisonPill.getInstance(), ActorRef.noSender());

        IotDeviceGroup.RespondAllTemperatures response =
                requester.expectMsgClass(IotDeviceGroup.RespondAllTemperatures.class);
        assertEquals(1L, response.requestId);

        Map<String, IotDeviceGroup.TemperatureReading> expectedTemperatures = Map.of(device1Name, new IotDeviceGroup.Temperature(1.0),
                device2Name, IotDeviceGroup.DeviceNotAvailable.INSTANCE);

        assertEquals(expectedTemperatures, response.temperatures);
    }

    @Test
    public void testReturnTemperatureReadingEvenIfDeviceStopsAfterAnswering() {
        ActorRef queryActor = system.actorOf(IotDeviceGroupQuery.props(actorToDeviceId, 1L, requester.getRef(),
                new FiniteDuration(1, TimeUnit.SECONDS)));
        assertEquals(0L, device1.expectMsgClass(IotDevice.ReadTemperature.class).requestId);
        assertEquals(0L, device2.expectMsgClass(IotDevice.ReadTemperature.class).requestId);

        queryActor.tell(new IotDevice.RespondTemperature(0L, 1.0), device1.getRef());

        IotDeviceGroup.RespondAllTemperatures response = requester.expectMsgClass(Duration.ofSeconds(5),
                IotDeviceGroup.RespondAllTemperatures.class);

        assertEquals(1L, response.requestId);

        Map<String, IotDeviceGroup.TemperatureReading> expectedTemperatures = Map.of(device1Name, new IotDeviceGroup.Temperature(1.0),
                device2Name, IotDeviceGroup.DeviceTimedOut.INSTANCE);

        assertEquals(expectedTemperatures, response.temperatures);

    }
}