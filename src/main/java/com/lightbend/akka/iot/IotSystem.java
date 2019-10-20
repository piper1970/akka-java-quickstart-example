package com.lightbend.akka.iot;

import akka.actor.ActorPath;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.util.Timeout;
import scala.concurrent.Await;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static akka.pattern.Patterns.ask;

public class IotSystem {

    private final ActorSystem system;
    private ActorRef supervisor;

    @SuppressWarnings("ResultOfMethodCallIgnored")
    public static void main(String[] args) {
        IotSystem system = new IotSystem("iotSystem");
        try {
            // Do something meaningful....
            // 6. Get all temperatures from device group

            // Track device manager
            String deviceManagerId = "iot-device-manger";
            IotSupervisor.TrackDeviceManager deviceManagerMsg = new IotSupervisor.TrackDeviceManager(1L, deviceManagerId);
            IotSupervisor.DeviceManagerRegistered trackDeviceManagerResponse = (IotSupervisor.DeviceManagerRegistered) Await.result(ask(system.supervisor, deviceManagerMsg, Timeout.create(Duration.ofSeconds(30))),
                    Timeout.create(Duration.ofSeconds(30)).duration());

            if (trackDeviceManagerResponse == null) {
                System.err.println("Errors occurred setting up device manager...");
            }

            // Get device manager list
            IotSupervisor.RequestDeviceManagerList deviceManagerListRequest = new IotSupervisor.RequestDeviceManagerList(2L);
            IotSupervisor.ReplyDeviceManagerList replyDeviceManagerList = (IotSupervisor.ReplyDeviceManagerList) Await.result(ask(system.supervisor, deviceManagerListRequest, Timeout.create(Duration.ofSeconds(30))),
                    Timeout.create(Duration.ofSeconds(30)).duration());

            // Make sure deviceManagerId is in the list
            if (replyDeviceManagerList.ids.contains(deviceManagerId)) {
                System.out.println("Found device manager id in list");
            }

            // Get device manager from list
            IotSupervisor.RequestDeviceManagerById requestDeviceManagerById = new IotSupervisor.RequestDeviceManagerById(3L, deviceManagerId);
            IotSupervisor.ResponseDeviceManagerById deviceManagerResponse = (IotSupervisor.ResponseDeviceManagerById) Await.result(ask(system.supervisor, requestDeviceManagerById, Timeout.create(Duration.ofSeconds(30))),
                    Timeout.create(Duration.ofSeconds(30)).duration());

            ActorRef deviceManager = Optional.ofNullable(deviceManagerResponse)
                    .map(resp -> resp.deviceManager)
                    .orElse(null);

            if (deviceManager == null) {
                System.err.println("Problems obtaining device manager");
                throw new RuntimeException("ByeBye");
            }


            // Track device with device manager
            IotDeviceManager.RequestTrackDevice requestTrackDevice = new IotDeviceManager.RequestTrackDevice("iot-group", "iot-device-1");
            IotDeviceManager.DeviceRegistered trackDeviceResponse = (IotDeviceManager.DeviceRegistered) Await.result(ask(deviceManager, requestTrackDevice, Timeout.create(Duration.ofSeconds(30))),
                    Timeout.create(Duration.ofSeconds(30)).duration());

            if (trackDeviceResponse == null) {
                System.err.println("Problems registering device");
            }

            // Lets send a message to the device from outside of system....
            // Hope this works....
            ActorPath deviceManagerPath = deviceManager.path();
            Optional.ofNullable(deviceManagerPath.child("iotGroup-iot-group"))
                    .flatMap(groupPath -> Optional.ofNullable(groupPath.child("iotDevice-iot-device-1")))
                    .ifPresentOrElse(childPath -> {
                        ActorRef childActor = system.system.actorFor(childPath);
                        IotDevice.RecordTemperature recordTemperature = new IotDevice.RecordTemperature(4L, 33.5);
                        childActor.tell(recordTemperature, ActorRef.noSender());
                    }, () -> {
                        System.err.println("Problems getting device reference");
                        throw new RuntimeException("ByeBye");
                    });

            TimeUnit.SECONDS.sleep(5);

            IotDeviceManager.RequestDeviceGroupById requestDeviceGroupById = new IotDeviceManager.RequestDeviceGroupById(5L, "iot-group");
            IotDeviceManager.RespondDeviceGroupById deviceGroupByIdResponse = (IotDeviceManager.RespondDeviceGroupById) Await.result(ask(deviceManager, requestDeviceGroupById, Timeout.create(Duration.ofSeconds(30))),
                    Timeout.create(Duration.ofSeconds(30)).duration());

            ActorRef deviceGroup = Optional.ofNullable(deviceGroupByIdResponse)
                    .map(resp -> resp.deviceGroupActor)
                    .orElseGet(() -> {
                        System.err.println("Problems obtaining device group");
                        throw new RuntimeException("ByeBye");
                    });

            IotDeviceGroup.RequestAllTemperatures requestAllTemperatures = new IotDeviceGroup.RequestAllTemperatures(9L);
            IotDeviceGroup.RespondAllTemperatures respondAllTemperatures = (IotDeviceGroup.RespondAllTemperatures) Await.result(ask(deviceGroup, requestAllTemperatures, Timeout.create(Duration.ofSeconds(30))),
                    Timeout.create(Duration.ofSeconds(30)).duration());

            System.out.println("RespondAllTemperatures for device group directly finished...");
            respondAllTemperatures.temperatures.forEach((key, val) -> {
                if (val instanceof IotDeviceGroup.TemperatureNotAvailable) {
                    System.out.println(String.format("Tempererature for device %s is not available", key));
                } else if (val instanceof IotDeviceGroup.DeviceNotAvailable) {
                    System.out.println(String.format("Device %s is not available", key));
                } else if (val instanceof IotDeviceGroup.DeviceTimedOut) {
                    System.out.println(String.format("Device %s timed out", key));
                } else if (val instanceof IotDeviceGroup.Temperature) {
                    IotDeviceGroup.Temperature tempVal = (IotDeviceGroup.Temperature) val;
                    System.out.println(String.format("Tempererature for device %s is %f", key, tempVal.value));
                }
            });

            IotDeviceManager.RequestAllGroupTemperatures requestAllGroupTemperatures = new IotDeviceManager.RequestAllGroupTemperatures(6L);
            IotDeviceManager.RespondAllGroupTemperatures respondAllGroupTemperatures = (IotDeviceManager.RespondAllGroupTemperatures) Await.result(ask(deviceManager, requestAllGroupTemperatures, Timeout.create(Duration.ofSeconds(30))),
                    Timeout.create(Duration.ofSeconds(30)).duration());

            System.out.println("RespondAllGroupTemperatures for device manager finished...");
            respondAllGroupTemperatures.groupTemperatures
                    .forEach((key, value) -> {
                        if (value instanceof IotDeviceManager.DeviceGroupTemperatures) {
                            IotDeviceManager.DeviceGroupTemperatures deviceTempList = (IotDeviceManager.DeviceGroupTemperatures) value;
                            deviceTempList.groupTemperatureReading
                                    .forEach((key1, val1) -> {
                                        if (val1 instanceof IotDeviceGroup.TemperatureNotAvailable) {
                                            System.out.println(String.format("Tempererature for device %s of group %s is not available", key1, key));
                                        } else if (val1 instanceof IotDeviceGroup.DeviceNotAvailable) {
                                            System.out.println(String.format("Device %s of group %s is not available", key1, key));
                                        } else if (val1 instanceof IotDeviceGroup.DeviceTimedOut) {
                                            System.out.println(String.format("Device %s of group %s timed out", key1, key));
                                        } else if (val1 instanceof IotDeviceGroup.Temperature) {
                                            IotDeviceGroup.Temperature tempVal = (IotDeviceGroup.Temperature) val1;
                                            System.out.println(String.format("Tempererature for device %s of group %s is %f", key1, key, tempVal.value));
                                        }
                                    });
                        } else if (value instanceof IotDeviceManager.DeviceGroupNotAvailable) {
                            System.out.println("Device group not available for " + key);
                        } else if (value instanceof IotDeviceManager.DeviceGroupTimedOut) {
                            System.out.println("Device group timed out for " + key);
                        }
                    });

            System.out.println("Press Enter to exit the system");
            System.in.read();
        } catch (Exception exc) {
            System.err.println("Exceptions occurred: " + exc.getMessage());
            exc.printStackTrace();
        } finally {
            system.terminate();
        }

    }

    private IotSystem(String systemName) {
        String supervisorName = systemName + "-supervisor";
        system = ActorSystem.create(systemName);
        supervisor = system.actorOf(IotSupervisor.props(), supervisorName);
    }

    private void terminate() {
        system.terminate();
    }
}
