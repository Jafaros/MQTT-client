package utb.fai;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import utb.fai.API.HumiditySensor;
import utb.fai.API.IrrigationSystem;

/**
 * Trida MQTT klienta pro mereni vhlkosti pudy a rizeni zavlazovaciho systemu. 
 * 
 * V teto tride implementuje MQTT klienta
 */
public class SoilMoistureMQTTClient {

    private MqttClient client;
    private HumiditySensor humiditySensor;
    private IrrigationSystem irrigationSystem;

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private ScheduledFuture<?> irrigationTimeoutTask;
    private final AtomicBoolean irrigationActive = new AtomicBoolean(false);

    /**
     * Vytvori instacni tridy MQTT klienta pro mereni vhlkosti pudy a rizeni
     * zavlazovaciho systemu
     * 
     * @param sensor     Senzor vlhkosti
     * @param irrigation Zarizeni pro zavlahu pudy
     */
    public SoilMoistureMQTTClient(HumiditySensor sensor, IrrigationSystem irrigation) {
        this.humiditySensor = sensor;
        this.irrigationSystem = irrigation;
    }

    /**
     * Metoda pro spusteni klienta
     */
    public void start() {
        try {
            // inicializace MQTT klienta
            client = new MqttClient(Config.BROKER, Config.CLIENT_ID);
            client.connect();

            client.setCallback(new MqttCallback() {
                @Override
                public void connectionLost(Throwable cause) {
                    cause.printStackTrace();
                }

                @Override
                public void messageArrived(String topic, MqttMessage message) throws Exception {
                    String payload = new String(message.getPayload());

                    switch (payload) {
                        case Config.REQUEST_GET_HUMIDITY: {
                            float humidity = humiditySensor.readRAWValue();
                            boolean hasFault = humiditySensor.hasFault();

                            if (hasFault) {
                                String faultResponse = Config.RESPONSE_FAULT + ";" + "HUMIDITY_SENSOR";
                                client.publish(Config.TOPIC_OUT, new MqttMessage(faultResponse.getBytes()));
                            } else {
                                String humidityResponse = Config.RESPONSE_HUMIDITY + ";" + humidity;
                                client.publish(Config.TOPIC_OUT, new MqttMessage(humidityResponse.getBytes()));
                            }
                            break;
                        }
                        case Config.REQUEST_GET_STATUS: {
                            boolean isIrrigating = irrigationSystem.isActive();
                            boolean humiditySensorHasFault = humiditySensor.hasFault();
                            boolean irrigationSystemHasFault = irrigationSystem.hasFault();

                            String statusResponse = Config.RESPONSE_STATUS + ";" + (isIrrigating ? "irrigation_on" : "irrigation_off");
                            client.publish(Config.TOPIC_OUT, new MqttMessage(statusResponse.getBytes()));

                            if (irrigationSystemHasFault) {
                                String faultResponse = Config.RESPONSE_FAULT + ";" + "IRRIGATION_SYSTEM";
                                client.publish(Config.TOPIC_OUT, new MqttMessage(faultResponse.getBytes()));
                            }

                            if (humiditySensorHasFault) {
                                String faultResponse = Config.RESPONSE_FAULT + ";" + "HUMIDITY_SENSOR";
                                client.publish(Config.TOPIC_OUT, new MqttMessage(faultResponse.getBytes()));
                            }

                            break;
                        }
                        case Config.REQUEST_START_IRRIGATION: {
                            irrigationSystem.activate();
                            boolean isIrrigating = irrigationSystem.isActive();
                            boolean humiditySensorHasFault = humiditySensor.hasFault();
                            boolean irrigationSystemHasFault = irrigationSystem.hasFault();

                            if (irrigationSystemHasFault) {
                                String faultResponse = Config.RESPONSE_FAULT + ";" + "IRRIGATION_SYSTEM";
                                client.publish(Config.TOPIC_OUT, new MqttMessage(faultResponse.getBytes()));
                            }
                            
                            if (humiditySensorHasFault) {
                                String faultResponse = Config.RESPONSE_FAULT + ";" + "HUMIDITY_SENSOR";
                                client.publish(Config.TOPIC_OUT, new MqttMessage(faultResponse.getBytes()));
                            }
                            
                            if (isIrrigating && !irrigationSystemHasFault && !humiditySensorHasFault) {
                                String statusResponse = Config.RESPONSE_STATUS + ";" + (isIrrigating ? "irrigation_on" : "irrigation_off");
                                client.publish(Config.TOPIC_OUT, new MqttMessage(statusResponse.getBytes()));
                                startIrrigationTimeoutTask(client);
                            }
                            break;
                        }
                        case Config.REQUEST_STOP_IRRIGATION: {
                            irrigationSystem.deactivate();
                            boolean isIrrigating = irrigationSystem.isActive();
                            boolean humiditySensorHasFault = humiditySensor.hasFault();
                            boolean irrigationSystemHasFault = irrigationSystem.hasFault();

                            if (!irrigationSystem.isActive() && !humiditySensorHasFault && !irrigationSystemHasFault) {
                                String statusResponse = Config.RESPONSE_STATUS + ";" + (isIrrigating ? "irrigation_on" : "irrigation_off");
                                client.publish(Config.TOPIC_OUT, new MqttMessage(statusResponse.getBytes()));
                                stopIrrigationTimeoutTask();
                            }

                            if (irrigationSystemHasFault) {
                                String faultResponse = Config.RESPONSE_FAULT + ";" + "IRRIGATION_SYSTEM";
                                client.publish(Config.TOPIC_OUT, new MqttMessage(faultResponse.getBytes()));
                            }

                            if (humiditySensorHasFault) {
                                String faultResponse = Config.RESPONSE_FAULT + ";" + "HUMIDITY_SENSOR";
                                client.publish(Config.TOPIC_OUT, new MqttMessage(faultResponse.getBytes()));
                            }
                            break;
                        }
                        default:
                            String faultResponse = Config.RESPONSE_FAULT + ";unknown-request";
                            client.publish(Config.TOPIC_OUT, new MqttMessage(faultResponse.getBytes()));
                            break;
                    }
                }

                @Override
                public void deliveryComplete(org.eclipse.paho.client.mqttv3.IMqttDeliveryToken token) {
                    
                }
            });
            
            client.subscribe(Config.TOPIC_IN);

            sendHumidity();

            Thread humidityThread = new Thread(() -> {
                while (true) {
                    try {
                        Thread.sleep(10000);
                        sendHumidity();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });

            humidityThread.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void sendHumidity() {
        try {
            float humidity = humiditySensor.readRAWValue();
            boolean humiditySensorHasFault = humiditySensor.hasFault();
            boolean irrigationSystemHasFault = irrigationSystem.hasFault();

            if (humiditySensorHasFault || humidity < 0) {
                String faultResponse = Config.RESPONSE_FAULT + ";" + "HUMIDITY_SENSOR";
                client.publish(Config.TOPIC_OUT, new MqttMessage(faultResponse.getBytes()));
            } 
            
            if (irrigationSystemHasFault) {
                String faultResponse = Config.RESPONSE_FAULT + ";" + "IRRIGATION_SYSTEM";
                client.publish(Config.TOPIC_OUT, new MqttMessage(faultResponse.getBytes()));
            } 
            
            if (!humiditySensorHasFault && !irrigationSystemHasFault && humidity >= 0) {
                String message = Config.RESPONSE_HUMIDITY + ";" + humidity;
                client.publish(Config.TOPIC_OUT, new MqttMessage(message.getBytes()));
            }
        } catch (Exception e) {
            System.out.println("Error while sending humidity: " + e.getMessage());
        }
    }

    private synchronized void startIrrigationTimeoutTask(MqttClient client) {
         if (!irrigationActive.get()) {
                irrigationSystem.activate();
                irrigationActive.set(true);
         }

        if (irrigationTimeoutTask != null && !irrigationTimeoutTask.isDone()) {
            irrigationTimeoutTask.cancel(false);
        }
        
        irrigationTimeoutTask = scheduler.schedule(() -> {
                // automaticke ukonceni po timeoutu
                synchronized (SoilMoistureMQTTClient.this) {
                    if (irrigationActive.get()) {
                        try {
                            irrigationSystem.deactivate();
                            irrigationActive.set(false);
                            String message = Config.RESPONSE_STATUS + ";" + "irrigation_off";
                            client.publish(Config.TOPIC_OUT, new MqttMessage(message.getBytes()));
                            System.out.println("Irrigation stopped (timeout).");
                        } catch (Exception e) {
                            System.err.println("Error stopping irrigation after timeout: " + e.getMessage());
                        }
                    }
                }
            }, 30, TimeUnit.SECONDS);
    }

    private synchronized void stopIrrigationTimeoutTask() {
        try {
            if (irrigationTimeoutTask != null && !irrigationTimeoutTask.isDone()) {
                irrigationTimeoutTask.cancel(false);
            }

            if (irrigationActive.get()) {
                irrigationSystem.deactivate();
                irrigationActive.set(false);
            }
        } catch (Exception e) {
            System.err.println("Error stopping irrigation: " + e.getMessage());
        }
    }
}
