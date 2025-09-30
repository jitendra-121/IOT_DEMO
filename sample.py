import random
import time
import json
import threading
from azure.iot.device import IoTHubDeviceClient, Message
from azure.iot.device.exceptions import ConnectionFailedError, ConnectionDroppedError, ClientError

# Replace with your DEVICE connection string from Azure IoT Hub
CONNECTION_STRING = " "

class IoTDevice:
    def __init__(self, connection_string):
        self.connection_string = connection_string
        self.device_client = None
        self.connected = False
        self.running = False
        
    def connect(self):
        """Connect to Azure IoT Hub with retry logic"""
        max_retries = 3
        retry_delay = 5
        
        for attempt in range(max_retries):
            try:
                print(f"Connection attempt {attempt + 1}/{max_retries}...")
                self.device_client = IoTHubDeviceClient.create_from_connection_string(self.connection_string)
                self.device_client.connect()
                self.connected = True
                print("✅ Successfully connected to Azure IoT Hub!")
                return True
            except ConnectionFailedError as e:
                print(f"❌ Connection failed: {e}")
                if attempt < max_retries - 1:
                    print(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
            except Exception as e:
                print(f"❌ Unexpected error during connection: {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
        
        print("❌ Failed to connect after all retry attempts")
        return False
    
    def disconnect(self):
        """Safely disconnect from Azure IoT Hub"""
        self.running = False
        if self.device_client and self.connected:
            try:
                self.device_client.disconnect()
                self.connected = False
                print("📡 Disconnected from Azure IoT Hub")
            except Exception as e:
                print(f"Error during disconnect: {e}")
    
    def send_message(self, data):
        """Send a message to Azure IoT Hub with error handling"""
        if not self.connected or not self.device_client:
            print("❌ Not connected to IoT Hub")
            return False
            
        try:
            message_json = json.dumps(data)
            msg = Message(message_json)
            msg.content_type = "application/json"
            msg.content_encoding = "utf-8"
            
            print(f"📤 Sending: {message_json}")
            self.device_client.send_message(msg)
            print("✅ Message sent successfully")
            return True
            
        except ConnectionDroppedError:
            print("⚠️ Connection dropped during message send. Attempting to reconnect...")
            self.connected = False
            return self.reconnect_and_send(data)
        except ClientError as e:
            print(f"❌ Client error: {e}")
            return False
        except Exception as e:
            print(f"❌ Unexpected error sending message: {e}")
            return False
    
    def reconnect_and_send(self, data):
        """Attempt to reconnect and resend message"""
        if self.connect() and self.device_client:
            try:
                message_json = json.dumps(data)
                msg = Message(message_json)
                msg.content_type = "application/json"
                msg.content_encoding = "utf-8"
                
                self.device_client.send_message(msg)
                print("✅ Message sent after reconnection")
                return True
            except Exception as e:
                print(f"❌ Failed to send message after reconnection: {e}")
                return False
        return False
    
    def send_telemetry_loop(self):
        """Send telemetry data in a loop"""
        self.running = True
        message_count = 0
        
        while self.running:
            try:
                # Simulate sensor data
                temperature = round(random.uniform(20.0, 30.0), 2)
                humidity = round(random.uniform(40.0, 60.0), 2)
                message_count += 1
                
                telemetry_data = {
                    "messageId": message_count,
                    "temperature": temperature,
                    "humidity": humidity,
                    "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
                }
                
                success = self.send_message(telemetry_data)
                if success:
                    print(f"📊 Telemetry #{message_count} - Temp: {temperature}°C, Humidity: {humidity}%")
                else:
                    print(f"⚠️ Failed to send telemetry #{message_count}")
                
                # Wait before sending next message
                for _ in range(10):  # 10 second wait, but check every second if we should stop
                    if not self.running:
                        break
                    time.sleep(1)
                    
            except KeyboardInterrupt:
                print("\n🛑 Telemetry stopped by user")
                break
            except Exception as e:
                print(f"❌ Error in telemetry loop: {e}")
                time.sleep(5)
    
    def receive_messages(self):
        """Listen for cloud-to-device messages"""
        while self.running and self.connected and self.device_client:
            try:
                # Non-blocking receive with timeout
                message = self.device_client.receive_message(timeout=1)
                if message:
                    print(f"📥 Received from cloud: {message.data}")
            except Exception as e:
                print(f"Error receiving message: {e}")
                time.sleep(1)

def main():
    # Create IoT device instance
    iot_device = IoTDevice(CONNECTION_STRING)
    
    try:
        # Connect to Azure IoT Hub
        if not iot_device.connect():
            print("❌ Failed to connect to Azure IoT Hub. Exiting...")
            return
        
        print("🚀 Starting telemetry transmission...")
        print("💡 Note: Background connection warnings are normal - messages are still being sent!")
        print("Press Ctrl+C to stop")
        print("-" * 70)
        
        # For now, let's skip the receive thread to reduce connection complexity
        # Uncomment the lines below if you need cloud-to-device messaging:
        # receive_thread = threading.Thread(target=iot_device.receive_messages, daemon=True)
        # receive_thread.start()
        
        # Start sending telemetry (blocking)
        iot_device.send_telemetry_loop()
        
    except KeyboardInterrupt:
        print("\n🛑 Application stopped by user")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
    finally:
        iot_device.disconnect()
        print("👋 Application terminated")

if __name__ == "__main__":
    main()
