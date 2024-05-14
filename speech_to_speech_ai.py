import threading
from deepgram import (
    DeepgramClient,
    DeepgramClientOptions,
    LiveTranscriptionEvents,
    LiveOptions,
)
from groq import Groq
from openai import OpenAI
import wave
import paho.mqtt.client as paho
from paho import mqtt
import os
from dotenv import load_dotenv
# import httpx

# MQTT Broker settings
mqtt_input_channel = "pixa/input/"
mqtt_output_channel = "pixa/output/"

class SpeechToText: 
    config = DeepgramClientOptions(
        options={
            "keepalive": "true",
        }
    )
    
    def __init__(self,mqtt_client):
        print("[SpeechToText]:[init]")
                
        self.llm = LLMTextToText(mqtt_client)
        
        self.text_generated = ""
        
        self.lock = threading.Lock()
        
        api_key = os.getenv("DEEPGRAM_API_KEY")
    
        self.deepgram = DeepgramClient(api_key, self.config)
        
        self.dg_connection = self.deepgram.listen.live.v("1")
         
        self._configure_deepgram()
                
    def _configure_deepgram(self):
        print("[SpeechToText]:[configure] Configuring deepgram connection")
        try:
            self.dg_connection.on(LiveTranscriptionEvents.Transcript, self._on_message)
            self.dg_connection.on(LiveTranscriptionEvents.Metadata, self._on_metadata)
            self.dg_connection.on(LiveTranscriptionEvents.Error, self._on_error)

            options = LiveOptions(
                model="nova-2", 
                language="en-US", 
                smart_format=True,
                encoding="linear16",
                sample_rate="16000",
            )
                        
            self.dg_connection.start(options)            
        except Exception as e:
            print(f"\n\n[SpeechToText]:[configure]: Error: {e}\n\n")
            raise e
        
    def start_conversion(self, bytes_data):
        try:    
            thread = threading.Thread(target=self._send_bytes_to_convert, args=(bytes_data,))
            thread.start() 
        except Exception as e:
            print(f"\n\n[SpeechToText]:[start_conversion]: Error: {e}\n\n")
            raise e
        
    def _send_bytes_to_convert(self,bytes_data):  
        try:            
            self.dg_connection.send(bytes_data)
        except Exception as e:
            print(f"\n\n[SpeechToText]:[_send_bytes_to_convert]: Error: {e}\n\n")
            
            if self.text_generated != "":
                bytes_data = b"##"
            
            raise e
        finally:
            self.lock.acquire()
            is_speech_completed = len(bytes_data)<=2
            if is_speech_completed:
                print("\n[SpeechToText]:[_send_bytes_to_convert]: End signal detected")
                print(f"\n[SpeechToText]:[_send_bytes_to_convert]: full sentence {self.text_generated}\n")

                if self.text_generated != "":
                    self.llm.start_conversion(self.text_generated)
                    self.text_generated = "" 
                
                self.lock.release()
                return
            self.lock.release()
        
    def _on_message(self,  *args, **kwargs):
        try:
            sentence = kwargs.get('result').channel.alternatives[0].transcript
            
            if len(sentence) == 0:
                return
            print(f"new sentence: {sentence}")
        
            self.text_generated += " " + sentence
        except Exception as e:
            print(f"\n\n[SpeechToText]:[_on_message]: Error: {e}\n\n")

    def _on_metadata(self, *args, **kwargs):
        print(f"\n\n[SpeechToText]:[_on_metadata]: Meta-Data:{kwargs.get('metadata')}\n\n")

    def _on_error(self,  *args, **kwargs):
        print(f"\n\n[SpeechToText]:[_on_error]: Meta-Data:{kwargs.get('error')}\n\n")
    
    def dispose_deepgram(self):
        try:
            self.dg_connection.finish()
        except Exception as e:
            print(f"\n\n[SpeechToText]:[dispose_deepgram]: Error: {e}\n\n")
            raise e
            
class LLMTextToText:
    def __init__(self,mqtt_client):
        self.tts = TextToSpeech(mqtt_client)
        
        api_key = os.getenv("GROQ_API_KEY")
        
        self.client = Groq(
            api_key=api_key,
        )
        
    def start_conversion(self, content):
        try:
            self._send_content(content)
            # thread = threading.Thread(target=self._send_content, args=(content,))
            # thread.start() 
        except Exception as e:
            print(f"\n\n[LLMTextToText]:[start_conversion]: Error: {e}\n\n")
            raise e
    
    def _send_content(self,content):
        try:
            chat_completion = self.client.chat.completions.create(
                messages=[
                    {
                        "role": "user",
                        "content": content,
                    }
                ],
                model="mixtral-8x7b-32768",
            )
            response = chat_completion.choices[0].message.content
            print(f"[LLMTextToText]:[_send_content]: AI Response: {response}")
            
            self.tts.start_conversion(response)
        except Exception as e:
            print(f"\n\n[LLMTextToText]:[_send_content]: Error: {e}\n\n")
            raise e
        
class TextToSpeech:
    buffer_size = 2048
        
    def __init__(self,mqtt_client):
        self.mqtt_client = mqtt_client
        
        api_key = os.getenv("OPENAI_API_KEY")
        
        self.client = OpenAI(api_key=api_key)
        
    def start_conversion(self, text_data):
        try:
            self._send_text_to_convert(text_data)
            # thread = threading.Thread(target=self._send_text_to_convert, args=(text_data,))
            # thread.start()
        except Exception as e:
            print(f"\n\n[TextToSpeech]:[start_conversion] Error: {e}\n\n")
            raise e
    
    def _send_text_to_convert(self,textData):
        try:
            self.response = self.client.audio.speech.create(
                model="tts-1",
                voice="alloy",
                input=textData,
                response_format="pcm",
            )
            index = 0
            for chunk in self.response.iter_bytes(chunk_size=self.buffer_size):
                self.mqtt_client.publish(f"{mqtt_output_channel}{index}", chunk)
                index += 1
            #Send last chunk
            self.mqtt_client.publish(f"{mqtt_output_channel}{index}", b"##")
        except Exception as e:
            print(f"\n\n[TextToSpeech]:[send_to_convert] Response Error: {e} {self.response.text}\n\n")
            raise e
    
    def write_to_wav(self,chunks):
        SAMPLE_RATE = 11000
        SAMPLE_WIDTH = 2
        CHANNELS = 2

        """Write audio chunks to a WAV file."""
        with wave.open(f"output{len(chunks)}.wav", 'wb') as wav_file:
            wav_file.setnchannels(CHANNELS)
            wav_file.setsampwidth(SAMPLE_WIDTH)
            wav_file.setframerate(SAMPLE_RATE)
            for chunk in chunks:
                wav_file.writeframes(chunk)
                
class MQTTConnection:
    def __init__(self):
        print(f"[MQTTConnection]:[__init__]")
        
        # MQTT Client setup
        try:
            self.client = paho.Client(paho.CallbackAPIVersion.VERSION2,client_id="", userdata=None, protocol=paho.MQTTv5)
            self.client.tls_set(tls_version=mqtt.client.ssl.PROTOCOL_TLS)
            
            self.speechToText = SpeechToText(self.client)
            
            mqtt_broker = os.getenv("MQTT_BROKER")
            mqtt_username = os.getenv("MQTT_USERNAME")
            mqtt_password = os.getenv("MQTT_PASSWORD")
            mqtt_port = int(os.getenv("MQTT_PORT"))
            
            self.client.username_pw_set(mqtt_username, mqtt_password)
            self.client.on_connect = self.on_connect
            self.client.on_subscribe = self.on_subscribe
            self.client.on_message = self.on_message
            self.client.connect(mqtt_broker, mqtt_port)
            self.client.loop_forever()
        except Exception as e:
            raise e
    
    def on_connect(self, client, user_data, flags, reason_code, properties=None):
        print(f"[MQTTConnection]:[on_connect]: mqtt client connected")
        
        client.subscribe(f"{mqtt_input_channel}#",qos=1)
        # self.audioStreamGenerationThread()
        
    def on_subscribe(self,client, userdata, mid, granted_qos, properties=None):
        print(f"[MQTTConnection]:[on_subscribe]: Subscribed: " + str(mid) + " " + str(granted_qos))
        
    def on_message(self, client, user_data, msg):
        # print(f"[StreamConsumer]:[on_mqtt_client_message]: Received new mqtt client message on topic:{msg.topic}: type:{type(msg.payload)} - {len(msg.payload)} bytes")
        self.speechToText.start_conversion(msg.payload)
        
    # def audioStreamGenerationThread(self):
    #     with httpx.stream("GET", "http://stream.live.vc.bbcmedia.co.uk/bbc_world_service") as r:
    #         for data in r.iter_bytes():
    #             self.speechToText.start_conversion(data)
    
if __name__ == "__main__":
    try:
        load_dotenv()
                
        mqtt = MQTTConnection()
    except Exception as e:
        print(f"\n\nError Found : {e}\n\n") 
    
    
  
  
  
  
  
  
  
  






























  
    
# class TextToSpeech:
#     api_key = '80d183038f129c6c198b63de02ca311e'
#     voice_id = "yl2ZDV1MzN4HbQJbMihG"
    
#     #TODO: change to pcm
#     url = f"https://api.elevenlabs.io/v1/text-to-speech/{voice_id}/stream?optimize_streaming_latency=4&output_format=mp3_44100_128"
    
#     headers = {
#         "Content-Type": "application/json",
#         "xi-api-key": api_key,
#     }
    
#     def __init__(self):
#         self.is_running = False
#         self.queue = Queue()
        
#     def start_conversion(self, text_data):
#         print(f"[TextToSpeech]:[start_conversion]")
        
#         thread = threading.Thread(target=self._send_to_convert, args=(text_data,))
#         thread.start()
    
#     def _send_to_convert(self,textData):
#         payload = {
#             "text": textData,
#             "model_id": "eleven_turbo_v2",
#             "voice_settings": {
#                 "stability": 0.5,
#                 "similarity_boost": 0.8,
#                 "style": 0.0,
#                 "use_speaker_boost": True
#             },
#             # "previous_text": "<string>",
#             # "next_text": "<string>",
#             # "previous_request_ids": ["<string>"],
#             # "next_request_ids": ["<string>"]
#         }

#         response = requests.request("POST", self.url, json=payload, headers=self.headers)

#         if response.ok:
#             self.is_running = True
#             for chunk in response.iter_content(chunk_size=1024):
#                 if not self.is_running:
#                     print("[TextToSpeech]:[send_to_convert] Stoped listing server chunks")
#                     break
                
#                 self.queue.put(chunk)
#                 print("[TextToSpeech]:[send_to_convert] Adding new chunk")
                
#             self.queue.put(None)  # Signal that the stream is done
#             print("[TextToSpeech]:[send_to_convert] Adding last None chunk")
#             self._save_audio()
#         else:
#             print(f"[TextToSpeech]:[send_to_convert] Response Error: {response.text}")
            
#     def _save_audio(self):
#         try:
#             with open('output.mp3', "wb") as f:
#                 while True:
#                     chunk = self.queue.get()
                    
#                     if chunk is None:
#                         break
                    
#                     f.write(chunk)
#             print(f"Audio saved to output.mp3")
#         except Exception as e:
#             print(f"[TextToSpeech]:[save_audio]: Error: {e}")

#     def stop_conversion(self):
#         print("[TextToSpeech]:[stop_conversion]")
#         self.is_running = False
