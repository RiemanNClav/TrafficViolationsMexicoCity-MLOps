
from typing import Any, Text, Dict, List
from rasa_sdk import Action, Tracker
from rasa_sdk.executor import CollectingDispatcher
import json


# class ActionHelloWorld(Action):
#     def name(self) -> Text:
#         return "action_hello_world"
    
#     def run(self, dispatcher: CollectingDispatcher,
#             tracker: Tracker,
#             domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
#          dispatcher.utter_message(text="Hello World!")
#          return []

# class ActionObtenerUbicacion(Action):
#     def name(self):
#         return "action_obtener_ubicacion"

#     def run(self, dispatcher, tracker, domain):
#         latitud = tracker.get_slot('latitud')
#         longitud = tracker.get_slot('longitud')

#         direccion = obtener_direccion(latitud, longitud)

#         dispatcher.utter_message(text=f"Te encuentras en {direccion}")
#         return []


# class ActionHandleLocation(Action):

#     def name(self) -> str:
#         return "action_handle_location"

#     def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: dict):
#         # Aquí simulamos que el chatbot recibe un JSON con la ubicación
#         last_message = tracker.latest_message['text']
        
#         # Supongamos que el mensaje es un JSON con coordenadas
#         try:
#             location_data = json.loads(last_message)
#             latitude = location_data['location']['latitude']
#             longitude = location_data['location']['longitude']

#             # Aquí puedes hacer lo que quieras con las coordenadas
#             dispatcher.utter_message(text=f"Recibí tu ubicación: Latitud {latitude}, Longitud {longitude}")
#         except (json.JSONDecodeError, KeyError):
#             dispatcher.utter_message(text="No pude entender la ubicación.")
        
#         return []