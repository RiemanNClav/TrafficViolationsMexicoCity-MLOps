
from typing import Any, Text, Dict, List
from rasa_sdk import Action, Tracker
from rasa_sdk.executor import CollectingDispatcher
from rasa_sdk.events import SlotSet
from actions.api import ApiRequest
from src.prediction.pipeline_prediction import PredictPipeline, CustomData


class ActionGetAddressLatLong(Action):

    def name(self) -> Text:
        return "action_get_address"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, 
            domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:

        # Obtener los valores de los slots

        latitud = tracker.get_slot('latitud')
        longitud = tracker.get_slot('longitud')
        colonia = tracker.get_slot('colonia')
        alcaldia = tracker.get_slot('alcaldia')
        color = tracker.get_slot('color')
        marca = tracker.get_slot('marca')

        api = ApiRequest()

        if (colonia == None and alcaldia == None):

            try:
                address = api.api_request_object_1(latitud, longitud)
                alcaldia, colonia, dia, mes = address
            except:
                response = 'No se encontraron las coordenadas que proporcionaste'
                dispatcher.utter_message(text=response)
                return [SlotSet("latitud", None), SlotSet("longitud", None), SlotSet("colonia", None), SlotSet("alcaldia", None), SlotSet("color", None), SlotSet("marca", None)]

        else:

            try:
                address = api.api_request_object_2(alcaldia, colonia)
                latitud, longitud, dia, mes = address
            except:
                response = 'No se encontró la dirreción que proporcionaste'
                dispatcher.utter_message(text=response)
                return [SlotSet("latitud", None), SlotSet("longitud", None), SlotSet("colonia", None), SlotSet("alcaldia", None), SlotSet("color", None), SlotSet("marca", None)]
            
        
        data = CustomData(mes, dia, marca, colonia, alcaldia) 
        pred_df = data.get_data_as_data_frame()
        predict_pipeline=PredictPipeline()
        prediccion = predict_pipeline.predict(pred_df)

        response = f"Los datos basados en estas coordenadas ({latitud}, {longitud}) son: \n"
        response += f"Alcaldia: {alcaldia} \n"
        response += f"Colonia: {colonia} \n"
        response += f"Dia: {dia} \n"
        response += f"Mes: {mes} \n"
        response += f"Color del Automovil: {color} \n"
        response += f"Marca del Automovil: {marca} \n"
        response += f"El incidente de tránsito que es más probable que cometas de acuerdo a estos registros es : {prediccion} \n"

        dispatcher.utter_message(text=response)
        return [SlotSet("latitud", None), SlotSet("longitud", None), SlotSet("colonia", None), SlotSet("alcaldia", None), SlotSet("color", None), SlotSet("marca", None)]