#from flask import Flask, request
#from twilio.twiml.messaging_response import MessagingResponse # Nueva línea
#import sqlite3
#from datetime import datetime

app = Flask(__name__)

# (La función init_db se queda igual)

@app.route('/webhook', methods=['POST'])
def webhook():
    telefono = request.values.get('From') 
    mensaje = request.values.get('Body')
    fecha_actual = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # --- GUARDAR EN BASE DE DATOS ---
    try:
        conn = sqlite3.connect('servicios.db')
        cursor = conn.cursor()
        cursor.execute("INSERT INTO pedidos (telefono, mensaje, fecha_hora) VALUES (?, ?, ?)",
                       (telefono, mensaje, fecha_actual))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Error BD: {e}")

    # --- RESPONDER AL USUARIO ---
    resp = MessagingResponse()
    
    # Aquí personalizas la respuesta
    respuesta_texto = "¡Hola! 🚗 Bienvenido a Transporte Ejecutivo. Hemos recibido tu solicitud y un asesor te contactará en breve."
    
    resp.message(respuesta_texto)

    print(f"✅ Mensaje de {telefono} guardado y respuesta enviada.")
    
    return str(resp) # Devolvemos el TwiML a Twilio

if __name__ == '__main__':
    # init_db()
    app.run(port=5000, debug=True)