#Scheduler servicee
from flask import Flask, jsonify
import datetime

app = Flask(__name__)

@app.route("/health")
def health():
    return jsonify({
        "service": "scheduler-service",
        "status": "healthy",
        "timestamp": datetime.datetime.utcnow().isoformat()
    })

@app.route("/")
def home():
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Scheduler Service</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 20px; background: #f9f9f9; }
            .card { padding: 20px; border-radius: 8px; background: white;
                    box-shadow: 0 2px 8px rgba(0,0,0,0.1); max-width: 500px; }
        </style>
    </head>
    <body>
        <div class="card">
            <h1>Scheduler Service ✅</h1>
            <p>Status: Healthy</p>
            <p>Timestamp: <span id="timestamp"></span></p>
        </div>
        <script>
            async function loadHealth() {
                let res = await fetch("/health");
                let data = await res.json();
                document.getElementById("timestamp").textContent = data.timestamp;
            }
            loadHealth();
        </script>
    </body>
    </html>
    """

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8004)
