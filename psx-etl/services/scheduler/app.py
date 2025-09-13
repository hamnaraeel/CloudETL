from flask import Flask, jsonify
import datetime
import requests
import schedule
import time
import threading
import logging

app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
VISUALIZATION_SERVICE_URL = "http://visualization-service:8002"
EXTRACT_SERVICE_URL = "http://extract-service:8000"
TRANSFORM_SERVICE_URL = "http://transform-service:8001"

# Job status tracking
job_status = {
    "last_run": None,
    "next_run": None,
    "status": "initialized",
    "runs_count": 0,
    "errors": []
}

def trigger_data_update():
    """Function to trigger data updates in the pipeline"""
    try:
        logger.info("Starting scheduled data update...")
        job_status["status"] = "running"
        
        services_triggered = 0
        
        # 1. EXTRACT SERVICE - Trigger data extraction
        try:
            logger.info("Triggering Extract Service...")
            # Try the actual endpoint that exists in your extract service
            response = requests.get(f"{EXTRACT_SERVICE_URL}/extract_many", 
                       params={"ticker": "AAPL", "period": "1mo"},  # Use params, not json
                       timeout=30)
            
            if response.status_code in [200, 201]:
                logger.info("âœ… Successfully triggered Extract Service")
                services_triggered += 1
            else:
                logger.warning(f"âš ï¸ Extract service returned status: {response.status_code}")
                
        except requests.exceptions.RequestException as e:
            logger.warning(f"âš ï¸ Extract service connection error: {str(e)}")

        # 2. TRANSFORM SERVICE - Trigger data processing
        try:
            logger.info("Triggering Transform Service...")
            # Use the actual working endpoint from your logs
            response = requests.post(f"{TRANSFORM_SERVICE_URL}/transform_batch", 
                                   json= {"tickers": ["AAPL", "GOOGL", "MSFT"]}, # Send required tickers for batch processing
                                   timeout=30)
            
            if response.status_code in [200, 201]:
                logger.info("âœ… Successfully triggered Transform Service")
                services_triggered += 1
            else:
                logger.warning(f"âš ï¸ Transform service returned status: {response.status_code}")
                
        except requests.exceptions.RequestException as e:
            logger.warning(f"âš ï¸ Transform service connection error: {str(e)}")

        # 3. VISUALIZATION SERVICE - Trigger dashboard refresh
        try:
            logger.info("Triggering Visualization Service...")
            response = requests.get(f"{VISUALIZATION_SERVICE_URL}/refresh", timeout=10)
            
            if response.status_code == 200:
                logger.info("âœ… Successfully triggered Visualization Service")
                services_triggered += 1
            else:
                logger.warning(f"âš ï¸ Visualization service returned status: {response.status_code}")
                
        except requests.exceptions.RequestException as e:
            logger.warning(f"âš ï¸ Visualization service connection error: {str(e)}")

        # 4. OPTIONAL: Chain the services in proper order
        # Uncomment this section if you want to ensure proper data flow sequence
        """
        # Wait a bit between services to ensure data flows properly
        if services_triggered > 0:
            logger.info("Waiting 5 seconds between service triggers...")
            time.sleep(5)
        """
        
        # Update job status
        job_status["last_run"] = datetime.datetime.utcnow().isoformat()
        job_status["runs_count"] += 1
        
        if services_triggered >= 2:  # At least 2 out of 3 services
            job_status["status"] = "completed"
            logger.info(f"âœ… Scheduled update completed - triggered {services_triggered}/3 services")
        elif services_triggered > 0:
            job_status["status"] = "partial"
            logger.warning(f"âš ï¸ Partial update - triggered {services_triggered}/3 services")
        else:
            job_status["status"] = "error"
            logger.error("âŒ No services could be triggered")
        
    except Exception as e:
        logger.error(f"Error in scheduled data update: {str(e)}")
        job_status["status"] = "error"
        job_status["errors"].append({
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "error": str(e)
        })
        # Keep only last 10 errors
        if len(job_status["errors"]) > 10:
            job_status["errors"] = job_status["errors"][-10:]

def trigger_specific_service(service_name):
    """Trigger a specific service manually"""
    try:
        if service_name == "extract":
            response = requests.get(f"{EXTRACT_SERVICE_URL}/extract_many", 
                                   params={"ticker": "AAPL", "period": "1mo"}, timeout=30)
            return response.status_code in [200, 201]
            
        elif service_name == "transform":
            response = requests.post(f"{TRANSFORM_SERVICE_URL}/transform_batch", 
                                   json={"tickers": ["AAPL", "GOOGL", "MSFT"]}, timeout=30)
            return response.status_code in [200, 201]
            
        elif service_name == "visualization":
            response = requests.get(f"{VISUALIZATION_SERVICE_URL}/refresh", timeout=10)
            return response.status_code == 200
            
        return False
        
    except Exception as e:
        logger.error(f"Error triggering {service_name}: {str(e)}")
        return False

def run_scheduler():
    """Run the scheduler in a separate thread"""
    logger.info("Starting scheduler thread...")
    
    # Schedule the job to run every 30 seconds
    schedule.every(30).seconds.do(trigger_data_update)
    
    # Run initial update after 10 seconds (to allow services to start up)
    schedule.every().minute.at(":10").do(trigger_data_update).tag('initial')
    
    while True:
        try:
            # Update next run time
            jobs = schedule.get_jobs()
            if jobs:
                next_job = min(jobs, key=lambda job: job.next_run)
                job_status["next_run"] = next_job.next_run.isoformat()
            
            schedule.run_pending()
            time.sleep(1)
            
            # Remove the initial job after first run
            if job_status["runs_count"] >= 1:
                schedule.clear('initial')
                
        except Exception as e:
            logger.error(f"Error in scheduler thread: {str(e)}")
            time.sleep(5)  # Wait before retrying

@app.route("/health")
def health():
    return jsonify({
        "service": "scheduler-service",
        "status": "healthy",
        "timestamp": datetime.datetime.utcnow().isoformat(),
        "job_status": job_status
    })

@app.route("/status")
def status():
    """Get detailed scheduler status"""
    return jsonify({
        "service": "scheduler-service",
        "job_status": job_status,
        "scheduled_jobs": len(schedule.jobs),
        "timestamp": datetime.datetime.utcnow().isoformat()
    })

@app.route("/trigger")
def manual_trigger():
    """Manually trigger a data update"""
    try:
        # Run the update function in a separate thread to avoid blocking
        threading.Thread(target=trigger_data_update, daemon=True).start()
        return jsonify({
            "message": "Data update triggered manually",
            "timestamp": datetime.datetime.utcnow().isoformat()
        })
    except Exception as e:
        return jsonify({
            "error": f"Failed to trigger update: {str(e)}",
            "timestamp": datetime.datetime.utcnow().isoformat()
        }), 500

@app.route("/trigger/<service_name>")
def manual_trigger_service(service_name):
    """Manually trigger a specific service"""
    try:
        success = trigger_specific_service(service_name)
        if success:
            return jsonify({
                "message": f"Successfully triggered {service_name} service",
                "timestamp": datetime.datetime.utcnow().isoformat()
            })
        else:
            return jsonify({
                "error": f"Failed to trigger {service_name} service",
                "timestamp": datetime.datetime.utcnow().isoformat()
            }), 500
    except Exception as e:
        return jsonify({
            "error": f"Error triggering {service_name}: {str(e)}",
            "timestamp": datetime.datetime.utcnow().isoformat()
        }), 500

# Keep your existing home route with the beautiful dashboard
@app.route("/")
def home():
    return """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Professional Scheduler Service</title>
        <style>
            * {
                margin: 0;
                padding: 0;
                box-sizing: border-box;
            }

            body {
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
                background: linear-gradient(135deg, #1e3c72 0%, #2a5298 100%);
                min-height: 100vh;
                color: white;
                padding: 20px;
            }

            .header {
                text-align: center;
                margin-bottom: 40px;
            }

            .header h1 {
                font-size: 3.5rem;
                font-weight: 800;
                background: linear-gradient(45deg, #64b5f6, #42a5f5, #1e88e5, #1976d2);
                background-size: 400% 400%;
                -webkit-background-clip: text;
                -webkit-text-fill-color: transparent;
                background-clip: text;
                animation: gradient 3s ease-in-out infinite;
                margin-bottom: 10px;
                letter-spacing: -2px;
            }

            .header .subtitle {
                font-size: 1.2rem;
                opacity: 0.8;
                font-weight: 400;
            }

            @keyframes gradient {
                0%, 100% { background-position: 0% 50%; }
                50% { background-position: 100% 50%; }
            }

            .dashboard {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
                gap: 20px;
                max-width: 1200px;
                margin: 0 auto;
            }

            .card {
                background: rgba(255, 255, 255, 0.1);
                backdrop-filter: blur(10px);
                border: 1px solid rgba(255, 255, 255, 0.2);
                border-radius: 20px;
                padding: 30px;
                box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3);
                transition: transform 0.3s ease, box-shadow 0.3s ease;
            }

            .card:hover {
                transform: translateY(-5px);
                box-shadow: 0 15px 40px rgba(0, 0, 0, 0.4);
            }

            .card h2 {
                font-size: 1.5rem;
                margin-bottom: 20px;
                color: #e3f2fd;
                display: flex;
                align-items: center;
                gap: 10px;
            }

            .status-grid {
                display: grid;
                grid-template-columns: repeat(2, 1fr);
                gap: 15px;
                margin-bottom: 20px;
            }

            .status-item {
                background: rgba(255, 255, 255, 0.05);
                padding: 15px;
                border-radius: 12px;
                border-left: 4px solid #42a5f5;
            }

            .status-item label {
                font-size: 0.9rem;
                opacity: 0.7;
                display: block;
                margin-bottom: 5px;
            }

            .status-item .value {
                font-size: 1.1rem;
                font-weight: 600;
                color: #e3f2fd;
            }

            .status-indicator {
                display: inline-block;
                width: 12px;
                height: 12px;
                border-radius: 50%;
                margin-right: 8px;
            }

            .status-indicator.running {
                background: #ffc107;
                box-shadow: 0 0 10px #ffc107;
                animation: pulse 2s infinite;
            }

            .status-indicator.completed {
                background: #4caf50;
                box-shadow: 0 0 10px #4caf50;
            }

            .status-indicator.error {
                background: #f44336;
                box-shadow: 0 0 10px #f44336;
            }

            .status-indicator.partial {
                background: #ff9800;
                box-shadow: 0 0 10px #ff9800;
            }

            @keyframes pulse {
                0%, 100% { opacity: 1; }
                50% { opacity: 0.5; }
            }

            .refresh-btn {
                background: linear-gradient(45deg, #1976d2, #42a5f5);
                border: none;
                color: white;
                padding: 15px 30px;
                border-radius: 25px;
                font-size: 1rem;
                font-weight: 600;
                cursor: pointer;
                transition: all 0.3s ease;
                width: 100%;
                display: flex;
                align-items: center;
                justify-content: center;
                gap: 10px;
                margin-bottom: 10px;
            }

            .refresh-btn:hover {
                background: linear-gradient(45deg, #1565c0, #1e88e5);
                transform: translateY(-2px);
                box-shadow: 0 8px 25px rgba(25, 118, 210, 0.4);
            }

            .refresh-btn:active {
                transform: translateY(0);
            }

            .service-btn {
                background: linear-gradient(45deg, #6a4c93, #9b59b6);
                border: none;
                color: white;
                padding: 12px 25px;
                border-radius: 20px;
                font-size: 0.9rem;
                font-weight: 600;
                cursor: pointer;
                transition: all 0.3s ease;
                width: 100%;
                margin-bottom: 8px;
            }

            .service-btn:hover {
                background: linear-gradient(45deg, #5a3d7a, #8e44ad);
                transform: translateY(-1px);
            }

            .errors-container {
                max-height: 200px;
                overflow-y: auto;
                background: rgba(0, 0, 0, 0.2);
                border-radius: 12px;
                padding: 15px;
            }

            .error-item {
                background: rgba(244, 67, 54, 0.1);
                border: 1px solid rgba(244, 67, 54, 0.3);
                border-radius: 8px;
                padding: 10px;
                margin-bottom: 10px;
                font-size: 0.9rem;
            }

            .error-time {
                opacity: 0.7;
                font-size: 0.8rem;
                margin-bottom: 5px;
            }

            .stats-row {
                display: flex;
                justify-content: space-between;
                align-items: center;
                margin-bottom: 15px;
            }

            .metric {
                text-align: center;
            }

            .metric-value {
                font-size: 2rem;
                font-weight: 700;
                color: #42a5f5;
                line-height: 1;
            }

            .metric-label {
                font-size: 0.9rem;
                opacity: 0.7;
                margin-top: 5px;
            }

            @media (max-width: 768px) {
                .header h1 {
                    font-size: 2.5rem;
                }
                .status-grid {
                    grid-template-columns: 1fr;
                }
                .dashboard {
                    grid-template-columns: 1fr;
                }
            }
        </style>
    </head>
    <body>
        <div class="header">
            <h1>ðŸš€ AUTOMATED SCHEDULER</h1>
            <p class="subtitle">âš¡ Real-Time Pipeline Orchestration & Advanced Automation âš¡</p>
        </div>

        <div class="dashboard">
            <div class="card">
                <h2>ðŸ“Š System Status</h2>
                <div class="stats-row">
                    <div class="metric">
                        <div class="metric-value" id="runs-count">0</div>
                        <div class="metric-label">Total Runs</div>
                    </div>
                    <div class="metric">
                        <div class="metric-value">30s</div>
                        <div class="metric-label">Interval</div>
                    </div>
                </div>
                
                <div class="status-grid">
                    <div class="status-item">
                        <label>Service Status</label>
                        <div class="value">
                            <span class="status-indicator completed"></span>
                            Healthy
                        </div>
                    </div>
                    <div class="status-item">
                        <label>Job Status</label>
                        <div class="value" id="job-status-display">
                            <span class="status-indicator" id="job-indicator"></span>
                            <span id="job-status">Initialized</span>
                        </div>
                    </div>
                    <div class="status-item">
                        <label>Last Run</label>
                        <div class="value" id="last-run">Never</div>
                    </div>
                    <div class="status-item">
                        <label>Next Run</label>
                        <div class="value" id="next-run">Calculating...</div>
                    </div>
                </div>

                <button class="refresh-btn" onclick="triggerUpdate()">
                    ðŸ”„ Trigger Full Pipeline
                </button>
            </div>

            <div class="card">
                <h2>ðŸŽ¯ Individual Services</h2>
                <button class="service-btn" onclick="triggerService('extract')">
                    ðŸ“¥ Trigger Extract Service
                </button>
                <button class="service-btn" onclick="triggerService('transform')">
                    âš™ï¸ Trigger Transform Service
                </button>
                <button class="service-btn" onclick="triggerService('visualization')">
                    ðŸ“Š Trigger Visualization Service
                </button>
            </div>

            <div class="card">
                <h2>âš ï¸ System Logs</h2>
                <div class="errors-container" id="errors">
                    <p style="text-align: center; opacity: 0.7; padding: 20px;">
                        No recent errors - System running smoothly âœ…
                    </p>
                </div>
            </div>
        </div>

        <script>
            async function loadStatus() {
                try {
                    let res = await fetch("/health");
                    let data = await res.json();
                    
                    document.getElementById("job-status").textContent = 
                        data.job_status.status.charAt(0).toUpperCase() + data.job_status.status.slice(1);
                    document.getElementById("runs-count").textContent = data.job_status.runs_count;
                    document.getElementById("last-run").textContent = 
                        data.job_status.last_run ? new Date(data.job_status.last_run).toLocaleString() : "Never";
                    document.getElementById("next-run").textContent = 
                        data.job_status.next_run ? new Date(data.job_status.next_run).toLocaleString() : "Calculating...";
                    
                    // Update status indicator
                    const indicator = document.getElementById("job-indicator");
                    indicator.className = `status-indicator ${data.job_status.status}`;
                    
                    // Update errors
                    const errorsDiv = document.getElementById("errors");
                    if (data.job_status.errors && data.job_status.errors.length > 0) {
                        errorsDiv.innerHTML = data.job_status.errors
                            .slice(-5)
                            .reverse()
                            .map(err => `
                                <div class="error-item">
                                    <div class="error-time">${new Date(err.timestamp).toLocaleString()}</div>
                                    <div>${err.error}</div>
                                </div>
                            `).join("");
                    } else {
                        errorsDiv.innerHTML = `
                            <p style="text-align: center; opacity: 0.7; padding: 20px;">
                                No recent errors - System running smoothly âœ…
                            </p>
                        `;
                    }
                    
                } catch (error) {
                    console.error("Failed to load status:", error);
                }
            }
            
            async function triggerUpdate() {
                const btn = document.querySelector('.refresh-btn');
                const originalText = btn.innerHTML;
                btn.innerHTML = 'â³ Triggering...';
                btn.disabled = true;
                
                try {
                    const response = await fetch("/trigger");
                    const data = await response.json();
                    
                    if (response.ok) {
                        btn.innerHTML = 'âœ… Pipeline Triggered!';
                        setTimeout(() => {
                            btn.innerHTML = originalText;
                            btn.disabled = false;
                        }, 3000);
                    } else {
                        btn.innerHTML = 'âŒ Failed to Trigger';
                        setTimeout(() => {
                            btn.innerHTML = originalText;
                            btn.disabled = false;
                        }, 2000);
                    }
                } catch (error) {
                    btn.innerHTML = 'âŒ Connection Error';
                    setTimeout(() => {
                        btn.innerHTML = originalText;
                        btn.disabled = false;
                    }, 2000);
                }
                
                // Refresh status after triggering
                setTimeout(loadStatus, 1000);
            }
            
            async function triggerService(serviceName) {
                const buttons = document.querySelectorAll('.service-btn');
                const btn = Array.from(buttons).find(b => b.textContent.toLowerCase().includes(serviceName));
                const originalText = btn.innerHTML;
                
                btn.innerHTML = `â³ Triggering ${serviceName}...`;
                btn.disabled = true;
                
                try {
                    const response = await fetch(`/trigger/${serviceName}`);
                    const data = await response.json();
                    
                    if (response.ok) {
                        btn.innerHTML = `âœ… ${serviceName} Triggered!`;
                        setTimeout(() => {
                            btn.innerHTML = originalText;
                            btn.disabled = false;
                        }, 2000);
                    } else {
                        btn.innerHTML = `âŒ ${serviceName} Failed`;
                        setTimeout(() => {
                            btn.innerHTML = originalText;
                            btn.disabled = false;
                        }, 2000);
                    }
                } catch (error) {
                    btn.innerHTML = `âŒ ${serviceName} Error`;
                    setTimeout(() => {
                        btn.innerHTML = originalText;
                        btn.disabled = false;
                    }, 2000);
                }
                
                // Refresh status after triggering
                setTimeout(loadStatus, 1000);
            }
            
            // Load status initially and then every 3 seconds
            loadStatus();
            setInterval(loadStatus, 3000);
        </script>
    </body>
    </html>
    """

if __name__ == "__main__":
    # Start the scheduler in a separate thread
    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()
    
    # Start the Flask app
    app.run(host="0.0.0.0", port=8004, debug=False)
