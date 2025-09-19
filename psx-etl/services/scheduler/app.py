from flask import Flask, jsonify
import datetime
import requests
import schedule
import time
import threading
import logging
import os

app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
VISUALIZATION_SERVICE_URL = os.getenv('VISUALIZATION_SERVICE_URL', 'http://visualization-service:8002')
LOAD_SERVICE_URL = os.getenv('LOAD_SERVICE_URL', 'http://load-service:8003')

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
        
        # 1. LOAD SERVICE - Trigger data loading to warehouse
        try:
            logger.info("Triggering Load Service...")
            response = requests.post(f"{LOAD_SERVICE_URL}/load/batch", 
                                   json={"tickers": "AAPL,GOOGL,MSFT", "period": "1mo"},
                                   timeout=30)
            
            if response.status_code in [200, 201]:
                logger.info("Successfully triggered Load Service")
                services_triggered += 1
            else:
                logger.warning(f"Load service returned status: {response.status_code}")
                
        except requests.exceptions.RequestException as e:
            logger.warning(f"Load service connection error: {str(e)}")

        # 2. VISUALIZATION SERVICE - Trigger dashboard refresh
        try:
            logger.info("Triggering Visualization Service...")
            response = requests.get(f"{VISUALIZATION_SERVICE_URL}/refresh", timeout=10)
            
            if response.status_code == 200:
                logger.info("Successfully triggered Visualization Service")
                services_triggered += 1
            else:
                logger.warning(f"Visualization service returned status: {response.status_code}")
                
        except requests.exceptions.RequestException as e:
            logger.warning(f"Visualization service connection error: {str(e)}")
        
        # Update job status
        job_status["last_run"] = datetime.datetime.utcnow().isoformat()
        job_status["runs_count"] += 1
        
        if services_triggered >= 2:  # Both services successful
            job_status["status"] = "completed"
            logger.info(f"Scheduled update completed - triggered {services_triggered}/2 services")
        elif services_triggered > 0:
            job_status["status"] = "partial"
            logger.warning(f"Partial update - triggered {services_triggered}/2 services")
        else:
            job_status["status"] = "error"
            logger.error("No services could be triggered")
        
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
        if service_name == "load":
            response = requests.post(f"{LOAD_SERVICE_URL}/load/batch", 
                                   json={"tickers": "AAPL,GOOGL,MSFT", "period": "1mo"}, timeout=30)
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
    """Comprehensive health check for scheduler service"""
    import time
    import psutil
    
    start_time = time.time()
    
    checks = {
        "service": "scheduler-service",
        "version": "1.0.0",
        "timestamp": datetime.datetime.utcnow().isoformat(),
        "checks": {}
    }
    
    # Test 1: System resource check
    try:
        memory_percent = psutil.virtual_memory().percent
        cpu_percent = psutil.cpu_percent(interval=0.1)
        
        if memory_percent > 90:
            checks["checks"]["memory"] = "unhealthy - high usage"
        elif memory_percent > 80:
            checks["checks"]["memory"] = "degraded - moderate usage"
        else:
            checks["checks"]["memory"] = "healthy"
        checks["memory_usage_percent"] = round(memory_percent, 1)
        
        if cpu_percent > 90:
            checks["checks"]["cpu"] = "unhealthy - high usage"
        elif cpu_percent > 80:
            checks["checks"]["cpu"] = "degraded - moderate usage"
        else:
            checks["checks"]["cpu"] = "healthy"
        checks["cpu_usage_percent"] = round(cpu_percent, 1)
        
    except Exception as e:
        checks["checks"]["system"] = f"degraded - {str(e)[:50]}"
    
    # Test 2: Scheduler status
    try:
        jobs_count = len(schedule.jobs)
        if jobs_count > 0:
            checks["checks"]["scheduler"] = "healthy"
            checks["active_jobs"] = jobs_count
        else:
            checks["checks"]["scheduler"] = "degraded - no scheduled jobs"
            checks["active_jobs"] = 0
            
    except Exception as e:
        checks["checks"]["scheduler"] = f"unhealthy - {str(e)[:50]}"
    
    # Test 3: Job execution history
    try:
        runs_count = job_status.get("runs_count", 0)
        last_run = job_status.get("last_run")
        current_status = job_status.get("status", "unknown")
        
        # Check if scheduler has been running
        if runs_count == 0:
            checks["checks"]["job_execution"] = "degraded - no runs yet"
        elif current_status == "error":
            checks["checks"]["job_execution"] = "unhealthy - last job failed"
        elif current_status in ["running", "completed", "partial"]:
            checks["checks"]["job_execution"] = "healthy"
        else:
            checks["checks"]["job_execution"] = f"degraded - status: {current_status}"
            
        checks["total_runs"] = runs_count
        checks["last_run"] = last_run
        
    except Exception as e:
        checks["checks"]["job_execution"] = f"degraded - {str(e)[:50]}"
    
    # Test 4: Service connectivity checks
    services_health = {
        "load_service": "unknown",
        "visualization_service": "unknown"
    }
    
    # Test Load Service
    try:
        response = requests.get(f"{LOAD_SERVICE_URL}/health", timeout=2)
        if response.status_code == 200:
            services_health["load_service"] = "healthy"
        else:
            services_health["load_service"] = f"degraded - status {response.status_code}"
    except Exception as e:
        services_health["load_service"] = f"unhealthy - {str(e)[:30]}"
    
    # Test Visualization Service  
    try:
        response = requests.get(f"{VISUALIZATION_SERVICE_URL}/health", timeout=2)
        if response.status_code == 200:
            services_health["visualization_service"] = "healthy"
        else:
            services_health["visualization_service"] = f"degraded - status {response.status_code}"
    except Exception as e:
        services_health["visualization_service"] = f"unhealthy - {str(e)[:30]}"
    
    checks["checks"]["service_connectivity"] = services_health
    
    # Evaluate service connectivity health
    unhealthy_services = sum(1 for status in services_health.values() if "unhealthy" in status)
    degraded_services = sum(1 for status in services_health.values() if "degraded" in status)
    
    if unhealthy_services > 1:
        checks["checks"]["services_overall"] = "unhealthy - multiple services down"
    elif unhealthy_services > 0 or degraded_services > 1:
        checks["checks"]["services_overall"] = "degraded - some services struggling"  
    else:
        checks["checks"]["services_overall"] = "healthy"
    
    # Test 5: Response time check
    response_time = (time.time() - start_time) * 1000
    checks["response_time_ms"] = round(response_time, 2)
    
    if response_time > 3000:
        checks["checks"]["response_time"] = "unhealthy - too slow"
    elif response_time > 1500:
        checks["checks"]["response_time"] = "degraded - slow"
    else:
        checks["checks"]["response_time"] = "healthy"
    
    # Overall status calculation
    unhealthy_count = sum(1 for check in checks["checks"].values() 
                         if isinstance(check, str) and "unhealthy" in check)
    degraded_count = sum(1 for check in checks["checks"].values() 
                        if isinstance(check, str) and "degraded" in check)
    
    if unhealthy_count > 0:
        checks["status"] = "unhealthy"
    elif degraded_count > 0:
        checks["status"] = "degraded"
    else:
        checks["status"] = "healthy"
    
    # Include job status for backwards compatibility
    checks["job_status"] = job_status
    
    return jsonify(checks)

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
            <h1>AUTOMATED SCHEDULER</h1>
            <p class="subtitle">Real-Time Pipeline Orchestration & Advanced Automation</p>
        </div>

        <div class="dashboard">
            <div class="card">
                <h2>System Status</h2>
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
                    Trigger Full Pipeline
                </button>
            </div>

            <div class="card">
                <h2>Individual Services</h2>
                <button class="service-btn" onclick="triggerService('load')">
                    Trigger Load Service
                </button>
                <button class="service-btn" onclick="triggerService('visualization')">
                    Trigger Visualization Service
                </button>
            </div>

            <div class="card">
                <h2>System Logs</h2>
                <div class="errors-container" id="errors">
                    <p style="text-align: center; opacity: 0.7; padding: 20px;">
                        No recent errors - System running smoothly
                    </p>
                </div>
            </div>
        </div>

        <script>
            async function loadStatus() {
                try {
                    let res = await fetch("/scheduler/health");
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
                                No recent errors - System running smoothly
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
                btn.innerHTML = 'Triggering...';
                btn.disabled = true;
                
                try {
                    const response = await fetch("/scheduler/trigger");
                    const data = await response.json();
                    
                    if (response.ok) {
                        btn.innerHTML = 'Pipeline Triggered!';
                        setTimeout(() => {
                            btn.innerHTML = originalText;
                            btn.disabled = false;
                        }, 3000);
                    } else {
                        btn.innerHTML = 'Failed to Trigger';
                        setTimeout(() => {
                            btn.innerHTML = originalText;
                            btn.disabled = false;
                        }, 2000);
                    }
                } catch (error) {
                    btn.innerHTML = 'Connection Error';
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
                
                btn.innerHTML = `Triggering ${serviceName}...`;
                btn.disabled = true;
                
                try {
                    const response = await fetch(`/scheduler/trigger/${serviceName}`);
                    const data = await response.json();
                    
                    if (response.ok) {
                        btn.innerHTML = `${serviceName} Triggered!`;
                        setTimeout(() => {
                            btn.innerHTML = originalText;
                            btn.disabled = false;
                        }, 2000);
                    } else {
                        btn.innerHTML = `${serviceName} Failed`;
                        setTimeout(() => {
                            btn.innerHTML = originalText;
                            btn.disabled = false;
                        }, 2000);
                    }
                } catch (error) {
                    btn.innerHTML = `${serviceName} Error`;
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