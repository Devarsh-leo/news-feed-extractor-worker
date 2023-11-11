from flask import Flask, request, send_file, abort, render_template, redirect
from flask_cors import CORS
from threading import Thread
from modules.flows import main
import os
import logging
import requests
from datetime import datetime

REACT_PORT = os.environ.get("NEWS_EXTRACTOR_REACT_PORT")
NODE_PORT = os.environ.get("NEWS_EXTRACTOR_NODE_PORT")
SERVICE_THREAD = None
LAST_THREAD = None
formatted_datetime = datetime.now().strftime("%d_%m_%Y-%H_%M_%S")
os.makedirs('logs',exist_ok=True)
logging.basicConfig(
    level=logging.DEBUG,
    filename=f"logs/app-{formatted_datetime}.log", 
    filemode="w",
    format='%(asctime)s [%(levelname)s] %(funcName)s:%(lineno)d - %(message)s'
)

# Create a Flask app
app = Flask(__name__)
CORS(app, expose_headers=["Content-Disposition"])

# Initialize CORS extension with app and specify allowed origins
CORS(app, resources={r"/extract-news": {"origins": f"http://localhost:{REACT_PORT}"}})

shared = {"latest_file_path": None}
react_url = f"http://localhost:{REACT_PORT}".strip()
node_url = f"http://localhost:{NODE_PORT}".strip()

# print(REACT_PORT)

# Define a route and a view function
# @app.route("/")
# def hello_world():
#     return render_template("index.html")


@app.route("/")
def hello_world():
    return render_template("index.html")


@app.route("/react-app")
def react_app():
    return redirect(react_url)


@app.route("/react-ready")
def check_react_status():
    try:
        react_r = requests.get(react_url)
        logging.info(f"REACT {react_r.status_code}")
    except Exception as e:
        logging.error(f"Error: {e}")
        react_r = None

    try:
        node_r = requests.get(node_url)
        logging.info(f"NODE {node_r.status_code} ")
    except Exception as e:
        logging.error(f"Error: {e}")
        node_r = None

    if react_r and node_r and node_r.status_code == 200 and react_r.status_code == 200:
        logging.info(f"REACT is running at {react_url}!")
        return "OK"
    if react_r:
        return str(react_r.status_code)
    if node_r:
        return str(node_r.status_code)
    abort(404)


@app.route("/extract-news", methods=["POST"])
def extract_news_for():
    global SERVICE_THREAD, LAST_THREAD
    # Access the data sent with the POST request
    logging.info(request.json)
    query_params = request.json.get("site_data")
    # print(query_params)
    if query_params:
        if SERVICE_THREAD:
            LAST_THREAD[1].append(1)
        shared["latest_file_path"] = None
        kill_thread = []
        SERVICE_THREAD = Thread(target=main, args=(query_params, kill_thread, shared))
        SERVICE_THREAD.daemon = True
        SERVICE_THREAD.start()
        LAST_THREAD = SERVICE_THREAD, kill_thread
        # main(urls, session_id, from_date, to_date)
        # else:
        #     abort(404)
    return "OK"


@app.route("/get-file-path", methods=["GET"])
def get_latest_file_path():
    return shared["latest_file_path"] if shared["latest_file_path"] else ""


@app.route("/get-file")
def download_file():
    shared["latest_file_path"] if shared["latest_file_path"] else abort(404)
    return send_file(shared["latest_file_path"], as_attachment=True)


# Run the Flask app
if __name__ == "__main__":
    app.run(port=6789, debug=False)
