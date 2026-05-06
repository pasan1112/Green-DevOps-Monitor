from flask import Flask, jsonify

app = Flask(__name__)


@app.route("/")
def index():
    return jsonify({
        "message": "Green DevOps sample backend is running"
    })


@app.route("/health")
def health():
    return jsonify({
        "status": "ok"
    })


@app.route("/compute")
def compute():
    total = 0
    for i in range(5_000_000):
        total += i

    return jsonify({
        "result": total
    })


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5052)