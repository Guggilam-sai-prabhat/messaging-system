"""
Diagnostic: test whether meta/llama-3.3-70b-instruct specifically is
overloaded/unhealthy right now, by hitting several different models on the
same NVIDIA NIM endpoint back-to-back. If smaller/different models succeed
while llama-3.3-70b fails, the problem is model-specific load, not the
network path or endpoint as a whole.
"""

import os
import subprocess
import time

NVIDIA_API_KEY = os.environ.get("NVIDIA_API_KEY", "")
NVIDIA_API_BASE_URL = os.environ.get("NVIDIA_API_BASE_URL", "https://integrate.api.nvidia.com/v1")
URL = f"{NVIDIA_API_BASE_URL}/chat/completions"

MODELS_TO_TEST = [
    "meta/llama-3.3-70b-instruct",   # currently configured — the 70B model
    "meta/llama-3.1-8b-instruct",    # smaller model, less compute-heavy
    "mistralai/mistral-7b-instruct-v0.3",  # different provider entirely
    "microsoft/phi-3-mini-4k-instruct",     # small, fast model
]

TIMEOUT_S = 12


def test_model(model: str) -> None:
    payload = (
        '{"model": "%s", "messages": [{"role": "user", "content": "hi"}], "max_tokens": 5}'
        % model
    )
    t0 = time.time()
    try:
        result = subprocess.run(
            [
                "curl", "-s", "--max-time", str(TIMEOUT_S),
                "-w", "\nHTTPSTATUS:%{http_code}",
                "-X", "POST", URL,
                "-H", f"Authorization: Bearer {NVIDIA_API_KEY}",
                "-H", "Content-Type: application/json",
                "-d", payload,
            ],
            capture_output=True, timeout=TIMEOUT_S + 2, text=True,
        )
        elapsed = time.time() - t0
        if "HTTPSTATUS:200" in result.stdout:
            print(f"  {model:40s} OK      elapsed={elapsed:.2f}s")
        else:
            status_line = result.stdout.strip().splitlines()[-1] if result.stdout.strip() else "no output"
            print(f"  {model:40s} FAILED  elapsed={elapsed:.2f}s  ({status_line})")
    except subprocess.TimeoutExpired:
        print(f"  {model:40s} TIMEOUT elapsed={time.time()-t0:.2f}s")


print(f"Testing {len(MODELS_TO_TEST)} models against {URL}\n")
for model in MODELS_TO_TEST:
    test_model(model)

print("\n=== NVIDIA build platform status check ===")
try:
    result = subprocess.run(
        ["curl", "-s", "-o", "/dev/null", "-w", "%{http_code}", "--max-time", "8",
         "https://build.nvidia.com"],
        capture_output=True, timeout=10, text=True,
    )
    print(f"  build.nvidia.com responded: HTTP {result.stdout}")
except subprocess.TimeoutExpired:
    print("  build.nvidia.com did not respond within 8s")
