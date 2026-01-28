import runpod
import os
import websocket
import json
import uuid
import logging
import urllib.request
import urllib.parse
import subprocess
import time
import requests
import glob

# -------------------------
# Logging
# -------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

server_address = os.getenv("SERVER_ADDRESS", "127.0.0.1")
client_id = str(uuid.uuid4())


# -------------------------
# Helpers
# -------------------------
def download_file_from_url(url: str, output_path: str) -> str:
    """Download file with wget (works well inside serverless containers)."""
    try:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        # -L follow redirects, retries for transient issues
        result = subprocess.run(
            ["wget", "-L", "-O", output_path, "--no-verbose", "--timeout=30", "--tries=3", "--retry-connrefused", url],
            capture_output=True,
            text=True,
            timeout=120,
        )

        if (
            result.returncode == 0
            and os.path.exists(output_path)
            and os.path.getsize(output_path) > 0
        ):
            logger.info(
                f"✅ Download OK: {url} -> {output_path} ({os.path.getsize(output_path)} bytes)"
            )
            return output_path

        raise Exception(f"wget failed: {result.stderr}")

    except subprocess.TimeoutExpired:
        raise Exception("Download timeout")
    except Exception as e:
        raise Exception(f"Download error: {e}")


def load_workflow(workflow_path: str):
    with open(workflow_path, "r") as f:
        return json.load(f)


def queue_prompt(prompt):
    url = f"http://{server_address}:8188/prompt"
    payload = {"prompt": prompt, "client_id": client_id}
    data = json.dumps(payload).encode("utf-8")

    req = urllib.request.Request(url, data=data)
    req.add_header("Content-Type", "application/json")

    try:
        response = urllib.request.urlopen(req)
        result = json.loads(response.read())
        return result
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="ignore")
        logger.error(f"HTTP error {e.code}: {e.reason} | body: {body}")
        raise
    except Exception as e:
        logger.error(f"Prompt send error: {e}")
        raise


def get_history(prompt_id: str):
    url = f"http://{server_address}:8188/history/{prompt_id}"
    with urllib.request.urlopen(url) as response:
        return json.loads(response.read())


def view_download(filename: str, subfolder: str, folder_type: str) -> bytes:
    """Download a file from ComfyUI /view endpoint."""
    url = f"http://{server_address}:8188/view"
    data = {
        "filename": filename,
        "subfolder": subfolder or "",
        "type": folder_type or "output",
    }
    url_values = urllib.parse.urlencode(data)
    with urllib.request.urlopen(f"{url}?{url_values}") as response:
        return response.read()


def wait_for_comfyui():
    http_url = f"http://{server_address}:8188/"
    max_http_attempts = 240  # ~4min
    for i in range(max_http_attempts):
        try:
            urllib.request.urlopen(http_url, timeout=5)
            logger.info(f"✅ ComfyUI HTTP ready (attempt {i+1})")
            return
        except Exception as e:
            logger.info(f"⏳ Waiting for ComfyUI... ({i+1}/{max_http_attempts}) {e}")
            time.sleep(1)
    raise Exception("ComfyUI not reachable via HTTP after timeout.")


def run_workflow_and_get_output_file(prompt) -> str:
    """
    Run workflow and return a local mp4 path.
    Robust strategy:
      1) Inspect history outputs (videos/gifs/images) and take fullpath if present
      2) If only filename/subfolder/type exists, download via /view
      3) Fallback to filesystem scan /ComfyUI/output/**/*.mp4 (newest)
    """
    wait_for_comfyui()

    ws_url = f"ws://{server_address}:8188/ws?clientId={client_id}"
    ws = websocket.WebSocket()

    # WebSocket connect retry (~3min)
    for attempt in range(36):
        try:
            ws.connect(ws_url)
            logger.info(f"✅ WebSocket connected (attempt {attempt+1})")
            break
        except Exception as e:
            logger.info(f"⏳ WebSocket retry {attempt+1}/36: {e}")
            time.sleep(5)
    else:
        raise Exception("WebSocket connect timeout.")

    prompt_id = queue_prompt(prompt)["prompt_id"]
    logger.info(f"▶️ Running workflow prompt_id={prompt_id}")

    # Wait completion
    while True:
        out = ws.recv()
        if isinstance(out, str):
            msg = json.loads(out)
            if msg.get("type") == "executing":
                data = msg.get("data", {})
                if data.get("node") is None and data.get("prompt_id") == prompt_id:
                    logger.info("✅ Workflow finished")
                    break

    ws.close()

    history_all = get_history(prompt_id)
    history = history_all.get(prompt_id)
    if not history:
        raise Exception("No history returned for prompt_id.")

    outputs = history.get("outputs", {})
    logger.info(f"🧾 history.outputs node count = {len(outputs)}")

    # DEBUG: log keys per node (so we see real structure)
    for nid, nout in outputs.items():
        try:
            logger.info(f"🔎 node {nid} keys: {list(nout.keys())}")
        except Exception:
            pass

    # 1) Try history outputs
    for node_id, node_output in outputs.items():
        for key in ("videos", "gifs", "images"):
            items = node_output.get(key)
            if not items or not isinstance(items, list):
                continue

            for item in items:
                if not isinstance(item, dict):
                    continue

                fullpath = item.get("fullpath")
                if fullpath and os.path.exists(fullpath) and os.path.getsize(fullpath) > 0:
                    logger.info(f"✅ Output via fullpath (node {node_id}): {fullpath}")
                    return fullpath

                filename = item.get("filename")
                subfolder = item.get("subfolder", "")
                folder_type = item.get("type", "output")
                if filename:
                    logger.info(
                        f"✅ Output via /view (node {node_id}): {filename} subfolder={subfolder} type={folder_type}"
                    )
                    data = view_download(filename, subfolder, folder_type)

                    tmp_dir = f"/tmp/{uuid.uuid4()}"
                    os.makedirs(tmp_dir, exist_ok=True)
                    local_path = os.path.join(tmp_dir, filename)

                    with open(local_path, "wb") as f:
                        f.write(data)

                    if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
                        # Ensure .mp4 extension for uploads
                        if not local_path.lower().endswith(".mp4"):
                            mp4_path = local_path + ".mp4"
                            os.rename(local_path, mp4_path)
                            local_path = mp4_path
                        return local_path

    # 2) Fallback: scan filesystem for newest MP4
    logger.warning("⚠️ No output found in history outputs. Falling back to filesystem scan /ComfyUI/output.")
    candidates = sorted(
        glob.glob("/ComfyUI/output/**/*.mp4", recursive=True),
        key=lambda p: os.path.getmtime(p),
        reverse=True,
    )
    if candidates:
        logger.info(f"✅ Output via filesystem: {candidates[0]}")
        return candidates[0]

    raise Exception("Could not find video output in history AND no mp4 found in /ComfyUI/output.")


def supabase_upload_file(local_path: str, dest_path: str, content_type: str = "video/mp4") -> str:
    """
    Upload MP4 to Supabase Storage and return PUBLIC URL.
    Requires bucket public OR you'll need signed URLs (not implemented here).
    """
    supabase_url = os.environ["SUPABASE_URL"].rstrip("/")
    key = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
    bucket = os.environ.get("SUPABASE_BUCKET", "results")

    upload_url = f"{supabase_url}/storage/v1/object/{bucket}/{dest_path}"

    with open(local_path, "rb") as f:
        r = requests.post(
            upload_url,
            headers={
                "Authorization": f"Bearer {key}",
                "apikey": key,
                "Content-Type": content_type,
                "x-upsert": "true",
            },
            data=f,
            timeout=300,
        )

    if not r.ok:
        raise Exception(f"Supabase upload failed: {r.status_code} {r.text}")

    public_url = f"{supabase_url}/storage/v1/object/public/{bucket}/{dest_path}"
    return public_url


# -------------------------
# Handler (URL-only, I2V only)
# -------------------------
def handler(job):
    job_input = job.get("input", {})

    # URL-only strict validation
    image_url = job_input.get("image_url")
    wav_url = job_input.get("wav_url")
    if not image_url or not wav_url:
        return {"error": "URL-only mode: you must provide image_url and wav_url."}

    # Optional params
    prompt_text = job_input.get("prompt", "A person talking naturally")
    width = int(job_input.get("width", 640))
    height = int(job_input.get("height", 640))
    max_frame = int(job_input.get("max_frame", 350))
    force_offload = bool(job_input.get("force_offload", True))

    # Supabase config validation
    for env_key in ["SUPABASE_URL", "SUPABASE_SERVICE_ROLE_KEY"]:
        if not os.environ.get(env_key):
            return {"error": f"Missing env var: {env_key}"}

    task_id = f"infinitetalk_{uuid.uuid4()}"
    temp_dir = f"/tmp/{task_id}"
    os.makedirs(temp_dir, exist_ok=True)

    # Download inputs to local files
    local_image = download_file_from_url(image_url, os.path.join(temp_dir, "input_image.jpg"))
    local_audio = download_file_from_url(wav_url, os.path.join(temp_dir, "input_audio.wav"))

    # Load workflow (I2V single only)
    workflow_path = "/I2V_single.json"
    if not os.path.exists(workflow_path):
        return {"error": f"Workflow not found at {workflow_path}. Ensure it is copied into container root (/)."}

    prompt = load_workflow(workflow_path)

    # Safety: ensure output saving is enabled for VHS node if present
    if "131" in prompt and "inputs" in prompt["131"]:
        prompt["131"]["inputs"]["save_output"] = True

    # Inject force_offload into WanVideoSampler (id 128 in your workflow)
    if "128" in prompt and prompt["128"].get("class_type") == "WanVideoSampler":
        prompt["128"].setdefault("inputs", {})["force_offload"] = force_offload

    # Inject paths & params using known node IDs
    required_nodes = ["284", "125", "241", "245", "246", "270"]
    for nid in required_nodes:
        if nid not in prompt:
            return {"error": f"Workflow missing node id {nid}. Your exported workflow must match handler node ids."}

    prompt["284"]["inputs"]["image"] = local_image
    prompt["125"]["inputs"]["audio"] = local_audio
    prompt["241"]["inputs"]["positive_prompt"] = prompt_text
    prompt["245"]["inputs"]["value"] = width
    prompt["246"]["inputs"]["value"] = height
    prompt["270"]["inputs"]["value"] = max_frame

    # Run and get local MP4 file
    try:
        output_video_path = run_workflow_and_get_output_file(prompt)
    except Exception as e:
        logger.error(f"Workflow run failed: {e}")
        return {"error": f"Workflow run failed: {e}"}

    if not os.path.exists(output_video_path) or os.path.getsize(output_video_path) == 0:
        return {"error": "Output video file missing or empty."}

    # Upload to Supabase
    prefix = os.environ.get("SUPABASE_PATH_PREFIX", "infinitetalk").strip("/")
    filename = f"{task_id}.mp4"
    dest_path = f"{prefix}/{filename}" if prefix else filename

    try:
        video_url = supabase_upload_file(output_video_path, dest_path, "video/mp4")
    except Exception as e:
        logger.error(f"Supabase upload failed: {e}")
        return {"error": f"Supabase upload failed: {e}"}

    return {"video_url": video_url}


runpod.serverless.start({"handler": handler})
