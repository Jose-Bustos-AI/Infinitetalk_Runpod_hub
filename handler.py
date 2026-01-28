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
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

server_address = os.getenv("SERVER_ADDRESS", "127.0.0.1")
client_id = str(uuid.uuid4())

OUTPUT_DIRS = [
    "/ComfyUI/output",
    "/ComfyUI/user/output",
    "/tmp",
]

def download_file_from_url(url: str, output_path: str) -> str:
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    result = subprocess.run(
        ["wget", "-L", "-O", output_path, "--no-verbose", "--timeout=30", "--tries=3", "--retry-connrefused", url],
        capture_output=True,
        text=True,
        timeout=120,
    )

    if result.returncode == 0 and os.path.exists(output_path) and os.path.getsize(output_path) > 0:
        logger.info(f"✅ Download OK: {url} -> {output_path} ({os.path.getsize(output_path)} bytes)")
        return output_path

    raise Exception(f"wget failed: {result.stderr}")


def load_workflow(workflow_path: str):
    with open(workflow_path, "r") as f:
        return json.load(f)


def queue_prompt(prompt):
    url = f"http://{server_address}:8188/prompt"
    payload = {"prompt": prompt, "client_id": client_id}
    data = json.dumps(payload).encode("utf-8")

    req = urllib.request.Request(url, data=data)
    req.add_header("Content-Type", "application/json")

    response = urllib.request.urlopen(req)
    return json.loads(response.read())


def get_history(prompt_id: str):
    url = f"http://{server_address}:8188/history/{prompt_id}"
    with urllib.request.urlopen(url) as response:
        return json.loads(response.read())


def view_download(filename: str, subfolder: str, folder_type: str) -> bytes:
    url = f"http://{server_address}:8188/view"
    data = {"filename": filename, "subfolder": subfolder or "", "type": folder_type or "output"}
    url_values = urllib.parse.urlencode(data)
    with urllib.request.urlopen(f"{url}?{url_values}") as response:
        return response.read()


def wait_for_comfyui():
    http_url = f"http://{server_address}:8188/"
    for i in range(600):  # hasta 10 min
        try:
            urllib.request.urlopen(http_url, timeout=5)
            logger.info(f"✅ ComfyUI HTTP ready (attempt {i+1})")
            return
        except Exception as e:
            if i % 10 == 0:
                logger.info(f"⏳ Waiting for ComfyUI... ({i+1}/600) {e}")
            time.sleep(1)
    raise Exception("ComfyUI not reachable via HTTP after timeout.")


def snapshot_mp4s(prefix: str | None = None) -> set[str]:
    found = set()
    for d in OUTPUT_DIRS:
        for p in glob.glob(f"{d}/**/*.mp4", recursive=True):
            if not os.path.exists(p) or os.path.getsize(p) == 0:
                continue
            if prefix is None or Path(p).name.startswith(prefix):
                found.add(p)
    return found


def newest_mp4(prefix: str | None = None) -> str | None:
    candidates = []
    for d in OUTPUT_DIRS:
        candidates.extend(glob.glob(f"{d}/**/*.mp4", recursive=True))
    candidates = [p for p in candidates if os.path.exists(p) and os.path.getsize(p) > 0]
    if prefix:
        candidates = [p for p in candidates if Path(p).name.startswith(prefix)]
    if not candidates:
        return None
    candidates.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return candidates[0]


def run_workflow_and_get_output_file(prompt, job_prefix: str) -> str:
    wait_for_comfyui()

    before = snapshot_mp4s(prefix=job_prefix)

    ws_url = f"ws://{server_address}:8188/ws?clientId={client_id}"
    ws = websocket.WebSocket()

    for attempt in range(60):
        try:
            ws.connect(ws_url)
            logger.info(f"✅ WebSocket connected (attempt {attempt+1})")
            break
        except Exception:
            time.sleep(5)
    else:
        raise Exception("WebSocket connect timeout.")

    prompt_id = queue_prompt(prompt)["prompt_id"]
    logger.info(f"▶️ Running workflow prompt_id={prompt_id}")

    execution_error = None

    while True:
        out = ws.recv()
        if not isinstance(out, str):
            continue

        msg = json.loads(out)
        mtype = msg.get("type")

        if mtype == "execution_error":
            execution_error = msg.get("data") or msg

        if mtype == "executing":
            data = msg.get("data", {})
            if data.get("node") is None and data.get("prompt_id") == prompt_id:
                break

    ws.close()

    if execution_error:
        raise Exception(f"Comfy execution_error: {json.dumps(execution_error)[:2000]}")

    history = get_history(prompt_id).get(prompt_id, {})
    outputs = history.get("outputs", {})

    # 1) Intenta leer del history (incluye ui)
    for _, node_output in outputs.items():
        # a) ui
        ui = node_output.get("ui")
        if isinstance(ui, dict):
            for key in ("videos", "gifs", "images"):
                items = ui.get(key)
                if isinstance(items, list):
                    for item in items:
                        if isinstance(item, dict):
                            fp = item.get("fullpath")
                            if fp and os.path.exists(fp) and os.path.getsize(fp) > 0:
                                return fp
                            fn = item.get("filename")
                            if fn:
                                data = view_download(fn, item.get("subfolder", ""), item.get("type", "output"))
                                tmp = f"/tmp/{uuid.uuid4().hex}_{fn}"
                                with open(tmp, "wb") as f:
                                    f.write(data)
                                if os.path.getsize(tmp) > 0:
                                    if not tmp.lower().endswith(".mp4"):
                                        tmp2 = tmp + ".mp4"
                                        os.rename(tmp, tmp2)
                                        tmp = tmp2
                                    return tmp

        # b) keys normales
        for key in ("videos", "gifs", "images"):
            items = node_output.get(key)
            if isinstance(items, list):
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    fp = item.get("fullpath")
                    if fp and os.path.exists(fp) and os.path.getsize(fp) > 0:
                        return fp
                    fn = item.get("filename")
                    if fn:
                        data = view_download(fn, item.get("subfolder", ""), item.get("type", "output"))
                        tmp = f"/tmp/{uuid.uuid4().hex}_{fn}"
                        with open(tmp, "wb") as f:
                            f.write(data)
                        if os.path.getsize(tmp) > 0:
                            if not tmp.lower().endswith(".mp4"):
                                tmp2 = tmp + ".mp4"
                                os.rename(tmp, tmp2)
                                tmp = tmp2
                            return tmp

    # 2) Snapshot diff
    time.sleep(2)
    after = snapshot_mp4s(prefix=job_prefix)
    new_files = list(after - before)
    if new_files:
        new_files.sort(key=lambda p: os.path.getmtime(p), reverse=True)
        return new_files[0]

    # 3) Newest
    latest = newest_mp4(prefix=job_prefix)
    if latest:
        return latest

    # Debug: listar mp4s
    debug = {}
    for d in OUTPUT_DIRS:
        try:
            debug[d] = [str(p) for p in Path(d).rglob("*.mp4")][:30]
        except Exception:
            debug[d] = "unreadable"

    raise Exception(f"Could not find mp4 anywhere. Debug dirs: {json.dumps(debug)[:2000]}")


def supabase_upload_file(local_path: str, dest_path: str, content_type: str = "video/mp4") -> str:
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

    return f"{supabase_url}/storage/v1/object/public/{bucket}/{dest_path}"


def handler(job):
    job_input = job.get("input", {})

    image_url = job_input.get("image_url")
    wav_url = job_input.get("wav_url")
    if not image_url or not wav_url:
        return {"error": "URL-only: provide image_url and wav_url."}

    for env_key in ["SUPABASE_URL", "SUPABASE_SERVICE_ROLE_KEY"]:
        if not os.environ.get(env_key):
            return {"error": f"Missing env var: {env_key}"}

    prompt_text = job_input.get("prompt", "A person talking naturally")
    width = int(job_input.get("width", 640))
    height = int(job_input.get("height", 640))
    max_frame = int(job_input.get("max_frame", 350))
    force_offload = bool(job_input.get("force_offload", True))

    task_id = f"infinitetalk_{uuid.uuid4().hex}"
    temp_dir = f"/tmp/{task_id}"
    os.makedirs(temp_dir, exist_ok=True)

    local_image = download_file_from_url(image_url, os.path.join(temp_dir, "input_image.jpg"))
    local_audio = download_file_from_url(wav_url, os.path.join(temp_dir, "input_audio.wav"))

    workflow_path = "/I2V_single.json"
    if not os.path.exists(workflow_path):
        return {"error": f"Workflow not found at {workflow_path}."}

    prompt = load_workflow(workflow_path)

    # VHS: guardado + prefix único
    if "131" in prompt and "inputs" in prompt["131"]:
        prompt["131"]["inputs"]["save_output"] = True
        prompt["131"]["inputs"]["filename_prefix"] = task_id

    # WanVideoModelLoader: asegurar modo estable (por si alguien vuelve a poner sageattn)
    if "122" in prompt and "inputs" in prompt["122"]:
        prompt["122"]["inputs"]["attention_mode"] = "sdpa"

    # Sampler: offload
    if "128" in prompt and prompt["128"].get("class_type") == "WanVideoSampler":
        prompt["128"].setdefault("inputs", {})["force_offload"] = force_offload

    # Inject inputs
    required_nodes = ["284", "125", "241", "245", "246", "270"]
    for nid in required_nodes:
        if nid not in prompt:
            return {"error": f"Workflow missing node id {nid}."}

    prompt["284"]["inputs"]["image"] = local_image
    prompt["125"]["inputs"]["audio"] = local_audio
    prompt["241"]["inputs"]["positive_prompt"] = prompt_text
    prompt["245"]["inputs"]["value"] = width
    prompt["246"]["inputs"]["value"] = height
    prompt["270"]["inputs"]["value"] = max_frame

    try:
        output_video_path = run_workflow_and_get_output_file(prompt, job_prefix=task_id)
    except Exception as e:
        logger.error(f"Workflow run failed: {e}")
        return {"error": f"Workflow run failed: {e}"}

    prefix = os.environ.get("SUPABASE_PATH_PREFIX", "infinitetalk").strip("/")
    dest_path = f"{prefix}/{task_id}.mp4" if prefix else f"{task_id}.mp4"

    try:
        video_url = supabase_upload_file(output_video_path, dest_path, "video/mp4")
    except Exception as e:
        logger.error(f"Supabase upload failed: {e}")
        return {"error": f"Supabase upload failed: {e}"}

    return {"video_url": video_url}


runpod.serverless.start({"handler": handler})
