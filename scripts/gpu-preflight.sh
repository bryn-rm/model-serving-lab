#!/usr/bin/env bash
# GPU host preflight: host/toolkit compatibility, image digest, in-container
# CUDA check, vLLM flag support and model revision. Evidence goes to OUT_DIR.
#
#   scripts/gpu-preflight.sh OUT_DIR           # check and report only
#   scripts/gpu-preflight.sh OUT_DIR --write   # also pin configs/gpu-pilot.env
#
# Fails (nonzero) on anything that would invalidate a run. Uses no credentials
# except an optional HF_TOKEN for the Hugging Face revision lookup.
set -euo pipefail

out=${1:?usage: scripts/gpu-preflight.sh OUT_DIR [--write]}
write=${2:-}
env_file=${GPU_ENV:-configs/gpu-pilot.env}
mkdir -p "$out"
# shellcheck disable=SC1090
set -a; . "$env_file"; set +a
fail() { echo "PREFLIGHT FAIL: $*" >&2; exit 1; }
note() { echo "preflight: $*"; }

for tool in docker nvidia-smi curl jq python3 git; do
  command -v "$tool" >/dev/null || fail "missing $tool"
done
docker compose version >"$out/docker-compose-version.txt" || fail "Docker Compose v2 unavailable"
docker version --format '{{.Server.Version}}' >"$out/docker-version.txt"

# Host GPU and driver. The header's "CUDA Version" is the newest CUDA runtime
# this driver supports; the image's CUDA must not exceed it.
nvidia-smi >"$out/nvidia-smi.txt"
nvidia-smi --query-gpu=name,driver_version,memory.total,compute_cap --format=csv,noheader \
  >"$out/gpu.csv" 2>/dev/null ||
  nvidia-smi --query-gpu=name,driver_version,memory.total --format=csv,noheader >"$out/gpu.csv"
[ "$(wc -l <"$out/gpu.csv")" -ge 1 ] || fail "no GPU visible to nvidia-smi"
[ "$(wc -l <"$out/gpu.csv")" -eq 1 ] || note "more than one GPU; the stack reserves one (count: 1)"
driver_cuda=$(sed -n 's/.*CUDA Version: *\([0-9.]*\).*/\1/p' "$out/nvidia-smi.txt" | head -1)
note "GPU: $(head -1 "$out/gpu.csv"); driver supports CUDA ${driver_cuda:-unknown}"
docker info --format '{{json .Runtimes}}' >"$out/docker-runtimes.json"
df -h "$(docker info --format '{{.DockerRootDir}}')" "${HF_CACHE_DIR:-$HOME/.cache/huggingface}" \
  >"$out/disk.txt" 2>/dev/null || df -h >"$out/disk.txt"
note "disk (need roughly 25 GB image + 16 GB weights):"; cat "$out/disk.txt"

# Image: pull by tag, resolve the registry digest, refuse silent drift.
tag_ref="$VLLM_IMAGE_REPO:$VLLM_IMAGE_TAG"
docker pull "$tag_ref" >"$out/docker-pull.txt"
digest=$(docker image inspect --format '{{range .RepoDigests}}{{println .}}{{end}}' "$tag_ref" |
  sed -n "s|^$VLLM_IMAGE_REPO@||p" | head -1)
[ -n "$digest" ] || fail "could not resolve a registry digest for $tag_ref"
if [ -n "${VLLM_IMAGE_DIGEST:-}" ] && [ "$VLLM_IMAGE_DIGEST" != "$digest" ]; then
  fail "tag $tag_ref now resolves to $digest, but $env_file pins $VLLM_IMAGE_DIGEST (tag moved; pull the pinned digest or record a new pin deliberately)"
fi
image_ref="$VLLM_IMAGE_REPO@$digest"
note "image $tag_ref = $image_ref"

# In-container CUDA: the definitive compatibility check is that the pinned
# image's torch sees the GPU and can run a BF16 matmul with this driver.
docker run --rm --gpus all --entrypoint python3 "$image_ref" -c '
import json, torch, vllm
ok = torch.cuda.is_available()
r = {"vllm_version": vllm.__version__, "torch_version": torch.__version__,
     "torch_cuda": torch.version.cuda, "cuda_available": ok}
if ok:
    r["device"] = torch.cuda.get_device_name(0)
    r["capability"] = ".".join(map(str, torch.cuda.get_device_capability(0)))
    r["bf16_supported"] = torch.cuda.is_bf16_supported()
    free, total = torch.cuda.mem_get_info()
    r["memory_total_gib"] = round(total / 2**30, 2)
    x = torch.randn(256, 256, device="cuda", dtype=torch.bfloat16)
    r["bf16_matmul_ok"] = bool(torch.isfinite(x @ x).all())
print(json.dumps(r))
' >"$out/container-cuda.json" 2>"$out/container-cuda.stderr" ||
  fail "image could not start with --gpus all; see $out/container-cuda.stderr (NVIDIA Container Toolkit or driver/CUDA mismatch)"
cat "$out/container-cuda.json"
jq -e '.cuda_available and .bf16_matmul_ok' "$out/container-cuda.json" >/dev/null ||
  fail "torch in the image cannot use the GPU (driver CUDA ${driver_cuda:-unknown} vs image CUDA $(jq -r .torch_cuda "$out/container-cuda.json")). Choose a compatible stable image and record the substitution"
jq -e '.memory_total_gib >= 22' "$out/container-cuda.json" >/dev/null ||
  note "WARNING: under 22 GiB GPU memory; 7B BF16 weights alone are ~15 GB"

# Flags: every argument in compose.gpu.yaml must exist in the pinned release.
docker run --rm --gpus all --entrypoint vllm "$image_ref" serve --help=all >"$out/vllm-serve-help.txt" 2>&1 ||
  docker run --rm --gpus all --entrypoint vllm "$image_ref" serve --help >"$out/vllm-serve-help.txt" 2>&1 ||
  fail "vllm serve --help failed; see $out/vllm-serve-help.txt"
has_flag() { grep -qE -- "(^|[[:space:],])$1([[:space:]=,]|$)" "$out/vllm-serve-help.txt"; }
missing=()
for flag in --revision --tokenizer-revision --served-model-name --dtype --kv-cache-dtype \
  --max-model-len --gpu-memory-utilization --max-num-seqs --max-num-batched-tokens \
  "$CHUNKED_PREFILL_FLAG" --generation-config --host --port; do
  has_flag "$flag" || missing+=("$flag")
done
prefix_flag=
for candidate in --no-enable-prefix-caching --disable-prefix-caching; do
  if has_flag "$candidate"; then prefix_flag=$candidate; break; fi
done
[ -n "$prefix_flag" ] || missing+=("prefix-caching disable flag")
[ ${#missing[@]} -eq 0 ] || fail "flags unsupported by $tag_ref: ${missing[*]}"
note "all serve flags supported; prefix caching disabled with $prefix_flag"

# Model revision: resolve main once, then keep the pinned commit.
# The optional token is passed as a header file, never on the command line.
hf_get() {
  if [ -n "${HF_TOKEN:-}" ]; then
    curl -fsS -H @<(printf 'Authorization: Bearer %s\n' "$HF_TOKEN") "$1"
  else
    curl -fsS "$1"
  fi
}
revision=${MODEL_REVISION:-}
if [ -z "$revision" ]; then
  revision=$(hf_get "https://huggingface.co/api/models/$MODEL_ID/revision/main" | jq -r .sha)
fi
[[ "$revision" =~ ^[0-9a-f]{40}$ ]] || fail "could not resolve a 40-hex revision for $MODEL_ID"
hf_get "https://huggingface.co/api/models/$MODEL_ID/revision/$revision" |
  jq '{id, sha, files: [.siblings[].rfilename | select(test("tokenizer|config|generation_config|\\.json$"))]}' \
    >"$out/model-revision.json" || fail "revision $revision not found for $MODEL_ID"
jq -e '.files | index("tokenizer_config.json") and index("config.json")' "$out/model-revision.json" >/dev/null ||
  fail "revision $revision lacks tokenizer_config.json/config.json"
note "model $MODEL_ID revision $revision (tokenizer and chat template from the same revision)"

jq -n --arg tag "$tag_ref" --arg digest "$digest" --arg rev "$revision" --arg prefix "$prefix_flag" \
  --arg driver_cuda "${driver_cuda:-}" --slurpfile cuda "$out/container-cuda.json" \
  '{image_tag: $tag, image_digest: $digest, model_revision: $rev, tokenizer_revision: $rev,
    prefix_caching_flag: $prefix, driver_cuda: $driver_cuda, container: $cuda[0]}' >"$out/preflight.json"

if [ "$write" = "--write" ]; then
  sed -i -e "s|^VLLM_IMAGE_DIGEST=.*|VLLM_IMAGE_DIGEST=$digest|" \
    -e "s|^MODEL_REVISION=.*|MODEL_REVISION=$revision|" \
    -e "s|^PREFIX_CACHING_FLAG=.*|PREFIX_CACHING_FLAG=$prefix_flag|" "$env_file"
  note "pinned $env_file; review with git diff and commit it before measured runs"
fi
note "PASS (evidence in $out)"
