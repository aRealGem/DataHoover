# Fix HF auth + fix the hub/transformers version mismatch you hit

## 1) Make huggingface_hub compatible with transformers 4.57.6 (AWQ/GPTQModel path)

```bash
pip -q install --upgrade --force-reinstall "huggingface_hub>=0.34.0,<1.0" "transformers==4.57.6"
```

## 2) Pull token from Colab Secrets (supports your current secret names as a fallback)

```python
import os

tok = os.environ.get("HF_TOKEN")
if not tok:
    from google.colab import userdata

    for name in ("HF_TOKEN", "HF_TOKE", "HuggingF"):
        tok = userdata.get(name)
        if tok:
            break

assert tok, "Add a Colab secret named HF_TOKEN (a 'read' token is enough)."
os.environ["HF_TOKEN"] = tok  # allow HF/Transformers to use it automatically
```

## 3) Persist login in this runtime + verify

```python
from huggingface_hub import login

login(token=tok, add_to_git_credential=False)
```

```bash
hf auth whoami
```

```python
import transformers, huggingface_hub

print("transformers:", transformers.__version__)
print("huggingface_hub:", huggingface_hub.__version__)
```
