FROM python:3.11-slim

# Minimal OS deps for dev convenience
RUN apt-get update && apt-get install -y --no-install-recommends \
    git curl ca-certificates \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /workspaces/data-hoover-starter

# Install project deps
COPY pyproject.toml /tmp/pyproject.toml
RUN pip install -U pip \
 && pip install -e /tmp/ 2>/dev/null || true

# In Dev Containers, the workspace will be mounted over this path.
# We'll (re)install editable deps once mounted.
CMD [ "bash", "-lc", "pip install -e . && bash" ]
