# ============================================================================
# Dockerfile: Container Definition for Portfolio Analytics Pipeline
# ============================================================================
#
# WHAT IS A DOCKERFILE?
#   A Dockerfile is a recipe for building a Docker image (a packaged environment).
#   Each line is an instruction that creates a "layer" in the image.
#
# DOCKER CONCEPTS:
#   - Image: A snapshot of an environment (like a VM template)
#   - Container: A running instance of an image (like a running VM)
#   - Layer: Each instruction creates a new layer (cached for speed)
#   - Registry: Where images are stored (Docker Hub, AWS ECR)
#
# WHY USE DOCKER?
#   ✓ "Works on my machine" → Works everywhere
#   ✓ Consistent environments (dev, staging, production identical)
#   ✓ Easy deployment (just run the container)
#   ✓ Industry standard (every tech company uses it)
#
# HOW TO USE THIS FILE:
#   Build:  docker build -t portfolio-analytics .
#   Run:    docker run portfolio-analytics
#   Push:   docker push your-registry/portfolio-analytics
#
# ============================================================================

# ----------------------------------------------------------------------------
# STAGE 1: Base Image
# ----------------------------------------------------------------------------
# FROM: Specify the base image to start from
# Think of this like installing an operating system on a fresh computer
#
# Format: image-name:tag
# - python: Official Python image from Docker Hub
# - 3.11: Python version
# - slim: Minimal variant (smaller size, faster downloads)
#
# Other options:
# - python:3.11 (full version, includes build tools, ~900MB)
# - python:3.11-slim (~150MB) ← We use this
# - python:3.11-alpine (even smaller ~50MB, but more compatibility issues)
FROM python:3.11-slim

# ----------------------------------------------------------------------------
# Set Environment Variables
# ----------------------------------------------------------------------------
# ENV: Set environment variables that persist in the container
#
# These are like adding to your .bashrc file

# Prevent Python from writing .pyc files (compiled bytecode)
# Why? Saves space, not needed in containers (code doesn't change)
ENV PYTHONDONTWRITEBYTECODE=1

# Force Python output to appear immediately (no buffering)
# Why? See logs in real-time instead of waiting for buffer to flush
ENV PYTHONUNBUFFERED=1

# Set working directory for the app
# Why? So we don't clutter the root filesystem
ENV APP_HOME=/app

# ----------------------------------------------------------------------------
# Install System Dependencies
# ----------------------------------------------------------------------------
# RUN: Execute a command during the build process
#
# This layer installs OS-level packages (like apt-get install)
# We need these for Python packages to compile

RUN apt-get update && apt-get install -y \
    # gcc: C compiler (some Python packages need to compile C extensions)
    gcc \
    # g++: C++ compiler
    g++ \
    # build-essential: Basic build tools
    build-essential \
    # Clean up apt cache to reduce image size
    # Important: Always clean up in the same RUN command
    # Why? Each RUN creates a layer - cleaning in next RUN doesn't save space!
    && rm -rf /var/lib/apt/lists/*

# ----------------------------------------------------------------------------
# Create Application Directory
# ----------------------------------------------------------------------------
# WORKDIR: Set the working directory (like 'cd /app')
# Creates the directory if it doesn't exist
# All subsequent commands run from this directory
WORKDIR $APP_HOME

# ----------------------------------------------------------------------------
# Copy and Install Python Dependencies
# ----------------------------------------------------------------------------
# COPY: Copy files from your computer into the container
# Format: COPY <source> <destination>
#
# WHY COPY requirements.txt FIRST?
# Docker caches layers. If requirements.txt doesn't change, Docker reuses
# the cached layer with installed packages (saves ~1 minute per build!)
#
# Build process:
# 1. First build: Install all packages (~60 seconds)
# 2. Change your code (not requirements.txt)
# 3. Rebuild: Reuses cached packages layer! (~5 seconds)

# Copy just requirements.txt (not whole project yet)
COPY requirements.txt .

# Install Python dependencies
# --no-cache-dir: Don't store pip cache (saves space)
# --upgrade: Upgrade pip to latest version first
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# ----------------------------------------------------------------------------
# Copy Application Code
# ----------------------------------------------------------------------------
# Now copy the rest of the application
# This comes AFTER pip install so code changes don't invalidate package cache
#
# COPY . . means:
# - First dot: Everything in current directory (on your computer)
# - Second dot: Current directory in container (/app)

COPY . .
ENV PYTHONPATH=/app/src:$PYTHONPATH
# ----------------------------------------------------------------------------
# Create Non-Root User (Security Best Practice)
# ----------------------------------------------------------------------------
# By default, containers run as root (dangerous!)
# Best practice: Create a regular user to run the application
#
# Why?
# - Limits damage if container is compromised
# - Follows principle of least privilege
# - Required by some container platforms (Kubernetes)

# Create a user named 'appuser' with no password (-r = system user)
RUN useradd -r -u 1000 appuser

# Create directory for temporary files and give ownership to appuser
RUN mkdir -p /tmp && chown -R appuser:appuser /tmp

# Switch to non-root user for all subsequent commands
USER appuser

# ----------------------------------------------------------------------------
# Expose Port (Documentation Only)
# ----------------------------------------------------------------------------
# EXPOSE: Document which port the app uses
# This does NOT actually publish the port (just documentation)
# You still need -p flag when running: docker run -p 8050:8050
#
# Port 8050 is Dash's default port
EXPOSE 8050

# ----------------------------------------------------------------------------
# Health Check (Optional but Recommended)
# ----------------------------------------------------------------------------
# HEALTHCHECK: Define how Docker checks if container is healthy
# Useful for:
# - Auto-restart unhealthy containers
# - Load balancer knows which containers are ready
# - Monitoring/alerting
#
# This checks every 30 seconds if the script can run successfully

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import pymongo, redis; print('healthy')" || exit 1

# ----------------------------------------------------------------------------
# Default Command
# ----------------------------------------------------------------------------
# CMD: Default command to run when container starts
# Can be overridden: docker run image-name python other_script.py
#
# Format options:
# 1. CMD ["executable", "arg1", "arg2"] ← Preferred (exec form)
# 2. CMD command arg1 arg2 (shell form)
#
# Exec form is better because:
# - Process runs as PID 1 (receives signals properly)
# - No shell wrapper (slightly faster, cleaner logs)

# Default: Run the batch processor
CMD ["python", "-u", "src/batch_processor.py"]

# Alternative: Run the dashboard instead
# CMD ["gunicorn", "-b", "0.0.0.0:8050", "dashboard:server", "--workers=4"]

# ============================================================================
# MULTI-STAGE BUILD (Advanced - Optional)
# ============================================================================
#
# For production, you might want a multi-stage build:
# - Stage 1: Install build dependencies, compile packages
# - Stage 2: Copy only runtime files (smaller final image)
#
# Example:
#
# # Stage 1: Builder
# FROM python:3.11-slim AS builder
# WORKDIR /app
# COPY requirements.txt .
# RUN pip install --user --no-cache-dir -r requirements.txt
#
# # Stage 2: Runtime
# FROM python:3.11-slim
# WORKDIR /app
# COPY --from=builder /root/.local /root/.local
# COPY . .
# CMD ["python", "batch_processor.py"]
#
# Result: 30-40% smaller image!
#
# ============================================================================

# ============================================================================
# BUILD ARGUMENTS (Advanced - Optional)
# ============================================================================
#
# ARG: Define build-time variables (different from ENV)
# Can be overridden at build time:
#   docker build --build-arg PYTHON_VERSION=3.12 .
#
# Example:
# ARG PYTHON_VERSION=3.11
# FROM python:${PYTHON_VERSION}-slim
#
# ============================================================================

# ============================================================================
# .dockerignore FILE
# ============================================================================
#
# Create a .dockerignore file (like .gitignore) to exclude files from COPY
# This speeds up builds and reduces image size
#
# Example .dockerignore:
# __pycache__/
# *.pyc
# *.pyo
# .git/
# .env
# .venv/
# *.log
# .DS_Store
# .pytest_cache/
# .coverage
#
# ============================================================================