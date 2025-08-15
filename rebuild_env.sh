#!/bin/bash

# Environment name (customize if needed)
ENV_NAME="who_covers"

echo "🔁 Rebuilding Conda environment: $ENV_NAME"

# Remove the old environment if it exists
echo "🧹 Removing old environment (if exists)..."
conda env remove --name $ENV_NAME --yes > /dev/null 2>&1 || true

# Create new environment from environment.yml
echo "📦 Creating new environment from environment.yml..."
conda env create --name $ENV_NAME --file environment.yml

echo "✅ Done. To activate it, run: conda activate $ENV_NAME"
