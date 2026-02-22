#!/bin/bash
# =====================================================
# Setup script – push Atlas Annotation Tool to GitHub
# =====================================================
# Run this from the project folder:
#   bash setup_git.sh

set -e

echo "🔧 Initializing Git repository..."
git init
git add .
git commit -m "🎉 Initial commit – Atlas Capture Annotation Tool

Full-featured egocentric video annotation app:
- Video player with drag-and-drop support
- Auto-segment creation from video duration
- Real-time label validation engine (enforces all Atlas Capture rules)
- Segment editing: adjust timestamps, split, merge, delete, undo
- Keyboard shortcuts (E, S, D, M, U, L, P, Space, J/K)
- Built-in 6-tab guidelines reference
- Common mistakes checker (12 mistake types)
- Annotation history with JSON + CSV export
- No dependencies – pure HTML/CSS/JS"

echo ""
echo "📡 Adding remote origin..."
git remote add origin https://github.com/aymank2020/OCR_Annotation_Atlas.git

echo ""
echo "🚀 Pushing to GitHub..."
git branch -M main
git push -u origin main

echo ""
echo "✅ Done! Visit: https://github.com/aymank2020/OCR_Annotation_Atlas"
