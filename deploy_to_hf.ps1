# Quick Deploy Script for Hugging Face Spaces
# Run this after creating your HF Space

# 1. Initialize git (if not already done)
Write-Host "Initializing git repository..." -ForegroundColor Green
if (-not (Test-Path .git)) {
    git init
    Write-Host "Git repository initialized!" -ForegroundColor Green
} else {
    Write-Host "Git repository already exists." -ForegroundColor Yellow
}

# 2. Add all files
Write-Host "`nAdding files to git..." -ForegroundColor Green
git add .

# 3. Commit
Write-Host "`nCommitting files..." -ForegroundColor Green
git commit -m "Initial commit - Project Samarth production-ready"

# 4. Prompt for Hugging Face username and space name
Write-Host "`n" -ForegroundColor Cyan
$username = Read-Host "Enter your Hugging Face username"
$spacename = Read-Host "Enter your Space name (e.g., project-samarth)"

# 5. Add Hugging Face remote
Write-Host "`nAdding Hugging Face remote..." -ForegroundColor Green
$hfRemote = "https://huggingface.co/spaces/$username/$spacename"
git remote add hf $hfRemote
Write-Host "Remote added: $hfRemote" -ForegroundColor Green

# 6. Rename README for HF Spaces
Write-Host "`nPreparing README for Hugging Face..." -ForegroundColor Green
if (Test-Path README_SPACES.md) {
    Copy-Item README_SPACES.md README.md -Force
    git add README.md
    git commit -m "Add README.md for HF Spaces"
    Write-Host "README.md created!" -ForegroundColor Green
}

# 7. Push to Hugging Face
Write-Host "`n" -ForegroundColor Cyan
Write-Host "Ready to push to Hugging Face!" -ForegroundColor Cyan
Write-Host "This will upload your app to: $hfRemote" -ForegroundColor Cyan
Write-Host "`n"
$confirm = Read-Host "Press ENTER to push, or Ctrl+C to cancel"

Write-Host "`nPushing to Hugging Face..." -ForegroundColor Green
git push hf main

Write-Host "`n✅ Deployment complete!" -ForegroundColor Green
Write-Host "`nYour app will be live at:" -ForegroundColor Cyan
Write-Host "https://huggingface.co/spaces/$username/$spacename" -ForegroundColor Yellow
Write-Host "`n⚠️  IMPORTANT: Don't forget to add your API keys in Space Settings > Repository secrets!" -ForegroundColor Red
Write-Host "  - DATA_GOV_IN_API_KEY" -ForegroundColor Red
Write-Host "  - GEMINI_API_KEY" -ForegroundColor Red
Write-Host "`n"
