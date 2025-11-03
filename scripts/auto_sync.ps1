# autosync.ps1
# Run from the repo root. Example: powershell -ExecutionPolicy Bypass -File .\autosync.ps1

$ErrorActionPreference = "Stop"

function Exec($cmd, $allowFail = $false) {
  try {
    & git.exe $cmd
  } catch {
    if (-not $allowFail) { throw }
  }
}

# Ensure we're in a git repo
Exec "rev-parse --is-inside-work-tree" $false | Out-Null

# Basic identity safeguard (only needed once per machine)
# git config user.name "YourName"
# git config user.email "you@example.com"

# Fetch latest
Exec "fetch --all --prune"

# Check for local changes
$status = (& git.exe status --porcelain) -join "`n"
$hasLocalChanges = -not [string]::IsNullOrWhiteSpace($status)

# If dirty, stash them (keep index), pull, then re-apply
if ($hasLocalChanges) {
  Write-Host "[autosync] Local edits detected. Stashing..."
  Exec "stash push -u -k -m autosync-stash"
}

# Try a fast-forward first, then a normal pull if needed
try {
  Exec "merge --ff-only origin/main"
} catch {
  Exec "pull --rebase --autostash"
}

# Re-apply stash if we created one
$stashes = & git.exe stash list
if ($stashes -match "autosync-stash") {
  Write-Host "[autosync] Re-applying local edits..."
  Exec "stash pop" $true
}

# Stage and commit if there are new local changes
$afterStatus = (& git.exe status --porcelain) -join "`n"
$hasPostPullChanges = -not [string]::IsNullOrWhiteSpace($afterStatus)
if ($hasPostPullChanges) {
  Exec "add -A"
  # Only commit if something actually changed
  $diff = (& git.exe diff --cached --name-only) -join "`n"
  if (-not [string]::IsNullOrWhiteSpace($diff)) {
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    Exec "commit -m `"autosync: $timestamp`""
    Write-Host "[autosync] Committed local changes."
  }
}

# Push if our branch is ahead
$ahead = (& git.exe rev-list --count origin/main..HEAD).Trim()
if ($ahead -match '^\d+$' -and [int]$ahead -gt 0) {
  Exec "push -u origin main"
  Write-Host "[autosync] Pushed $ahead commit(s)."
} else {
  Write-Host "[autosync] Nothing to push."
}

Write-Host "[autosync] Done."
