param(
    [switch]$AutoConfirm
)

Set-StrictMode -Version Latest

# ensure we're in a git repo
$repo = & git rev-parse --show-toplevel 2>$null
if (-not $repo) {
    Write-Error "Not inside a git repository. Run this from within your repo."
    exit 1
}
Push-Location $repo

$ignorePaths = @("data/","data.backup/")

# backup or create .gitignore
if (Test-Path .gitignore) {
    Copy-Item .gitignore .gitignore.pre-cleanup.bak -Force
} else {
    New-Item .gitignore -ItemType File | Out-Null
    Copy-Item .gitignore .gitignore.pre-cleanup.bak -Force
}

# append ignore entries if missing
$added = $false
foreach ($p in $ignorePaths) {
    if (-not (Select-String -Path .gitignore -SimpleMatch -Pattern $p -Quiet)) {
        Add-Content -Path .gitignore -Value $p
        $added = $true
    }
}

if ($added) {
    Write-Host "Updated .gitignore and creating commit for that change."
    git add .gitignore
    git commit -m "Add Postgres runtime paths to .gitignore"
} else {
    Write-Host ".gitignore already contains the paths; no change."
}

# create a cleanup branch
$branch = "cleanup/remove-pgdata-" + (Get-Date -Format "yyyyMMdd-HHmmss")
git checkout -b $branch | Out-Null

# list tracked files under the paths
$tracked = & git ls-files -- "data" "data.backup"
if (-not $tracked) {
    Write-Host "No tracked files under data/ or data.backup/ found. Nothing to remove."
    Pop-Location
    exit 0
}

Write-Host "The following tracked files will be removed from the index (they will remain on disk):"
$tracked | ForEach-Object { Write-Host "  $_" }
Write-Host ("Total tracked files: {0}" -f ($tracked | Measure-Object -Line).Lines)

if (-not $AutoConfirm) {
    $ans = Read-Host "Proceed to remove these from the repo index and commit on branch $branch? Type 'yes' to continue"
    if ($ans -ne "yes") {
        Write-Host "Aborted by user."
        Pop-Location
        exit 1
    }
}

# remove from index (keep local copies) and commit
git rm -r --cached --ignore-unmatch data data.backup
git commit -m "Remove Postgres runtime files (data/ and data.backup/) from repository; keep them locally and add to .gitignore"

Write-Host "${`n`}Commit created on branch ${branch}:"
git --no-pager show --name-only --pretty=format:"%h %an %s%nDate: %ad" HEAD

Write-Host "`nNext steps:"
Write-Host " 1) Inspect the branch locally: git log --name-status $branch"
Write-Host " 2) Push branch and open a PR for review: git push -u origin $branch"
Write-Host " 3) If you need to purge the files from history, use git-filter-repo or BFG (this rewrites history; coordinate with collaborators)."

Pop-Location