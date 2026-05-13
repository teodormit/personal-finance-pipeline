# Registers the weekly backup task in Windows Task Scheduler.
# Run once: .\scripts\setup_backup_schedule.ps1
# Will prompt for administrator approval via Windows UAC automatically.

# Self-elevate: if not already admin, re-launch this script with a UAC prompt
if (-not ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    Start-Process powershell.exe `
        -ArgumentList "-NonInteractive -ExecutionPolicy Bypass -File `"$PSCommandPath`"" `
        -Verb RunAs -Wait
    exit
}

$ScriptPath = "C:\Code Repos\personal-finance-pipeline\scripts\backup.ps1"
$TaskName   = "FinancePipelineBackup"
$Username   = "$env:USERDOMAIN\$env:USERNAME"

# XML defines every task property explicitly — avoids parameter compatibility issues
$TaskXml = @"
<?xml version="1.0" encoding="UTF-16"?>
<Task version="1.2" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <RegistrationInfo>
    <Description>Weekly pg_dump backup of finance_warehouse. Runs on next start-up if the laptop was off at the scheduled time.</Description>
  </RegistrationInfo>
  <Principals>
    <Principal id="Author">
      <UserId>$Username</UserId>
      <LogonType>InteractiveToken</LogonType>
      <RunLevel>HighestAvailable</RunLevel>
    </Principal>
  </Principals>
  <Settings>
    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>
    <StartWhenAvailable>true</StartWhenAvailable>
    <ExecutionTimeLimit>PT10M</ExecutionTimeLimit>
    <Enabled>true</Enabled>
  </Settings>
  <Triggers>
    <CalendarTrigger>
      <StartBoundary>2026-05-17T21:00:00</StartBoundary>
      <ScheduleByWeek>
        <WeeksInterval>1</WeeksInterval>
        <DaysOfWeek><Sunday /></DaysOfWeek>
      </ScheduleByWeek>
    </CalendarTrigger>
  </Triggers>
  <Actions Context="Author">
    <Exec>
      <Command>powershell.exe</Command>
      <Arguments>-NonInteractive -ExecutionPolicy Bypass -File "$ScriptPath"</Arguments>
    </Exec>
  </Actions>
</Task>
"@

Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction SilentlyContinue
Register-ScheduledTask -TaskName $TaskName -Xml $TaskXml -Force | Out-Null

if ($?) {
    Write-Host ""
    Write-Host "Backup task registered successfully." -ForegroundColor Green
    Write-Host "  Schedule : Every Sunday at 21:00"
    Write-Host "  Missed   : Runs automatically on next laptop start-up"
    Write-Host "  Script   : $ScriptPath"
    Write-Host ""
    Write-Host "To run a backup right now: .\scripts\backup.ps1"
} else {
    Write-Host "ERROR: Failed to register task." -ForegroundColor Red
}
