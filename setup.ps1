<#
.SYNOPSIS
Installs TresorBot Sun, Earth, or both from the official Windows amd64 GitHub release.

.DESCRIPTION
Downloads and verifies every selected release asset before changing the installation.
Installs binaries under ~/.local/bin and creates (but never overwrites) role configs under
~/.config/tresorbot. Does not require administrator privileges or modify PATH/services.

.PARAMETER Role
Required: sun, earth, or both.

.PARAMETER Prerelease
Include prereleases when selecting the newest published release.

.PARAMETER Version
Install a pinned version. Accepts vN.N.N with an optional prerelease suffix.

.EXAMPLE
.\setup.ps1 -Role sun

.EXAMPLE
.\setup.ps1 -Role both -Prerelease

.EXAMPLE
.\setup.ps1 -Role earth -Version v1.2.3
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidateSet('sun', 'earth', 'both')]
    [string] $Role,
    [switch] $Prerelease,
    [string] $Version = $env:TRESORBOT_VERSION
)

$ErrorActionPreference = 'Stop'

function Get-Release {
    param([string] $PinnedVersion, [bool] $WantPrerelease)
    $repo = 'https://api.github.com/repos/Ladbaby/TresorBot/releases'
    $headers = @{ Accept = 'application/vnd.github+json'; 'User-Agent' = 'TresorBot-setup' }
    if ($PinnedVersion) {
        if ($PinnedVersion -notmatch '^v[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z]+(?:[.-][0-9A-Za-z]+)*)?$') {
            throw "Invalid version '$PinnedVersion'. Expected vN.N.N with an optional prerelease suffix."
        }
        return [pscustomobject]@{ tag_name = $PinnedVersion }
    }
    if ($WantPrerelease) {
        $releases = Invoke-RestMethod -Uri $repo -Headers $headers
        $release = $releases | Where-Object { -not $_.draft } | Select-Object -First 1
    } else {
        $release = Invoke-RestMethod -Uri "$repo/latest" -Headers $headers
        if ($release.draft) { $release = $null }
    }
    if (-not $release) { throw 'No matching published release was found.' }
    return $release
}

function Get-Asset {
    param($Release, [string] $Name)
    return [pscustomobject]@{ browser_download_url = "https://github.com/Ladbaby/TresorBot/releases/download/$($Release.tag_name)/$Name" }
}

function Confirm-Checksum {
    param([string] $Path, [string] $Name, [string[]] $Manifest)
    $entries = @($Manifest | Where-Object { $_ -match ('^([0-9a-fA-F]{64})\s+\*?(?:\./)?' + [regex]::Escape($Name) + '\s*$') })
    if ($entries.Count -ne 1) { throw "Missing or ambiguous checksum for '$Name'." }
    $expected = [regex]::Match($entries[0], '^([0-9a-fA-F]{64})').Groups[1].Value
    $stream = [IO.File]::OpenRead($Path)
    $sha = [Security.Cryptography.SHA256]::Create()
    try { $actual = [BitConverter]::ToString($sha.ComputeHash($stream)).Replace('-', '') }
    finally { $stream.Dispose(); $sha.Dispose() }
    if ($actual -ne $expected) { throw "SHA256 verification failed for '$Name'." }
}

try {
    if ($env:OS -ne 'Windows_NT') { throw 'This installer supports Windows only.' }
    $nativeArch = $env:PROCESSOR_ARCHITEW6432
    if (-not $nativeArch) { $nativeArch = $env:PROCESSOR_ARCHITECTURE }
    if ($nativeArch -ne 'AMD64') { throw "Windows amd64 is required (detected '$nativeArch')." }

    Add-Type -AssemblyName System.IO.Compression
    Add-Type -AssemblyName System.IO.Compression.FileSystem
    $wantPrerelease = $Prerelease.IsPresent -or ($env:TRESORBOT_PRERELEASE -match '^(1|true|yes)$')
    if ($Version -and $Prerelease) { throw 'Use either -Version or -Prerelease, not both.' }
    $release = Get-Release -PinnedVersion $Version -WantPrerelease $wantPrerelease
    $versionTag = [string]$release.tag_name
    if ($versionTag -notmatch '^v[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z]+(?:[.-][0-9A-Za-z]+)*)?$') { throw "Release has invalid version tag '$versionTag'." }

    $roles = if ($Role -eq 'both') { @('sun', 'earth') } else { @($Role) }
    $installHome = if ($env:HOME) { $env:HOME } else { $env:USERPROFILE }
    if (-not $installHome) { throw 'Could not determine home directory.' }
    $binDir = Join-Path $installHome '.local/bin'
    $configDir = Join-Path $installHome '.config/tresorbot'
    $stage = Join-Path ([IO.Path]::GetTempPath()) ('tresorbot-setup-' + [guid]::NewGuid().ToString('N'))
    [void][IO.Directory]::CreateDirectory($stage)

    try {
        $manifestAsset = Get-Asset $release 'checksums.txt'
        $manifestPath = Join-Path $stage 'checksums.txt'
        Invoke-WebRequest -Uri $manifestAsset.browser_download_url -OutFile $manifestPath -UseBasicParsing
        $manifest = Get-Content -LiteralPath $manifestPath
        $prepared = @()

        foreach ($itemRole in $roles) {
            $base = "tresorbot-$itemRole-windows-amd64-$versionTag"
            $zipAsset = Get-Asset $release "$base.zip"
            $configAsset = Get-Asset $release "$itemRole.example.yaml"
            $zipPath = Join-Path $stage "$base.zip"
            $examplePath = Join-Path $stage "$itemRole.example.yaml"
            Invoke-WebRequest -Uri $zipAsset.browser_download_url -OutFile $zipPath -UseBasicParsing
            Confirm-Checksum $zipPath "$base.zip" $manifest
            if (-not (Test-Path -LiteralPath (Join-Path $configDir "$itemRole.yaml"))) {
                Invoke-WebRequest -Uri $configAsset.browser_download_url -OutFile $examplePath -UseBasicParsing
                Confirm-Checksum $examplePath "$itemRole.example.yaml" $manifest
                if ((Get-Item -LiteralPath $examplePath).Length -le 0) { throw 'Empty example configuration.' }
            }

            $exeName = "tresorbot-$itemRole-windows-amd64.exe"
            $exePath = Join-Path $stage $exeName
            $archive = [IO.Compression.ZipFile]::OpenRead($zipPath)
            try {
                $entries = @($archive.Entries | Where-Object { $_.FullName -ceq $exeName })
                if ($entries.Count -ne 1 -or $entries[0].Length -le 0) { throw "Archive must contain exactly one non-empty '$exeName'." }
                $input = $entries[0].Open()
                $output = [IO.File]::Create($exePath)
                try { $input.CopyTo($output) } finally { $output.Dispose(); $input.Dispose() }
            } finally { $archive.Dispose() }
            if ((Get-Item -LiteralPath $exePath).Length -le 0) { throw "Extracted executable '$exeName' is empty." }
            $prepared += [pscustomobject]@{ Role = $itemRole; Exe = $exePath; Example = $examplePath }
        }

        # All selected assets are downloaded, verified, and extracted before installation starts.
        [void][IO.Directory]::CreateDirectory($binDir)
        [void][IO.Directory]::CreateDirectory($configDir)
        foreach ($item in $prepared) {
            $destExe = Join-Path $binDir "tresorbot-$($item.Role).exe"
            if (Test-Path -LiteralPath $destExe) {
                $backup = "$destExe.bak"
                if (Test-Path -LiteralPath $backup) { Remove-Item -LiteralPath $backup -Force }
                Move-Item -LiteralPath $destExe -Destination $backup
            }
            Move-Item -LiteralPath $item.Exe -Destination $destExe -Force
            $destConfig = Join-Path $configDir "$($item.Role).yaml"
            if (-not (Test-Path -LiteralPath $destConfig)) { Copy-Item -LiteralPath $item.Example -Destination $destConfig }
        }
    } finally {
        if (Test-Path -LiteralPath $stage) { Remove-Item -LiteralPath $stage -Recurse -Force }
    }

    Write-Host "Installed TresorBot $versionTag ($($roles -join ', ')) under $binDir."
    foreach ($itemRole in $roles) {
        Write-Host "${itemRole}: edit '$configDir/$itemRole.yaml', then run:"
        Write-Host "  & `"$binDir/tresorbot-$itemRole.exe`" --config `"$configDir/$itemRole.yaml`""
        if ($itemRole -eq 'earth') { Write-Host 'Earth requires Node.js and Pi.' }
        if ($itemRole -eq 'sun') { Write-Host 'Configure Sun credentials/tokens, start Sun, then create a runner to enroll Earth.' }
    }
    Write-Host 'Add ~/.local/bin to your user PATH manually if desired; this installer does not change PATH.'
} catch {
    Write-Error $_
    exit 1
}
