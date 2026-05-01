####  Logon to SFTP
sftp -P 522 `
  -oHostKeyAlgorithms=+ssh-rsa `
  -oPubkeyAcceptedAlgorithms=+ssh-rsa `
  Harris_IUH@secure.edidrop.com

DBO6RS9pF18ouiW


####  Export locally from SFTP
sftp> cd 837I/OUT

sftp> cd 837P/OUT
sftp> get *.277 "J:/DATA_DIMENSIONS/IN/"
sftp> exit


####  Archive within SFTP
# 1) Create a file list based on what you downloaded
cd "J:/DATA_DIMENSIONS/IN/"
$localDir = "J:/DATA_DIMENSIONS/IN/"
$files = Get-ChildItem $localDir -Filter "*.277" | Select-Object -ExpandProperty Name

# 2) Generate sftp commands
$cmds = @()
foreach ($f in $files) {
  $cmds += "rename $f archive-277/$f"
}
# $cmds += "quit"

# 3) Write the batch file. Currently, just copy and paste it manually into the SFTP terminal.
$cmds | Set-Content -Encoding ASCII .\sftp-archive-277.txt
