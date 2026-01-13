sftp -P 522 `
  -oHostKeyAlgorithms=+ssh-rsa `
  -oPubkeyAcceptedAlgorithms=+ssh-rsa `
  Harris_IUH@secure.edidrop.com

DBO6RS9pF18ouiW

sftp> cd 837I/IN
sftp> put "J:\DATA_DIMENSIONS\OUT\export-837I-HB-IUHEALTHTPL-20260113.837"
sftp> exit