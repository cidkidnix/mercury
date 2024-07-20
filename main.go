package main

import (
    "mercury/src/LedgerClient"
    "log"
    "flag"
)

func main() {
  log.SetFlags(0)
  connectionStr := flag.String("connection", "localhost:6865", "Where to connect to")
  token := flag.String("token", "", "Token to use")
  sandbox := flag.Bool("sandbox", false, "Running against daml sandbox")
  applicationId := flag.String("applicationid", "", "Application ID to use")
  startPoint := flag.String("start-point", "", "Where to start the PQS at")
  configPath := flag.String("config", "./config.json", "What config to load")

  flag.Parse()

  connectionVal := *connectionStr
  ledgerClient := LedgerClient.IntializeGRPCConnection(connectionVal, token, sandbox, applicationId, startPoint, *configPath)
  ledgerClient.WatchTransactionTreeStream()
}
