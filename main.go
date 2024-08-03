package main

import (
    "mercury/src/LedgerClient"
    repl "mercury/src/Repl"
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

  replMode := flag.Bool("repl-mode", false, "Start mercury in repl mode")

  flag.Parse()

  if *replMode {
    repl.Repl()
  } else {
    connectionVal := *connectionStr
    ledgerClient := LedgerClient.IntializeGRPCConnection(connectionVal, token, sandbox, applicationId, startPoint, *configPath)
    ledgerClient.WatchTransactionTreeStream()
  }
}
