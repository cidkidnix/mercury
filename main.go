package main

import (
    "pqs/src/LedgerClient"
    "flag"
)

func main() {
  connectionStr := flag.String("connection", "localhost:6865", "Where to connect to")
  flag.Parse()
  connectionVal := *connectionStr
  ledgerClient := LedgerClient.IntializeGRPCConnection(connectionVal)
  ledgerClient.WatchCommandService()
}
