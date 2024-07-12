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
  startPoint := flag.String("start-point", "LEDGER_BEGIN", "Where to start the PQS at")

  dbSelection := flag.String("db-type", "sqlite", "What database backend to use")

  dbHost := flag.String("postgres-db-host", "localhost", "Where the DB is located")
  dbUser := flag.String("postgres-db-user", "postgres", "User to connect to the database as")
  dbPassword := flag.String("postgres-db-password", "", "Password for database")
  dbName := flag.String("postgres-db-name", "postgres", "Database Name")
  dbPort := flag.Int("postgres-db-port", 5432, "Database Port")
  dbSslMode := flag.String("postgres-db-ssl-mode", "disabled", "")

  var dbConfig *LedgerClient.DatabaseConnection


  flag.Parse()

  switch *dbSelection {
    case "postgres":
      dbConfig = &LedgerClient.DatabaseConnection {
        Host: dbHost,
        User: dbUser,
        Password: dbPassword,
        Dbname: dbName,
        Port: dbPort,
        Sslmode: dbSslMode,
      }
    default:
      dbConfig = nil
  }

  connectionVal := *connectionStr
  ledgerClient := LedgerClient.IntializeGRPCConnection(connectionVal, token, sandbox, applicationId, startPoint, dbConfig)
  ledgerClient.WatchTransactionStream()
  //ledgerClient.GetActiveContractSet()
}
