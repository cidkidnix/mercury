package LedgerClient

import (
    "log"
    "io"
    "time"
    "google.golang.org/grpc"
    "google.golang.org/protobuf/encoding/protojson"
    "github.com/digital-asset/dazl-client/v7/go/api/com/daml/ledger/api/v1"
    "github.com/digital-asset/dazl-client/v7/go/api/com/daml/ledger/api/v1/admin"
    "context"
    "slices"
    "pqs/src/Database"
    "encoding/json"
    "fmt"

)

type LedgerId string;

type CreateEventWrapper struct {
  ContractKey *v1.Value
  CreateArguments *v1.Record
  ContractID string
  Witnesses []string
  Observers []string
  TemplateId *v1.Identifier
}

type LedgerContext struct {
  GetConnection func()(ConnectionWrapper)
  GetConnectionWithoutTimeout func()(ConnectionWrapper)
}

type ConnectionWrapper struct {
  connection *grpc.ClientConn
  ctx *context.Context
  cancelCtx context.CancelFunc
}

func IntializeGRPCConnection(connStr string) (ledgerContext LedgerContext) {
  return LedgerContext{
      GetConnectionWithoutTimeout: func() (ConnectionWrapper) {
        ctx := context.Background()
        conn, err := grpc.DialContext(ctx, connStr, grpc.WithInsecure(), grpc.WithBlock())
        if err != nil {
          log.Fatalf("did not connect")
        }
        return ConnectionWrapper {
          connection: conn,
          ctx: &ctx,
          cancelCtx: nil,
        }
      },
      GetConnection: func() (ConnectionWrapper) {
        ctx := context.Background()
        ctx, cancelCtx := context.WithTimeout(ctx, time.Second * 10)
        conn, err := grpc.DialContext(ctx, connStr, grpc.WithInsecure(), grpc.WithBlock())
        if err != nil {
          log.Fatalf("did not connect")
        }
        return ConnectionWrapper {
          connection: conn,
          ctx: &ctx,
          cancelCtx: cancelCtx,
        }
      },
  }
}

func (ledgerContext *LedgerContext) TraverseTransactionTreeEvents(parties []string, tree *v1.TransactionTree, ignoreList []string, channel chan CreateEventWrapper) () {
   for _, v := range (tree.EventsById) {
     data := v.GetCreated()
     if data == nil {
        event := v.GetExercised()
        for _, e := range (event.ChildEventIds) {
          log.Printf("Found ExercisedEvent %s", event.EventId)
          if slices.Contains(ignoreList, event.EventId) {
            log.Printf("TraverseTransactionTreeEvents: Already Seen %s, Bailing out of recurse", event.EventId)
          } else {
            ignoreList := append(ignoreList, event.EventId)
            tree := ledgerContext.FetchFromTransactionServiceByEventId(e, parties)
            ledgerContext.TraverseTransactionTreeEvents(parties, tree, ignoreList, channel)
          }
        }
      } else {
        log.Printf("CreatedEvent ignored")
        log.Printf("ContractID: %s", data.ContractId)
        channel <- CreateEventWrapper {
            ContractID: data.ContractId,
            ContractKey: data.ContractKey,
            CreateArguments: data.CreateArguments,
            TemplateId: data.TemplateId,
            Witnesses: data.WitnessParties,
            Observers: data.Observers,
        }
      }
   }
}

func (ledgerContext *LedgerContext) FetchFromTransactionServiceById(transactionId string, parties []string) (transaction *v1.TransactionTree) {
  connection := ledgerContext.GetConnection()
  defer connection.connection.Close()
  defer connection.cancelCtx()

  client := v1.NewTransactionServiceClient(connection.connection)
  ledgerId := ledgerContext.GetLedgerId()
  response, err := client.GetTransactionById(*connection.ctx, &v1.GetTransactionByIdRequest{
    LedgerId: ledgerId,
    TransactionId: transactionId,
    RequestingParties: parties,
  })

  if err != nil {
    panic(err)
  }

  return response.Transaction

}

func (ledgerContext *LedgerContext) FetchFromTransactionServiceByEventId(eventId string, parties []string) (transaction *v1.TransactionTree) {
  connection := ledgerContext.GetConnection()
  defer connection.connection.Close()
  defer connection.cancelCtx()

  client := v1.NewTransactionServiceClient(connection.connection)
  ledgerId := ledgerContext.GetLedgerId()
  response, err := client.GetTransactionByEventId(*connection.ctx, &v1.GetTransactionByEventIdRequest{
    LedgerId: ledgerId,
    EventId: eventId,
    RequestingParties: parties,
  })

  if err != nil {
    panic(err)
  }

  return response.Transaction

}

func (ledgerContext *LedgerContext) WatchCommandService() {
  connection := ledgerContext.GetConnectionWithoutTimeout()
  defer connection.connection.Close()

  parties := ledgerContext.GetParties()
  ledgerId := ledgerContext.GetLedgerId()
  client := v1.NewCommandCompletionServiceClient(connection.connection)
  response, err := client.CompletionStream(*connection.ctx, &v1.CompletionStreamRequest{
    LedgerId: ledgerId,
    Parties: parties,
    ApplicationId: "damlscriptappid",
    Offset: &v1.LedgerOffset {
      Value: &v1.LedgerOffset_Boundary {
          Boundary: v1.LedgerOffset_LEDGER_BEGIN,
      },
    },
  })

  db := Database.InitializeDB()
  if err != nil {
    panic(err)
  }
  channel := make(chan CreateEventWrapper)
  go func() {
    for {
        x := <-channel
        log.Printf("Found Create with ContractID: %s", x.ContractID)
        event := ledgerContext.GetEventByContractId(parties, x.ContractID)
        db.Create(&Database.CreatesTable{
          ContractID: x.ContractID,
        })
        if event.GetArchiveEvent() == nil {
            cKey, _ := protojson.Marshal(x.ContractKey)
            createArgs, _ := protojson.Marshal(x.CreateArguments)
            tid := x.TemplateId
            fTid := fmt.Sprintf("%s:%s:%s", tid.PackageId, tid.ModuleName, tid.EntityName)
            w, _ := json.Marshal(x.Witnesses)
            o, _ := json.Marshal(x.Observers)
            db.Create(&Database.ContractTable{
                CreateArguments: createArgs,
                ContractKey: cKey,
                ContractID: x.ContractID,
                Observers: o,
                Witnesses: w,
                TemplateFqn: fTid,
            })
        } else {
          log.Printf("ContractID: %s has been archived", x.ContractID)
          db.Create(&Database.ArchivesTable {
            ContractID: x.ContractID,
          })
        }
    }
  }()
  for {
    resp, err := response.Recv()
    if err == io.EOF {
        return
    } else if err == nil {
      for _, value := range resp.Completions {
          transaction := ledgerContext.FetchFromTransactionServiceById(value.TransactionId, parties)
          var t []string
          ledgerContext.TraverseTransactionTreeEvents(parties, transaction, t, channel)
      }
    }

    if err != nil {
        panic(err)
    }
  }
}

func (ledgerContext *LedgerContext) GetEventByContractId(parties []string, contractId string) (events *v1.GetEventsByContractIdResponse) {
  connection := ledgerContext.GetConnection()
  defer connection.connection.Close()
  defer connection.cancelCtx()

  client := v1.NewEventQueryServiceClient(connection.connection)
  response, err := client.GetEventsByContractId(*connection.ctx, &v1.GetEventsByContractIdRequest {
    ContractId: contractId,
    RequestingParties: parties,
  })
  if err != nil {
    panic(err)
  }
  return response
}

func (ledgerContext *LedgerContext) GetLedgerId() string {
    connection := ledgerContext.GetConnection()
    defer connection.cancelCtx()
    defer connection.connection.Close()

    client := v1.NewLedgerIdentityServiceClient(connection.connection)
    response, err := client.GetLedgerIdentity(*connection.ctx, &v1.GetLedgerIdentityRequest{})
    if err != nil {
      log.Fatalf("Failed to get ledger identity")
    }
    log.Printf("Ledger ID: %s", response.LedgerId)
    return response.LedgerId
}

func (ledgerContext *LedgerContext) GetParties() (allParties []string) {
  connection := ledgerContext.GetConnection()
  defer connection.cancelCtx()
  defer connection.connection.Close()

  client := admin.NewPartyManagementServiceClient(connection.connection)
  response, err := client.ListKnownParties(*connection.ctx, &admin.ListKnownPartiesRequest{})
  if err != nil {
    log.Fatalf("%s", err)
  }
  var partyList []string
  for _, value := range response.PartyDetails {
    partyList = append(partyList, value.Party)
  }

  return partyList
}

func (ledgerContext *LedgerContext) GetActiveContractSet() {
    connection := ledgerContext.GetConnection()
    defer connection.cancelCtx()
    defer connection.connection.Close()

    ledgerId := ledgerContext.GetLedgerId()
    parties := ledgerContext.GetParties()

    client := v1.NewActiveContractsServiceClient(connection.connection)
    partyMap := make(map[string]*v1.Filters)

    for _, value := range parties {
      partyMap[value] = &v1.Filters{}
    }
    response, err := client.GetActiveContracts(*connection.ctx, &v1.GetActiveContractsRequest{
      LedgerId: ledgerId,
      Filter: &v1.TransactionFilter{
        FiltersByParty: partyMap,
      },
    })
    if err != nil {
        log.Fatalf("%s", err)
    }
    for {
      resp, err := response.Recv()
      if err == io.EOF {
          return
      } else if err == nil {
        for _, value := range resp.ActiveContracts {
            log.Printf("Found Contract in ACS: %s", value.ContractId)
        }
      }

      if err != nil {
          panic(err)
      }
    }
}
