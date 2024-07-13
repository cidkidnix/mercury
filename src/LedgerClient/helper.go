package LedgerClient

import (
    "log"
    "io"
    "time"
    "google.golang.org/grpc"
    "github.com/digital-asset/dazl-client/v7/go/api/com/daml/ledger/api/v1"
    "github.com/digital-asset/dazl-client/v7/go/api/com/daml/ledger/api/v1/admin"
    "google.golang.org/grpc/metadata"
    "context"
    "slices"
    "mercury/src/Database"
    "encoding/json"
    "fmt"
    "gorm.io/gorm"
    "os"
    "reflect"
)


type DatabaseConnection struct {
  Host *string
  User *string
  Password *string
  Dbname *string
  Port *int
  Sslmode *string
}

type LedgerOffsetWrapper struct {
  Boundary *v1.LedgerOffset_Boundary
  Absolute *v1.LedgerOffset_Absolute
}

type ArchivedEventWrapper struct {
  ContractId string
  Offset string
}

type CreateEventWrapper struct {
  ContractKey *v1.Value
  CreateArguments *v1.Record
  ContractID string
  Witnesses []string
  Observers []string
  Signatories []string
  TemplateId *v1.Identifier
  Offset string
}

type ExercisedEventWrapper struct {
  ContractID string
  Consuming bool
}

type LedgerContext struct {
  GetConnection func()(ConnectionWrapper)
  GetConnectionWithoutTimeout func()(ConnectionWrapper)
  Sandbox bool
  ApplicationId string
  LedgerId string
  StartPoint string
  DB *gorm.DB
  DBConnection *DatabaseConnection
}

type ConnectionWrapper struct {
  connection *grpc.ClientConn
  ctx *context.Context
  cancelCtx context.CancelFunc
}

type ChannelWrapper struct {
  reciever chan(any)
  finished chan(bool)
}

type wrappedStream struct {
	grpc.ClientStream
}

func (w *wrappedStream) RecvMsg(m any) error {
	//logger("Receive a message (Type: %T) at %v", m, time.Now().Format(time.RFC3339))
	return w.ClientStream.RecvMsg(m)
}

func (w *wrappedStream) SendMsg(m any) error {
	//logger("Send a message (Type: %T) at %v", m, time.Now().Format(time.RFC3339))
	return w.ClientStream.SendMsg(m)
}

func newWrappedStream(s grpc.ClientStream) grpc.ClientStream {
	return &wrappedStream{s}
}

// streamInterceptor is an example stream interceptor.
func genStreamInterceptor(token string) func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
       nt := fmt.Sprintf("Bearer %s", token)
       ctx = metadata.AppendToOutgoingContext(ctx, "Authorization", nt)
	   s, err := streamer(ctx, desc, cc, method, opts...)
       if err != nil {
         return nil, err
       }
       return newWrappedStream(s), nil
    }
}


func genUnaryInterceptor(token string) func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
  return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
    nt := fmt.Sprintf("Bearer %s", token)
    ctx = metadata.AppendToOutgoingContext(ctx, "Authorization", nt)
	err := invoker(ctx, method, req, reply, cc, opts...)
	return err
  }
}

func IntializeGRPCConnection(connStr string, authToken *string, sandbox *bool, applicationId *string, startPoint *string, dbConnection *DatabaseConnection) (ledgerContext LedgerContext) {
  return LedgerContext{
      GetConnectionWithoutTimeout: func() (ConnectionWrapper) {
        ctx := context.Background()
        conn, err := grpc.DialContext(ctx, connStr, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithUnaryInterceptor(genUnaryInterceptor(*authToken)), grpc.WithStreamInterceptor(genStreamInterceptor(*authToken)))
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
        conn, err := grpc.DialContext(ctx, connStr, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithUnaryInterceptor(genUnaryInterceptor(*authToken)), grpc.WithStreamInterceptor(genStreamInterceptor(*authToken)))
        if err != nil {
          log.Fatalf("did not connect")
        }
        return ConnectionWrapper {
          connection: conn,
          ctx: &ctx,
          cancelCtx: cancelCtx,
        }
      },
      Sandbox: *sandbox,
      ApplicationId: *applicationId,
      LedgerId: "",
      StartPoint: *startPoint,
      DBConnection: dbConnection,
      DB: nil,
  }
}

func (ledgerContext *LedgerContext) LogInfo(message string) {
  log.Printf("\033[0;35m[INFO]\033[0m %s", message)
}

func (ledgerContext *LedgerContext) LogDebug(message string) {
  log.Printf("\033[0;34m[DEBUG]]\033[0m %s", message)
}

func (ledgerContext *LedgerContext) LogContract(message string) {
  enableContractLog := os.Getenv("LOG_CONTRACT")
  if enableContractLog == "1" {
    log.Printf("\033[0;36m[CONTRACT]\033[0m %s", message)
  }
}

func IsHashable(kind reflect.Kind) bool {
  switch kind {
    case reflect.String:
      return true
    default:
      return false
  }
}

func (ledgerContext *LedgerContext) TraverseTransactionTreeEvents(parties []string, tree *v1.TransactionTree, ignoreList []string, channel chan CreateEventWrapper, channelExercised chan ExercisedEventWrapper) () {
   log.Printf("Offset: %s", tree.Offset)
   log.Printf("TransactionTree Roots: %s", tree.RootEventIds)
   for _, v := range (tree.EventsById) {
     data := v.GetCreated()
     if data == nil {
        event := v.GetExercised()
        channelExercised <- ExercisedEventWrapper{
          ContractID: event.ContractId,
          Consuming: event.Consuming,
        }
        //log.Printf("CHILD EVENT IDS: %s", event.ChildEventIds)
        for _, e := range (event.ChildEventIds) {
          log.Printf("Found ExercisedEvent %s", event.EventId)
          if slices.Contains(ignoreList, event.EventId) {
            log.Printf("TraverseTransactionTreeEvents: Already Seen %s, Bailing out of recurse", event.EventId)
          } else {
            ignoreList := append(ignoreList, event.EventId)
            tree := ledgerContext.FetchFromTransactionServiceByEventId(e, parties)
            ledgerContext.TraverseTransactionTreeEvents(parties, tree, ignoreList, channel, channelExercised)
            log.Printf("Done with traversal for event %s", event.EventId)
          }
        }
      } else {
        log.Printf("CreatedEvent: %s", data.EventId)
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

func (ledgerContext *LedgerContext) GetDatabaseConnection() *gorm.DB {
  if ledgerContext.DB != nil {
    return(ledgerContext.DB)
  }

  var db *gorm.DB
  if ledgerContext.DBConnection != nil {
    dbCon := ledgerContext.DBConnection
    db = Database.InitializePostgresDB(*dbCon.Host, *dbCon.User, *dbCon.Password, *dbCon.Dbname, *dbCon.Port, *dbCon.Sslmode)
  } else {
    db = Database.InitializeSQLiteDB()
  }
  ledgerContext.DB = db
  return(db)
}

func (ledgerContext *LedgerContext) GetStartPoint() *v1.LedgerOffset {
  switch (ledgerContext.StartPoint) {
    case "LEDGER_BEGIN":
      return (&v1.LedgerOffset {
        Value: &v1.LedgerOffset_Boundary {
          Boundary: v1.LedgerOffset_LEDGER_BEGIN,
        },
      })
    case "OLDEST":
      db := ledgerContext.GetDatabaseConnection()
      var lOffset Database.LastOffset
      lastOffset := db.Last(&lOffset)


      var offset *v1.LedgerOffset

      if lastOffset.Error != nil {
        offset = &v1.LedgerOffset {
          Value: &v1.LedgerOffset_Boundary {
            Boundary: v1.LedgerOffset_LEDGER_BEGIN,
          },
        }
      } else {
        ledgerContext.LogInfo(fmt.Sprintf("Starting from Offset: %s", lOffset.Offset))
        offset = &v1.LedgerOffset {
          Value: &v1.LedgerOffset_Absolute {
            Absolute: lOffset.Offset,
          },
        }
      }
      return offset
    case "LATEST":
      db := ledgerContext.GetDatabaseConnection()
      var lOffset Database.LastOffset
      lastOffset := db.Last(&lOffset)

      var offset *v1.LedgerOffset

      if lastOffset.Error != nil {
        offset =  &v1.LedgerOffset {
          Value: &v1.LedgerOffset_Absolute {
            Absolute: ledgerContext.GetActiveContractSet(),
          },
        }
      } else {
        ledgerContext.LogInfo(fmt.Sprintf("Starting from Offset: %s", lOffset.Offset))
        offset = &v1.LedgerOffset {
          Value: &v1.LedgerOffset_Absolute {
            Absolute: lOffset.Offset,
          },
        }
      }


      return offset
    default:
      ledgerContext.LogInfo(fmt.Sprintf("Start Point %s Not Supported! Starting from OLDEST", ledgerContext.StartPoint))
      log.Printf("Starting from Offset: %s", ledgerContext.StartPoint)
      offset := &v1.LedgerOffset {
        Value: &v1.LedgerOffset_Absolute {
          Absolute: ledgerContext.StartPoint,
        },
      }
      return offset
  }
}

func (ledgerContext *LedgerContext) ParseLedgerData(value *v1.Value) (any) {
  switch x := value.GetSum().(type) {
    case (*v1.Value_Record):
      record := x.Record
      nMap := make(map[string]any)
      if record != nil {
        fields := record.GetFields()
        if fields != nil {
          for _, v := range(fields) {
            nMap[v.Label] = ledgerContext.ParseLedgerData(v.Value)
          }
        }
      }
      return nMap

    case (*v1.Value_Party):
      return x.Party
    case (*v1.Value_Text):
      return x.Text
    case (*v1.Value_List):
      emptyMap := make(map[string]any)
      if x.List.Elements != nil {
        var lMap [](any)
        for _, v := range(x.List.Elements) {
          lMap = append(lMap,  ledgerContext.ParseLedgerData(v))
        }
        return lMap
      } else {
        return emptyMap
      }
    case (*v1.Value_Date):
      return x.Date
    case (*v1.Value_Optional):
      newMap := make(map[string]any)
      emptyMap := make(map[string]any)
      if x.Optional.GetValue() != nil {
        newMap["Some"] = ledgerContext.ParseLedgerData(x.Optional.Value)
      } else {
        newMap["None"] = emptyMap
      }
      return newMap
    case (*v1.Value_Int64):
      return x.Int64
    case (*v1.Value_Numeric):
      return x.Numeric
    case (*v1.Value_Timestamp):
      return x.Timestamp
    case (*v1.Value_Bool):
      return x.Bool
    case (*v1.Value_ContractId):
      return x.ContractId
    case (*v1.Value_Map):
      newMap := make(map[string]any)
      for _, v := range(x.Map.Entries) {
        newMap[v.Key] = ledgerContext.ParseLedgerData(v.Value)
      }
      return newMap
    case (*v1.Value_GenMap):
      var mapList [](map[string]any)
      tMap := make(map[string]any)
      for _, v := range(x.GenMap.Entries) {
        newMap := make(map[string]any)
        keyVal := ledgerContext.ParseLedgerData(v.Key)
        keyKind := reflect.TypeOf(keyVal).Kind()
        // Decode the GenMap into a regular Map if our keys are hashable and are not empty
        if IsHashable(keyKind) && keyVal.(string) != "" {
          tMap[keyVal.(string)] = ledgerContext.ParseLedgerData(v.Value)
        } else {
          newMap["key"] = ledgerContext.ParseLedgerData(v.Key)
          newMap["value"] = ledgerContext.ParseLedgerData(v.Value)
        }

        if len(newMap) > 0 {
          mapList = append(mapList, newMap)
        }
      }
      // Make sure we append the correct bits into the list (if applicable)
      if (len(mapList) > 0) {
        if (len(tMap) > 0) {
          mapList = append(mapList, tMap)
        }
        return mapList
      } else {
        return tMap
      }

    case (*v1.Value_Variant):
      newMap := make(map[string]any)
      newMap[x.Variant.Constructor] = ledgerContext.ParseLedgerData(x.Variant.Value)
      return newMap
    case (*v1.Value_Enum):
      return x.Enum.Constructor
    case (*v1.Value_Unit):
      emptyMap := make(map[string]any)
      return emptyMap
  }
  if value == nil {
    emptyMap := make(map[string]any)
    return emptyMap
  }
  panic(fmt.Sprintf("%s", value))
  // We should never hit this case, which is why panic is produced above
  return nil
}

func (ledgerContext *LedgerContext) WatchTransactionStream() {
  connection := ledgerContext.GetConnectionWithoutTimeout()
  defer connection.connection.Close()

  db := ledgerContext.GetDatabaseConnection()
  ledgerId := ledgerContext.GetLedgerId()
  ledgerContext.LedgerId = ledgerId
  offset := ledgerContext.GetStartPoint()


  var parties []string
  parties = ledgerContext.GetParties()


  partyMap := make(map[string]*v1.Filters)
  for _, value := range parties {
    partyMap[value] = &v1.Filters{}
  }
  pipelineParties,  _ := json.Marshal(parties)
  ledgerContext.LogInfo(fmt.Sprintf("Staring pipeline on behalf of Parties: %v", string(pipelineParties)))

  client := v1.NewTransactionServiceClient(connection.connection)
  response, err := client.GetTransactions(*connection.ctx, &v1.GetTransactionsRequest {
    LedgerId: ledgerId,
    Filter: &v1.TransactionFilter {
      FiltersByParty: partyMap,
    },
    Begin: offset,
    Verbose: true,
  })
  if err != nil {
    panic(err)
  }

  createChannel := make(chan CreateEventWrapper)
  archiveChannel := make(chan ArchivedEventWrapper)

  dbCommitChannel := make(chan func()())

  go func() {
    for {
      x, more := <-dbCommitChannel
      if !more {
        return
      }
      x()
    }
  }()
  go func() {
    for {
        select {
          case x, more := <-createChannel:
            if !more {
              return
            }
            ledgerContext.LogContract(fmt.Sprintf("Create ContractID: %s", x.ContractID))
            cKey := ledgerContext.ParseLedgerData(x.ContractKey)
            cKeyS, _ := json.Marshal(cKey)
            //cKey, _ := protojson.Marshal(x.ContractKey)
            createArgs := ledgerContext.ParseLedgerData(&v1.Value {
                Sum: &v1.Value_Record {
                    Record: x.CreateArguments,
                },
            })
            createArgsS, _ := json.Marshal(createArgs)
            tid := x.TemplateId
            fTid := fmt.Sprintf("%s:%s:%s", tid.PackageId, tid.ModuleName, tid.EntityName)
            w, _ := json.Marshal(x.Witnesses)
            o, _ := json.Marshal(x.Observers)
            s, _ := json.Marshal(x.Signatories)
            dbCommitChannel <- func()() {
                db.FirstOrCreate(&Database.CreatesTable{ ContractID: x.ContractID }, Database.CreatesTable { ContractID: x.ContractID })
            }
            dbCommitChannel <- func()() {
              db.FirstOrCreate(&Database.ContractTable{
                  CreateArguments: createArgsS,
                  ContractKey: cKeyS,
                  ContractID: x.ContractID,
                  Observers: o,
                  Witnesses: w,
                  Signatories: s,
                  TemplateFqn: fTid,
                  Offset: x.Offset,
              }, Database.ContractTable { ContractID: x.ContractID })
            }
            dbCommitChannel <- func()() {
              db.Save(&Database.LastOffset{ Id: 1, Offset: x.Offset })
            }
          case y, more := <-archiveChannel:
            if !more {
              return
            }
            dbCommitChannel <- func()() {
              db.FirstOrCreate(&Database.ArchivesTable { ContractID: y.ContractId, Offset: y.Offset }, Database.ArchivesTable { ContractID: y.ContractId })
              db.Save(&Database.LastOffset{ Id: 1, Offset: y.Offset })
            }
        }
    }
  }()
  for {
    resp, err := response.Recv()
    if err == nil {
      for _, value := range resp.Transactions {
        for _, data := range value.Events {
          created := data.GetCreated()
          if created != nil {
            createChannel <- CreateEventWrapper {
                ContractID: created.ContractId,
                ContractKey: created.ContractKey,
                CreateArguments: created.CreateArguments,
                TemplateId: created.TemplateId,
                Witnesses: created.WitnessParties,
                Observers: created.Observers,
                Signatories: created.Signatories,
                Offset: value.Offset,
            }

          } else {
            archived := data.GetArchived()
            archiveChannel <- ArchivedEventWrapper {
              ContractId: archived.ContractId,
              Offset: value.Offset,
            }
          }
        }
      }
    }

    if err != nil {
      switch err.Error() {
        case "Aborted":
          ledgerContext.LogInfo(fmt.Sprintf("gRPC connection aborted, error message %s", err))
          ledgerContext.LogInfo("Cleaning up and reattempting connection")
          connection.connection.Close()
          close(dbCommitChannel)
          close(createChannel)
          close(archiveChannel)
          ledgerContext.WatchTransactionStream()
        default:
          panic(err)
      }
    }
  }
}
//func (ledgerContext *LedgerContext) WatchCommandService() {
//  connection := ledgerContext.GetConnectionWithoutTimeout()
//  defer connection.connection.Close()
//
//  var parties []string
//  ledgerId := ledgerContext.GetLedgerId()
//  ledgerContext.LedgerId = ledgerId
//  parties = ledgerContext.GetParties()
//  client := v1.NewCommandCompletionServiceClient(connection.connection)
//  response, err := client.CompletionStream(*connection.ctx, &v1.CompletionStreamRequest{
//    LedgerId: ledgerId,
//    Parties: parties,
//    ApplicationId: ledgerContext.ApplicationId,
//    Offset: &v1.LedgerOffset {
//      Value: &v1.LedgerOffset_Boundary {
//          Boundary: v1.LedgerOffset_LEDGER_BEGIN,
//      },
//    },
//  })
//
//  db := Database.InitializeDB()
//  if err != nil {
//    panic(err)
//  }
//  channel := make(chan CreateEventWrapper)
//  exerciseChannel := make(chan ExercisedEventWrapper)
//  dbCommitChannel := make(chan func()())
//  go func() {
//    for {
//      x := <-dbCommitChannel
//      x()
//    }
//  }()
//  go func() {
//    for {
//        select {
//          case x := <-channel:
//            log.Printf("Found Create with ContractID: %s", x.ContractID)
//            event := ledgerContext.GetEventByContractId(parties, x.ContractID)
//            dbCommitChannel <- func()() { db.Create(&Database.CreatesTable{ ContractID: x.ContractID, }) }
//            if event.GetArchiveEvent() == nil {
//                cKey, _ := protojson.Marshal(x.ContractKey)
//                createArgs, _ := protojson.Marshal(x.CreateArguments)
//                tid := x.TemplateId
//                fTid := fmt.Sprintf("%s:%s:%s", tid.PackageId, tid.ModuleName, tid.EntityName)
//                w, _ := json.Marshal(x.Witnesses)
//                o, _ := json.Marshal(x.Observers)
//                dbCommitChannel <- func()() {
//                  db.Create(&Database.ContractTable{
//                      CreateArguments: createArgs,
//                      ContractKey: cKey,
//                      ContractID: x.ContractID,
//                      Observers: o,
//                      Witnesses: w,
//                      TemplateFqn: fTid,
//                  })
//                }
//
//            } else {
//              log.Printf("ContractID: %s has been archived", x.ContractID)
//              dbCommitChannel <- func()() {
//                db.Create(&Database.ArchivesTable {
//                  ContractID: x.ContractID,
//                })
//              }
//            }
//          case exercised := <-exerciseChannel:
//                if exercised.Consuming {
//                  dbCommitChannel <- func()() {
//                    db.Where("contract_id = ?", exercised.ContractID).Delete(&Database.ContractTable{})
//                  }
//                }
//                log.Printf("Got event %s", exercised)
//        }
//        log.Printf("Waiting for next event")
//
//    }
//  }()
//  for {
//    resp, err := response.Recv()
//    if err == io.EOF {
//        return
//    } else if err == nil {
//      for _, value := range resp.Completions {
//          log.Printf("Dedup period: %s", value.DeduplicationPeriod)
//          if value.TransactionId == "" {
//            log.Printf("Failed to pull transaction %s, Reason: %s", value.TransactionId, value.Status)
//          } else {
//            transaction := ledgerContext.FetchFromTransactionServiceById(value.TransactionId, parties)
//            var t []string
//            ledgerContext.TraverseTransactionTreeEvents(parties, transaction, t, channel, exerciseChannel)
//          }
//      }
//    }
//
//    if err != nil {
//        panic(err)
//    }
//  }
//}

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

    if ledgerContext.LedgerId != "" {
      return(ledgerContext.LedgerId)
    }

    client := v1.NewLedgerIdentityServiceClient(connection.connection)
    response, err := client.GetLedgerIdentity(*connection.ctx, &v1.GetLedgerIdentityRequest{})
    if err != nil {
      log.Fatalf("Failed to get ledger identity")
    }
    ledgerContext.LogInfo(fmt.Sprintf("Ledger ID: %s", response.LedgerId))
    return response.LedgerId
}

func (ledgerContext *LedgerContext) GetParties() (allParties []string) {
  if ledgerContext.Sandbox {
    return(ledgerContext.GetPartiesSandbox())
  } else {
    return(ledgerContext.GetPartiesFromUser())
  }
}

func (ledgerContext *LedgerContext) GetPartiesFromUser() (allParties []string) {
  connection := ledgerContext.GetConnection()
  defer connection.cancelCtx()
  defer connection.connection.Close()

  client := admin.NewUserManagementServiceClient(connection.connection)
  response, err := client.ListUserRights(*connection.ctx, &admin.ListUserRightsRequest{
    UserId: ledgerContext.ApplicationId,
  })
  if err != nil {
    panic(err)
  }

  var readAs []string
  for _, v := range(response.Rights) {
    r := v.GetCanReadAs()
    if r != nil {
      readAs = append(readAs, r.Party)
    }
  }
  return readAs
}

func (ledgerContext *LedgerContext) GetPartiesSandbox() (allParties []string) {
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

func (ledgerContext *LedgerContext) GetActiveContractSet() (string) {
    connection := ledgerContext.GetConnectionWithoutTimeout()
    defer connection.connection.Close()

    db := ledgerContext.GetDatabaseConnection()

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
      Verbose: true,
    })
    if err != nil {
        log.Fatalf("%s", err)
    }
    for {
      resp, err := response.Recv()
      if err == io.EOF {
        panic("Didn't get offset, bailing")
      } else if err == nil {
        for _, value := range resp.ActiveContracts {
            log.Printf("Found Contract in ACS: %s", value.ContractId)
            db.FirstOrCreate(&Database.CreatesTable {
              ContractID: value.ContractId,
            }, &Database.CreatesTable { ContractID: value.ContractId })
            cKey := ledgerContext.ParseLedgerData(value.ContractKey)
            cKeyS, _ := json.Marshal(cKey)
            createArgs := ledgerContext.ParseLedgerData(&v1.Value {
              Sum: &v1.Value_Record {
                Record: value.CreateArguments,
              },
            })
            createArgsS, _ := json.Marshal(createArgs)
            tid := value.TemplateId
            fTid := fmt.Sprintf("%s:%s:%s", tid.PackageId, tid.ModuleName, tid.EntityName)
            w, _ := json.Marshal(value.WitnessParties)
            o, _ := json.Marshal(value.Observers)
            s, _ := json.Marshal(value.Signatories)
            db.FirstOrCreate(&Database.ContractTable {
              CreateArguments: createArgsS,
              ContractKey: cKeyS,
              ContractID: value.ContractId,
              Observers: o,
              Witnesses: w,
              Signatories: s,
              TemplateFqn: fTid,
              Offset: "replaceme", // We don't know the offset at this point in time until after the fact, we replace this later
           }, Database.ContractTable { ContractID: value.ContractId })
        }
        if resp.Offset != "" {
          log.Printf("Offset: %s", resp.Offset)
          db.Model(&Database.ContractTable{}).Where("offset = ?", "replaceme").Update("offset", resp.Offset)
          db.Create(&Database.LastOffset{
            Id: 1,
            Offset: resp.Offset,
          })
          return resp.Offset
        }
      }

      if err != nil {
          panic(err)
      }
    }
    return ""
}
