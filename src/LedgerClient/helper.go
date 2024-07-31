package LedgerClient

import (
    "log"
    "io"
    "time"
    "google.golang.org/grpc"
    "google.golang.org/grpc/status"
    "google.golang.org/grpc/codes"
    "google.golang.org/protobuf/encoding/protojson"
    "github.com/digital-asset/dazl-client/v7/go/api/com/daml/ledger/api/v1"
    "github.com/digital-asset/dazl-client/v7/go/api/com/daml/ledger/api/v1/admin"
    "context"
    //"slices"
    "mercury/src/Database"
    "mercury/src/GRPCClient"
    "mercury/src/Config"
    "encoding/json"
    "fmt"
    "gorm.io/gorm"
    "reflect"
    "strconv"
)

type DatabaseConnection struct {
  Host *string
  User *string
  Password *string
  Dbname *string
  Port *int
  Sslmode *string
}

type TransactionWrapper struct {
  TransactionId string
  CommandId string
  WorkflowId string
  Offset string
  EffectiveAt time.Time
  EventIds []string
}

type ArchivedEventWrapper struct {
  ContractId string
  EventId string
  TemplateId *v1.Identifier
  Witnesses []string
}

type CreateEventWrapper struct {
  ContractKey *v1.Value
  CreateArguments *v1.Record
  ContractID string
  Witnesses []string
  Observers []string
  Signatories []string
  TemplateId *v1.Identifier
  EventId string
}

type ExercisedEventWrapper struct {
  EventId string
  ContractID string
  TemplateId *v1.Identifier
  Choice string
  ChoiceArgument *v1.Value
  ActingParties []string
  Consuming bool
  Witnesses []string
  ChildEventIds []string
  ExerciseResult *v1.Value
}

type Retry struct {
  Count int
  Limit int
}

type LedgerContext struct {
  GetConnection func()(ConnectionWrapper)
  GetConnectionWithoutTimeout func()(ConnectionWrapper)
  Sandbox bool
  ApplicationId string
  LedgerId string
  StartPoint string
  DB *gorm.DB
  Retry *Retry
  ConfigPath string
  LogLevel string
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

func IntializeGRPCConnection(connStr string, authToken *string, sandbox *bool, applicationId *string, startPoint *string, configPath string) (ledgerContext LedgerContext) {
  var sPoint *string
  var appId *string
  if startPoint == nil || *startPoint == "" {
    sPoint = Config.GetConfig(configPath).Ledger.StartPoint
  } else {
    sPoint = startPoint
  }
  if applicationId == nil || *applicationId == "nil" {
    appId = Config.GetConfig(configPath).Ledger.ApplicationID
  } else {
    appId = applicationId
  }
  return LedgerContext{
      GetConnectionWithoutTimeout: func() (ConnectionWrapper) {
        ctx := context.Background()
        conn, err := grpc.DialContext(ctx, connStr, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithUnaryInterceptor(GRPCClient.GenTokenUnaryInterceptor(*authToken)), grpc.WithStreamInterceptor(GRPCClient.GenTokenStreamInterceptor(*authToken)), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(1024 * 1024 * 40)))
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
        conn, err := grpc.DialContext(ctx, connStr, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithUnaryInterceptor(GRPCClient.GenTokenUnaryInterceptor(*authToken)), grpc.WithStreamInterceptor(GRPCClient.GenTokenStreamInterceptor(*authToken)), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(1024 * 1024 * 40)))
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
      ApplicationId: *appId,
      LedgerId: "",
      StartPoint: *sPoint,
      ConfigPath: configPath,
      DB: nil,
      LogLevel: Config.GetConfig(configPath).LogLevel,
      Retry: &Retry {
        Count: 0,
        Limit: Config.GetConfig(configPath).Ledger.GRPCOptions.MaxRetries,
      },
  }
}

func (ledgerContext *LedgerContext) LogTime(name string, f func()()) {
  initial := time.Now()
  f()
  end := time.Since(initial)
  log.Printf(fmt.Sprintf("\033[0;36m[TIME]\033[0m %v: Time to complete %v", name, end))
}

func (ledgerContext *LedgerContext) LogInfo(message string) {
  log.Printf("\033[0;35m[INFO]\033[0m %s", message)
}

func (ledgerContext *LedgerContext) LogDebug(message string) {
  if ledgerContext.LogLevel == "DEBUG" {
    log.Printf("\033[0;34m[DEBUG]\033[0m %s", message)
  }
}

func (ledgerContext *LedgerContext) LogContract(message string) {
  if ledgerContext.LogLevel == "DEBUG" || ledgerContext.LogLevel == "CONTRACT" {
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

//func (ledgerContext *LedgerContext) TraverseTransactionTreeEvents(parties []string, tree *v1.TransactionTree, ignoreList []string, channel chan CreateEventWrapper, channelExercised chan ExercisedEventWrapper) () {
//   log.Printf("Offset: %s", tree.Offset)
//   log.Printf("TransactionTree Roots: %s", tree.RootEventIds)
//   for _, v := range (tree.EventsById) {
//     data := v.GetCreated()
//     if data == nil {
//        event := v.GetExercised()
//        channelExercised <- ExercisedEventWrapper{
//          ContractID: event.ContractId,
//          Consuming: event.Consuming,
//        }
//        //log.Printf("CHILD EVENT IDS: %s", event.ChildEventIds)
//        for _, e := range (event.ChildEventIds) {
//          log.Printf("Found ExercisedEvent %s", event.EventId)
//          if slices.Contains(ignoreList, event.EventId) {
//            log.Printf("TraverseTransactionTreeEvents: Already Seen %s, Bailing out of recurse", event.EventId)
//          } else {
//            ignoreList := append(ignoreList, event.EventId)
//            tree := ledgerContext.FetchFromTransactionServiceByEventId(e, parties)
//            ledgerContext.TraverseTransactionTreeEvents(parties, tree, ignoreList, channel, channelExercised)
//            log.Printf("Done with traversal for event %s", event.EventId)
//          }
//        }
//      } else {
//        log.Printf("CreatedEvent: %s", data.EventId)
//        channel <- CreateEventWrapper {
//            ContractID: data.ContractId,
//            ContractKey: data.ContractKey,
//            CreateArguments: data.CreateArguments,
//            TemplateId: data.TemplateId,
//            Witnesses: data.WitnessParties,
//            Observers: data.Observers,
//        }
//      }
//   }
//}

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

func (ledgerContext *LedgerContext) FetchFromTransactionServiceByEventId(eventId string) (transaction *v1.TransactionTree) {
  connection := ledgerContext.GetConnection()
  defer connection.connection.Close()
  defer connection.cancelCtx()

  parties := ledgerContext.GetParties()
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

  configs := Config.GetConfig(ledgerContext.ConfigPath)

  var db *gorm.DB
  if configs.Ledger.GetPostgres() != nil {
    x := configs.Ledger.GetPostgres()
    db = Database.InitializePostgresDB(x.Host, x.User, x.Password, x.Dbname, x.Port, x.Sslmode, configs.Experimental.IMustGoFast)
  } else {
    config := configs.Ledger.GetSQLite()
    db = Database.InitializeSQLiteDB(config.FileName, configs.Experimental.IMustGoFast)
  }
  sqlDb, err := db.DB()
  if err != nil {
    panic(err)
  }
  sqlDb.SetMaxIdleConns(10)
  sqlDb.SetMaxOpenConns(50)
  sqlDb.SetConnMaxLifetime(time.Hour)
  ledgerContext.DB = db
  return(db)
}

func (ledgerContext *LedgerContext) GetStartPoint() *v1.LedgerOffset {
  switch (ledgerContext.StartPoint) {
    case "LEDGER_BEGIN":
      // We may restart and call this again, make sure to not start from GENESIS twice
      ledgerContext.StartPoint = "OLDEST"
      return (&v1.LedgerOffset {
        Value: &v1.LedgerOffset_Boundary {
          Boundary: v1.LedgerOffset_LEDGER_BEGIN,
        },
      })
    case "OLDEST":
      db := ledgerContext.GetDatabaseConnection()
      var lOffset Database.Offsets
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
      var lOffset Database.Offsets
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
        // Decode the GenMap into a regular Map if our keys are hashable and are not empty
        switch val := ledgerContext.ParseLedgerData(v.Key).(type) {
            case (string):
              tMap[val] = ledgerContext.ParseLedgerData(v.Value)
            case (int):
              tMap[strconv.Itoa(val)] = ledgerContext.ParseLedgerData(v.Value)
            case (int64):
              tMap[strconv.Itoa(int(val))] = ledgerContext.ParseLedgerData(v.Value)
            case (int32):
              tMap[strconv.Itoa(int(val))] = ledgerContext.ParseLedgerData(v.Value)
            default:
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

func (ledgerContext *LedgerContext) RunInGoRoutine(f func()()) {
  configs := Config.GetConfig(ledgerContext.ConfigPath)
  if configs.Experimental.DatabaseGoFuncs {
    go func() {
      f()
    }()
  } else {
    f()
  }
}

func (ledgerContext *LedgerContext) worker(id int,  jobs <- chan(func()())) {
  for j := range jobs {
    ledgerContext.LogTime(fmt.Sprintf("Worker %d", id), func()() {
      j()
    })
  }
}

func (ledgerContext *LedgerContext) WatchTransactionTreeStream() {
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

  response, err := client.GetTransactionTrees(*connection.ctx, &v1.GetTransactionsRequest {
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

  exercisedChannel := make(chan ExercisedEventWrapper)
  createChannel := make(chan CreateEventWrapper)
  transactionChannel := make(chan TransactionWrapper)

  workerConnection := make(chan func()())

  for w := 0; w <= 30; w++ {
    go ledgerContext.worker(w, workerConnection)
  }

  go func() {
    for {
        select {
          case t, more := <-transactionChannel:
            if !more {
              return
            }
            ledgerContext.LogTime("decode_and_write_transaction", func()() {
              ledgerContext.LogDebug(fmt.Sprintf("Got Transaction: %s", t.TransactionId))
              workerConnection <- func()() {
                db.Create(&Database.TransactionTable {
                  TransactionId: t.TransactionId,
                  WorkflowId: t.WorkflowId,
                  CommandId: t.CommandId,
                  Offset: t.Offset,
                  EventIds: t.EventIds,
                })
              }

              workerConnection <- func()() {
                ixOffset, err := strconv.ParseUint(t.Offset, 16, 64)
                if err != nil {
                  panic(err)
                }
                for _, v := range(t.EventIds) {
                  db.Create(&Database.EventsTable {
                    EventId: v,
                    Offset: t.Offset,
                    OffsetIx: ixOffset,
                  })
                }
              }

              workerConnection <- func()() {
                ixOffset, err := strconv.ParseUint(t.Offset, 16, 64)
                if err != nil {
                  fmt.Printf("%s", t.Offset)
                  panic("Failed to decode offset into integer")
                }
                log.Printf("%s", t.EffectiveAt)
                db.Create(&Database.Offsets {
                  OffsetIx: ixOffset,
                  Offset: t.Offset,
                  TransactionId: t.TransactionId,
                  EffectiveAt: t.EffectiveAt,
                })
              }
            })
          case x, more := <-createChannel:
            if !more {
              return
            }
            ledgerContext.LogTime("decode_and_write_create", func()() {
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
              workerConnection <- func()() {
                db.Create(&Database.CreatesTable{
                    ContractID: x.ContractID,
                    ContractKey: cKeyS,
                    CreateArguments: createArgsS,
                    Observers: x.Observers,
                    Witnesses: x.Witnesses,
                    Signatories: x.Signatories,
                    TemplateFqn: fTid,
                    EventId: x.EventId,
                })
              }
            })
          case y, more := <-exercisedChannel:
            if !more {
              return
            }
            ledgerContext.LogTime("decode_and_write_exercised", func()() {
              ledgerContext.LogContract(fmt.Sprintf("Exercise ContractID: %s", y.ContractID))
              tid := y.TemplateId
              fTid := fmt.Sprintf("%s:%s:%s", tid.PackageId, tid.ModuleName, tid.EntityName)

              cArg := ledgerContext.ParseLedgerData(y.ChoiceArgument)
              cArgS, _ := json.Marshal(cArg)

              eResult := ledgerContext.ParseLedgerData(y.ExerciseResult)
              result, _ := json.Marshal(eResult)

              if y.Consuming {
                workerConnection <- func()() {
                  db.Create(&Database.ConsumingTable {
                    ContractId: y.ContractID,
                  })
                }
              }
              workerConnection <- func()() {
                db.Create(&Database.ExercisedTable {
                    EventId: y.EventId,
                    ContractId: y.ContractID,
                    TemplateFqn: fTid,
                    Choice: y.Choice,
                    ChoiceArgument: cArgS,
                    ActingParties: y.ActingParties,
                    Consuming: y.Consuming,
                    Witnesses: y.Witnesses,
                    ChildEventIds: y.ChildEventIds,
                    ExerciseResult: result,
                })
              }
            })
        }
    }
  }()

  for {
    resp, err := response.Recv()
    if err == nil {
      for _, value := range resp.Transactions {
        var eIds []string
        for eventId, data := range value.EventsById {
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
                EventId: created.EventId,
            }
            eIds = append(eIds, eventId)
          } else {
            exercised := data.GetExercised()
            exercisedChannel <- ExercisedEventWrapper {
              EventId: exercised.EventId,
              ContractID: exercised.ContractId,
              TemplateId: exercised.TemplateId,
              Choice: exercised.Choice,
              ChoiceArgument: exercised.ChoiceArgument,
              ActingParties: exercised.ActingParties,
              Consuming: exercised.Consuming,
              Witnesses: exercised.WitnessParties,
              ChildEventIds: exercised.ChildEventIds,
              ExerciseResult: exercised.ExerciseResult,
            }
            eIds = append(eIds, eventId)
          }
        }
        transactionChannel <- TransactionWrapper {
          TransactionId: value.TransactionId,
          CommandId: value.CommandId,
          WorkflowId: value.WorkflowId,
          Offset: value.Offset,
          EffectiveAt: value.EffectiveAt.AsTime(),
          EventIds: eIds,
        }
      }
    }

    if err != nil {
      switch status.Code(err) {
        case codes.Aborted:
          if ledgerContext.Retry.Limit == ledgerContext.Retry.Count {
            log.Fatalf("Hit retry limit")
          }
          ledgerContext.LogInfo(fmt.Sprintf("gRPC connection aborted, error message %s", err))
          ledgerContext.LogInfo("Cleaning up and reattempting connection")
          connection.connection.Close()
          close(createChannel)
          close(exercisedChannel)
          close(workerConnection)
          ledgerContext.Retry.Count = ledgerContext.Retry.Count + 1
          ledgerContext.WatchTransactionStream()
        default:
          log.Fatalf("Got unrecoverable gRPC error %s", err)
      }
    }
  }
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

  log.Printf("PartyMAP: %s", partyMap)
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
  transactionChannel := make(chan TransactionWrapper)

  workerConnection := make(chan func()())

  for w := 0; w <= 30; w++ {
    go ledgerContext.worker(w, workerConnection)
  }

  go func() {
    for {
        select {
          case t, more := <-transactionChannel:
            if !more {
              return
            }
            ledgerContext.LogTime("decode_and_write_transaction", func()() {
              ledgerContext.LogDebug(fmt.Sprintf("Got Transaction: %s", t.TransactionId))
              eIds, _ := json.Marshal(t.EventIds)
              log.Printf("IDS: %s", t.EventIds)
              log.Printf("JSON IDS: %s", eIds)
              workerConnection <- func()() {
                db.Create(&Database.TransactionTable {
                  TransactionId: t.TransactionId,
                  WorkflowId: t.WorkflowId,
                  CommandId: t.CommandId,
                  Offset: t.Offset,
                  EventIds: t.EventIds,
                })
              }

              workerConnection <- func()() {
                ixOffset, err := strconv.ParseUint(t.Offset, 16, 64)
                if err != nil {
                  panic(err)
                }
                for _, v := range(t.EventIds) {
                  db.Create(&Database.EventsTable {
                    EventId: v,
                    Offset: t.Offset,
                    OffsetIx: ixOffset,
                  })
                }
              }

              workerConnection <- func()() {
                ixOffset, err := strconv.ParseUint(t.Offset, 16, 64)
                if err != nil {
                  fmt.Printf("%s", t.Offset)
                  panic("Failed to decode offset into integer")
                }
                db.Create(&Database.Offsets {
                  OffsetIx: ixOffset,
                  Offset: t.Offset,
                  TransactionId: t.TransactionId,
                })
              }
            })
          case x, more := <-createChannel:
            if !more {
              return
            }
            ledgerContext.LogTime("decode_and_write_create", func()() {
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
              workerConnection <- func()() {
                db.Create(&Database.CreatesTable{
                    ContractID: x.ContractID,
                    ContractKey: cKeyS,
                    CreateArguments: createArgsS,
                    Observers: x.Observers,
                    Witnesses: x.Witnesses,
                    Signatories: x.Signatories,
                    TemplateFqn: fTid,
                    EventId: x.EventId,
                })
              }
            })
          case y, more := <-archiveChannel:
            if !more {
              return
            }
            ledgerContext.LogTime("decode_and_write_archive", func()() {
              ledgerContext.LogContract(fmt.Sprintf("Archive ContractID: %s", y.ContractId))
              tid := y.TemplateId
              fTid := fmt.Sprintf("%s:%s:%s", tid.PackageId, tid.ModuleName, tid.EntityName)
              workerConnection <- func()() {
                db.Create(&Database.ArchivesTable {
                    ContractID: y.ContractId,
                    EventId: y.EventId,
                    TemplateFqn: fTid,
                    Witnesses: y.Witnesses,
                })
              }
            })
        }
    }
  }()
  for {
    resp, err := response.Recv()
    tP, err := protojson.Marshal(resp)
    log.Printf("%s", tP)
    if err == nil {
      for _, value := range resp.Transactions {
        var eIds []string
        for _, data := range value.Events {
          created := data.GetCreated()
          log.Printf("CREATED: %s", created)
          if created != nil {
            createChannel <- CreateEventWrapper {
                ContractID: created.ContractId,
                ContractKey: created.ContractKey,
                CreateArguments: created.CreateArguments,
                TemplateId: created.TemplateId,
                Witnesses: created.WitnessParties,
                Observers: created.Observers,
                Signatories: created.Signatories,
                EventId: created.EventId,
            }
            eIds = append(eIds, created.EventId)
            log.Printf("%s Created Event ID: %s", value.TransactionId, created.EventId)
          } else {
            archived := data.GetArchived()
            archiveChannel <- ArchivedEventWrapper {
              ContractId: archived.ContractId,
              EventId: archived.EventId,
              TemplateId: archived.TemplateId,
              Witnesses: archived.WitnessParties,
            }
            log.Printf("%s Archived Event ID: %s", value.TransactionId, archived.EventId)
            eIds = append(eIds, archived.EventId)
          }
        }
        log.Printf("PRE IDS: %s", eIds)
        transactionChannel <- TransactionWrapper {
          TransactionId: value.TransactionId,
          CommandId: value.CommandId,
          WorkflowId: value.WorkflowId,
          Offset: value.Offset,
          EventIds: eIds,
        }
      }
    }

    if err != nil {
      switch status.Code(err) {
        case codes.Aborted:
          if ledgerContext.Retry.Limit == ledgerContext.Retry.Count {
            log.Fatalf("Hit retry limit")
          }
          ledgerContext.LogInfo(fmt.Sprintf("gRPC connection aborted, error message %s", err))
          ledgerContext.LogInfo("Cleaning up and reattempting connection")
          connection.connection.Close()
          close(createChannel)
          close(archiveChannel)
          close(workerConnection)
          ledgerContext.Retry.Count = ledgerContext.Retry.Count + 1
          ledgerContext.WatchTransactionStream()
        default:
          log.Fatalf("Got unrecoverable gRPC error %s", err)
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
  parties := make(map[string]interface {})
  for _, v := range(response.Rights) {
    r := v.GetCanReadAs()
    a := v.GetCanActAs()
    if r != nil {
      parties[r.Party] = nil
    }
    if a != nil {
      parties[a.Party] = nil
    }
  }
  for k, _ := range(parties) {
    readAs = append(readAs, k)
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
           ledgerContext.LogContract(fmt.Sprintf("ACS Contract: %s", value.ContractId))
           ledgerContext.LogTime("acs_query", func()() {
             transactionTree := ledgerContext.FetchFromTransactionServiceByEventId(value.EventId)
             var eIds []string
             for eventId, event := range(transactionTree.EventsById) {
               eIds = append(eIds, eventId)
               switch event.Kind.(type) {
                 case (*v1.TreeEvent_Created):
                   created := event.GetCreated()
                   cKey := ledgerContext.ParseLedgerData(created.ContractKey)
                   cKeyS, _ := json.Marshal(cKey)
                   createArgs := ledgerContext.ParseLedgerData(&v1.Value {
                     Sum: &v1.Value_Record {
                       Record: created.CreateArguments,
                     },
                   })
                   createArgsS, _ := json.Marshal(createArgs)
                   tid := created.TemplateId
                   fTid := fmt.Sprintf("%s:%s:%s", tid.PackageId, tid.ModuleName, tid.EntityName)
                   db.FirstOrCreate(&Database.CreatesTable {
                     CreateArguments: createArgsS,
                     ContractKey: cKeyS,
                     ContractID: created.ContractId,
                     Observers: created.Observers,
                     Witnesses: created.WitnessParties,
                     Signatories: created.Signatories,
                     TemplateFqn: fTid,
                     EventId: created.EventId,
                   }, Database.CreatesTable { ContractID: created.ContractId })
                 case (*v1.TreeEvent_Exercised):
                   exercised := event.GetExercised()
                   tid := exercised.TemplateId
                   fTid := fmt.Sprintf("%s:%s:%s", tid.PackageId, tid.ModuleName, tid.EntityName)
                   if exercised.Choice == "Archive" {
                     db.FirstOrCreate(&Database.ArchivesTable {
                        ContractID: exercised.ContractId,
                        EventId: exercised.EventId,
                        TemplateFqn: fTid,
                        Witnesses: exercised.WitnessParties,
                     }, Database.ArchivesTable { ContractID: exercised.ContractId })
                   } else {
                     ledgerContext.LogDebug(fmt.Sprintf("Event %s is not an archive, skipping", exercised.EventId))
                   }
               }

             }
             db.FirstOrCreate(&Database.TransactionTable {
               TransactionId: transactionTree.TransactionId,
               WorkflowId: transactionTree.WorkflowId,
               CommandId: transactionTree.CommandId,
               Offset: transactionTree.Offset,
               EventIds: eIds,
             })

             ixOffset, err := strconv.ParseUint(transactionTree.Offset, 16, 64)
             if err != nil {
               panic("Failed to convert offset to integer")
             }
             db.Create(&Database.Offsets {
               OffsetIx: ixOffset,
               Offset: transactionTree.Offset,
               TransactionId: transactionTree.TransactionId,
             })
          })
        }
        if resp.Offset != "" {
          ledgerContext.LogInfo(fmt.Sprintf("Offset: %s", resp.Offset))
          return resp.Offset
        }
      }

      if err != nil {
          panic(err)
      }
    }
    return ""
}
