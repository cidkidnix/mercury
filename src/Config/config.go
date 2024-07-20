package Config

import (
  "encoding/json"
  "os"
  "bytes"
)

type isDatabase_Sum interface {
  isDatabase_Sum()
}

func (*PostgresDatabase) isDatabase_Sum() {}
func (*SqliteDatabase) isDatabase_Sum() {}

func (d *Ledger) GetPostgres() (*PostgresDatabase) {
  if d, ok := d.Database.(*PostgresDatabase); ok {
    return d
  }
  return nil
}

func (d *Ledger) GetSQLite() (*SqliteDatabase) {
  if d, ok := d.Database.(*SqliteDatabase); ok {
    return d
  }
  return nil
}

func (d *Ledger) UnmarshalJSON(data []byte) error {
   var raw struct {
     StartPoint *string `json:"startpoint"`
     ApplicationID *string `json:"applicationid"`
     GRPCOptions *GRPCOptions `json:"grpc"`
     Database json.RawMessage `json:"database"`
   }

   if err := json.Unmarshal(data, &raw); err != nil {
     panic(err)
     return err
   }
   //fmt.Printf("%s", sqlite)
   var obj struct {
     SqliteDatabase json.RawMessage `json:"sqlite"`
     PostgresDatabase json.RawMessage `json:"postgres"`
   }
   if err := json.Unmarshal(raw.Database, &obj); err != nil {
     panic(err)
   }
   var sqlite SqliteDatabase
   var postgres PostgresDatabase

   if err := json.Unmarshal(obj.SqliteDatabase, &sqlite); err == nil {
    *d = Ledger {
       Database: &sqlite,
       StartPoint: raw.StartPoint,
       ApplicationID: raw.ApplicationID,
       GRPCOptions: raw.GRPCOptions,
    }
    return nil
   }

   if err := json.Unmarshal(obj.PostgresDatabase, &postgres); err == nil {
     *d = Ledger {
         Database: &postgres,
         StartPoint: raw.StartPoint,
         ApplicationID: raw.ApplicationID,
         GRPCOptions: raw.GRPCOptions,
     }
     return nil
   }

   panic("Failed to decode")
   return json.Unmarshal(data, &raw)
}


type PostgresDatabase struct {
  Host string `json:"host,omitempty"`
  User string `json:"user,omitempty"`
  Port int `json:"port,omitempty"`
  Password string `json:"password,omitempty"`
  Dbname string `json:"database,omitempty"`
  Sslmode string `json:"sslmode,omitempty"`
}

type SqliteDatabase struct {
  FileName string `json:"filename,omitempty"`
}

type Database struct {
  Postgres *PostgresDatabase `json:"postgres,omitempty"`
  Sqlite *SqliteDatabase `json:"sqlite,omitempty"`
}

type GRPCOptions struct {
  MaxRetries int `json:"maxretries,omitempty"`
}

type Ledger struct {
  Database isDatabase_Sum `json:"database"`
  StartPoint *string `json:"startpoint,omitempty"`
  ApplicationID *string `json:"applicationid,omitempty"`
  GRPCOptions *GRPCOptions `json:"grpc,omitempty"`
}

type Experimental struct {
  IMustGoFast bool `json:"disable_db_transaction,omitempty"`
  DatabaseGoFuncs bool `json:"db_multithread,omitempty"`
}

type Config struct {
  Ledger Ledger `json:"ledger"`
  Experimental Experimental `json:"experimental,omitempty"`
  LogLevel string `json:"loglevel"`
}

func GetConfig(configPath string) (*Config) {
  config, err := os.ReadFile(configPath)
  if err != nil {
    panic(err)
  }
  dec := json.NewDecoder(bytes.NewReader(config))
  dec.DisallowUnknownFields()

  var configS Config
  if err := dec.Decode(&configS); err != nil {
   panic(err)
  }
  return &configS
}
