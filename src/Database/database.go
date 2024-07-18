package Database

import (
  "gorm.io/gorm"
  "gorm.io/driver/sqlite"
  "gorm.io/driver/postgres"
  "fmt"
  "log"
  //"gorm.io/gorm/logger"
  //"github.com/digital-asset/dazl-client/v7/go/api/com/daml/ledger/api/v1"
)


type LastOffset struct {
    Offset string `gorm:"index:offset_idx_offset,unique"`
    Id int32
}

func (LastOffset) TableName() string {
  return "__offset"
}

type ExercisedTable struct {
  EventId string `gorm:"index:exercised_event_idx,uniuqe"`
  ContractId string `gorm:"index:exercised_idx_offset,unique"`
  TemplateFqn string
  Choice string
  ChoiceArgument []byte `gorm:"type:jsonb"`
  ActingParties []byte `gorm:"type:jsonb"`
  Consuming bool
  ChildEventIds []byte `gorm:"type:jsonb"`
  WitnessParties []byte `gorm:"type:jsonb"`
  ExerciseResult []byte `gorm:"type:jsonb"`
}

func (ExercisedTable) TableName() string {
  return "__exercised"
}

type TransactionTable struct {
  TransactionId string `gorm:"index:transactions_idx,unique"`
  WorkflowId string
  CommandId string
  Offset string
  EventIds []byte `gorm:"type:jsonb"`
}

func (TransactionTable) TableName() string {
  return "__transactions"
}

type ArchivesTable struct {
    ContractID string `gorm:"index:archives_idx_contract_id,unique"`
    EventId string `gorm:"index:archives_event_idx,unique;column:event_id"`
    TemplateFqn string `gorm:"index:archives_template_fqn_idx"`
    Witnesses []byte `gorm:"type:jsonb"`
}

func (ArchivesTable) TableName() string {
  return "__archives"
}

type CreatesTable struct {
   ContractID string `gorm:"index:creates_idx_contract_id,unique"`
   ContractKey []byte `gorm:"type:jsonb"`
   CreateArguments []byte `gorm:"type:jsonb;column:payload"`
   TemplateFqn string `gorm:"index:contracts_idx_template_fqn"`
   Witnesses []byte `gorm:"type:jsonb"`
   Observers []byte `gorm:"type:jsonb"`
   Signatories []byte `gorm:"type:jsonb"`
   EventId string `gorm:"index:creates_event_id,unique;column:event_id"`
}

func (CreatesTable) TableName() string {
  return "__creates"
}

func MigrateTables(db *gorm.DB) () {
  db.AutoMigrate(&TransactionTable{})
  db.AutoMigrate(&ArchivesTable{})
  db.AutoMigrate(&CreatesTable{})

  hasOldOffset := db.Migrator().HasTable("last_offsets")
  if hasOldOffset {
    db.Migrator().RenameTable("last_offsets", "__offset")
  } else {
    db.AutoMigrate(&LastOffset{})
  }
}

func InitializeSQLiteDB(dbName string, gofast bool) (db *gorm.DB) {
  var config *gorm.Config
  config = &gorm.Config{
    SkipDefaultTransaction: gofast,
  }

  db, err := gorm.Open(sqlite.Open(dbName), config)
  if err != nil {
    panic("Failed to open DB")
  }
  MigrateTables(db)
  return db
}

func InitializePostgresDB(host string, user string, password string, dbname string, port int, sslmode string, gofast bool) (db *gorm.DB) {
  connStr := fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%d sslmode=%s", host, user, password, dbname, port, sslmode)
  var config *gorm.Config
  config = &gorm.Config{
    SkipDefaultTransaction: gofast,
  }

  log.Printf("Connecting to PSQL DB %s", dbname)
  db, err := gorm.Open(postgres.Open(connStr), config)
  if err != nil {
    panic(fmt.Sprintf("Failed to connect to db, error: %s", err))
  }
  MigrateTables(db)
  db.Exec(`
    CREATE OR REPLACE FUNCTION active(template_id text default null)
      RETURNS table(contract_id text, contract_key jsonb, payload jsonb, template_fqn text, witnesses jsonb, observers jsonb, signatories jsonb, event_id text, transaction_id text)
      BEGIN ATOMIC
        SELECT contract_id, contract_key, payload, template_fqn, witnesses, observers, signatories, event_id, transaction_id
            FROM __creates AS t INNER JOIN __transactions on (__transactions.event_ids ? t.event_id)
            WHERE contract_id NOT IN (SELECT contract_id FROM __archives)
            AND
              CASE
                WHEN template_id IS NOT NULL THEN template_fqn LIKE FORMAT('%%%s%%', template_id)
                ELSE true
              END;
      END;
  `)
  db.Exec(`
    CREATE OR REPLACE FUNCTION lookup_contract(contract text)
      RETURNS table(contract_id text, contract_key jsonb, payload jsonb, template_fqn text, witnesses jsonb, observers jsonb, signatories jsonb, event_id text, transaction_id text)
      BEGIN ATOMIC
       SELECT __creates.*, transaction_id FROM __transactions AS t INNER JOIN __creates ON t.event_ids ? __creates.event_id WHERE contract_id = contract;
      END;
  `)
  db.Exec(`
    CREATE OR REPLACE FUNCTION offset_exists(n text)
      RETURNS table(ledger_offset text)
      BEGIN ATOMIC
        select __transactions.offset FROM __transactions WHERE __transactions.offset = n;
      END;
  `)
  //db.Exec("CREATE OR REPLACE FUNCTION lookup_contract(n text) RETURNS SETOF public.__contracts BEGIN ATOMIC SELECT * FROM __contracts WHERE contract_id = n; END;")
  //db.Exec("CREATE OR REPLACE FUNCTION offset_exists(n text) RETURNS SETOF public.__contracts BEGIN ATOMIC SELECT * FROM __contracts WHERE offset = n; END;")
  //db.Exec("CREATE OR REPLACE FUNCTION partial_template(n text) RETURNS SETOF public.__contracts BEGIN ATOMIC SELECT * FROM active() WHERE template_fqn LIKE n; END;")
  return db
}
