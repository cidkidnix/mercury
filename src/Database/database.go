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


type Offsets struct {
    OffsetIx uint64 `gorm:"index:offset_offsets_idx_int,unique"`
    Offset string `gorm:"index:offset_offsets_idx,unique"`
    TransactionId string `grom:"index:offset_offsets_transaction_idx,unique"`
}

func (Offsets) TableName() string {
  return "__offsets"
}

type EventsTable struct {
  EventId string `gorm:"index:events_event_id,unique"`
  Offset string
  OffsetIx uint64
}

func (EventsTable) TableName() string {
  return "__events"
}

type ExercisedTable struct {
  EventId string `gorm:"index:exercised_event_idx,unique;primaryKey"`
  ContractId string `gorm:"index:exercised_event_contractid_idx"`
  TemplateFqn string `gorm:"index:exercised_event_template_idx"`
  Choice string `gorm:"index:exercised_event_choice_idx"`
  ChoiceArgument []byte `gorm:"type:jsonb"`
  ActingParties []byte `gorm:"type:jsonb"`
  Consuming bool `gorm:"exercised_event_consuming"`
  ChildEventIds []byte `gorm:"type:jsonb"`
  Witnesses []byte `gorm:"type:jsonb"`
  ExerciseResult []byte `gorm:"type:jsonb"`
}

func (ExercisedTable) TableName() string {
  return "__exercised"
}

type TransactionTable struct {
  TransactionId string `gorm:"index:transactions_idx,unique;primaryKey"`
  WorkflowId string
  CommandId string `gorm:"index_transactions_idx_command_id"`
  Offset string `gorm:"index:transactions_idx_offset,unique"` // offsets are unique
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
   EventId string `gorm:"index:creates_event_id,unique;column:event_id`
}

func (CreatesTable) TableName() string {
  return "__creates"
}

type ConsumingTable struct {
  ContractId string `gorm:"index:creates_idx_contract_id"`
}

func (ConsumingTable) TableName() string {
  return "__consuming"
}

func MigrateTables(db *gorm.DB) () {
  db.AutoMigrate(&TransactionTable{})
  db.AutoMigrate(&ArchivesTable{})
  db.AutoMigrate(&CreatesTable{})
  db.AutoMigrate(&Offsets{})
  db.AutoMigrate(&EventsTable{})
  db.AutoMigrate(&ExercisedTable{})
  db.AutoMigrate(&ConsumingTable{})
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
    CREATE OR REPLACE FUNCTION active(template_id text default null, offset_end text default null)
      RETURNS TABLE(contract_id text, contract_key jsonb, payload jsonb, template_fqn text, witnesses jsonb, observers jsonb, signatories jsonb, event_id text, transaction_id text, "offset_ix" bigint)
      LANGUAGE plpgsql VOLATILE
      AS $BODY$
        BEGIN
          RETURN QUERY
            SELECT __creates.contract_id as contract_id, __creates.contract_key, __creates.payload, __creates.template_fqn, __creates.witnesses, __creates.observers, __creates.signatories, e.event_id, __transactions.transaction_id, e."offset_ix"
                FROM __events AS e
                  RIGHT JOIN __creates ON __creates.event_id = e.event_id
                  INNER JOIN __transactions ON __transactions."offset" = e."offset"
                WHERE NOT EXISTS (SELECT __exercised.contract_id FROM __exercised WHERE __exercised.contract_id = __creates.contract_id AND __exercised.consuming)
                AND
                  CASE
                    WHEN template_id IS NOT NULL THEN __creates.template_fqn LIKE FORMAT('%%%s%%', template_id)
                    ELSE true
                  END
                AND
                  CASE
                    WHEN offset_end IS NOT NULL THEN e.offset_ix BETWEEN 0 AND (SELECT __offsets.offset_ix FROM __offsets WHERE __offsets."offset" = offset_end)
                    ELSE true
                  END;
            RETURN;
        END;
     $BODY$;
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
  db.Exec(`
    CREATE INDEX IF NOT EXISTS event_ids_jsonb ON __transactions USING gin(event_ids);
  `)
  db.Exec(`
    CREATE INDEX IF NOT EXISTS payload_creates ON __creates USING gin(payload);
  `)
  //db.Exec("CREATE OR REPLACE FUNCTION lookup_contract(n text) RETURNS SETOF public.__contracts BEGIN ATOMIC SELECT * FROM __contracts WHERE contract_id = n; END;")
  //db.Exec("CREATE OR REPLACE FUNCTION offset_exists(n text) RETURNS SETOF public.__contracts BEGIN ATOMIC SELECT * FROM __contracts WHERE offset = n; END;")
  //db.Exec("CREATE OR REPLACE FUNCTION partial_template(n text) RETURNS SETOF public.__contracts BEGIN ATOMIC SELECT * FROM active() WHERE template_fqn LIKE n; END;")
  return db
}
