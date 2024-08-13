package Database

import (
  "gorm.io/gorm"
  "gorm.io/driver/sqlite"
  "gorm.io/driver/postgres"
  "fmt"
  "log"
  pq "github.com/lib/pq"
  "time"
  "gorm.io/gorm/logger"
  //"github.com/digital-asset/dazl-client/v7/go/api/com/daml/ledger/api/v1"
)


type Offsets struct {
    OffsetIx uint64 `gorm:"index:offset_offsets_idx_int,unique"`
    Offset string `gorm:"index:offset_offsets_idx,unique"`
    EffectiveAt time.Time `gorm:"type:timestamp with time zone"`
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
  ActingParties pq.StringArray `gorm:"type:text[]"`
  Consuming bool `gorm:"exercised_event_consuming"`
  ChildEventIds pq.StringArray `gorm:"type:text[]"`
  Witnesses pq.StringArray `gorm:"type:text[]"`
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
  EventIds pq.StringArray `gorm:"type:text[]"`
}

func (TransactionTable) TableName() string {
  return "__transactions"
}

type ArchivesTable struct {
    ContractID string `gorm:"index:archives_idx_contract_id,unique"`
    EventId string `gorm:"index:archives_event_idx,unique;column:event_id"`
    TemplateFqn string `gorm:"index:archives_template_fqn_idx"`
    Witnesses pq.StringArray `gorm:"type:text[]"`
}

func (ArchivesTable) TableName() string {
  return "__archives"
}

type CreatesTable struct {
   ContractID string `gorm:"index:creates_idx_contract_id,unique"`
   ContractKey []byte `gorm:"type:jsonb"`
   CreateArguments []byte `gorm:"type:jsonb;column:payload"`
   TemplateFqn string `gorm:"index:contracts_idx_template_fqn"`
   Witnesses pq.StringArray `gorm:"type:text[]"`
   Observers pq.StringArray `gorm:"type:text[]"`
   Signatories pq.StringArray `gorm:"type:text[]"`
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
    Logger: logger.Default.LogMode(logger.Silent),
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
    Logger: logger.Default.LogMode(logger.Silent),
  }

  log.Printf("Connecting to PSQL DB %s", dbname)
  db, err := gorm.Open(postgres.Open(connStr), config)
  if err != nil {
    panic(fmt.Sprintf("Failed to connect to db, error: %s", err))
  }
  MigrateTables(db)

  // Table View for exercises and related data
  db.Exec(`
    CREATE OR REPLACE VIEW exercised AS
      SELECT __exercised.*, e.offset, e.offset_ix, __offsets.effective_at, __transactions.command_id, __transactions.workflow_id, __transactions.transaction_id
      FROM __events as e
        INNER JOIN __exercised ON __exercised.event_id = e.event_id
        INNER JOIN __transactions ON __transactions."offset" = e."offset"
        INNER JOIN __offsets ON __offsets.offset_ix = e.offset_ix
  `)

  // Table View for creates ("contracts") and related data
  db.Exec(`
    CREATE OR REPLACE VIEW contract AS
      SELECT __creates.*,  e."offset", e.offset_ix, __offsets.effective_at, __transactions.command_id, __transactions.workflow_id, __transactions.transaction_id
      FROM __events as e
        INNER JOIN __creates ON __creates.event_id = e.event_id
        INNER JOIN __transactions ON __transactions."offset" = e."offset"
        INNER JOIN __offsets ON __offsets.offset_ix = e.offset_ix
  `)

  // Table View for all active contracts
  db.Exec(`
    CREATE OR REPLACE VIEW active AS
      SELECT __creates.contract_id, __creates.contract_key, __creates.payload, __creates.template_fqn, __creates.witnesses, __creates.observers, __creates.signatories, e.event_id, __transactions.transaction_id, e."offset_ix"
      FROM __events AS e
        INNER JOIN __creates ON __creates.event_id = e.event_id
        LEFT OUTER JOIN __consuming ON __consuming.contract_id = __creates.contract_id
        INNER JOIN __transactions ON __transactions."offset" = e."offset"
      WHERE __consuming.contract_id IS NULL
  `)

  db.Exec(`
    CREATE OR REPLACE FUNCTION active(template_id text default null, offset_end text default null)
      RETURNS TABLE(contract_id text, contract_key jsonb, payload jsonb, template_fqn text, witnesses text[], observers text[], signatories text[], event_id text, transaction_id text, "offset_ix" bigint)
      BEGIN ATOMIC
          SELECT * FROM active as a
          WHERE CASE
              WHEN template_id IS NOT NULL THEN a.template_fqn LIKE FORMAT('%%%s%%', template_id)
              ELSE true
            END
          AND
            CASE
              WHEN offset_end IS NOT NULL THEN a.offset_ix BETWEEN 0 AND (
                  SELECT __offsets.offset_ix
                  FROM __offsets
                  WHERE __offsets."offset" = offset_end
                  LIMIT 1)
              ELSE true
            END;
      END;
  `)
  db.Exec(`
    CREATE OR REPLACE FUNCTION lookup_contract(contract text)
      RETURNS table(contract_id text, contract_key jsonb, payload jsonb, template_fqn text, witnesses text[], observers text[], signatories text[], event_id text, transaction_id text, offset_ix bigint)
      BEGIN ATOMIC
      SELECT __creates.*, __transactions.transaction_id, offset_ix
        FROM __creates
          INNER JOIN __transactions ON __transactions.event_ids @> ARRAY[__creates.event_id]
          INNER JOIN __offsets ON __offsets."offset" = __transactions."offset"
        WHERE contract_id = contract;
      END;
  `)
  db.Exec(`
    CREATE OR REPLACE FUNCTION validate_offset_exists(n text)
      RETURNS text
      LANGUAGE plpgsql VOLATILE
      AS $$
        DECLARE offset_new TEXT;
        BEGIN
          select __transactions."offset" FROM __transactions WHERE __transactions."offset" = n LIMIT 1 INTO offset_new;
          IF offset_new IS NULL THEN
            RAISE EXCEPTION 'Illegal offset %', n;
          ELSE
            RETURN offset_new;
          END IF;
        END;
      $$
  `)
  db.Exec(`
    CREATE OR REPLACE FUNCTION latest_offset()
      RETURNS text
      BEGIN ATOMIC
        select __offsets."offset" from __offsets ORDER BY offset_ix DESC LIMIT 1;
      END;
  `)
  db.Exec(`
    CREATE OR REPLACE FUNCTION nearest_offset(n timestamptz)
      RETURNS text
      LANGUAGE plpgsql
      AS $func$
        DECLARE
          t text;
        BEGIN
          SELECT __offsets."offset" INTO t FROM __offsets WHERE effective_at <= n ORDER BY effective_at DESC LIMIT 1;

          IF COUNT(t) = 0 THEN
            RETURN NULL;
          ELSE
            RETURN t;
          END IF;
        END;
      $func$
  `)
  db.Exec(`
    CREATE INDEX IF NOT EXISTS consumed_contract_id ON __consuming USING BTREE(contract_id)
  `)
  //db.Exec(`
  //  CREATE INDEX IF NOT EXISTS event_ids_jsonb ON __transactions USING gin(event_ids);
  //`)
  //db.Exec(`
  //  CREATE INDEX IF NOT EXISTS payload_creates ON __creates USING gin(payload);
  //`)
  //db.Exec("CREATE OR REPLACE FUNCTION lookup_contract(n text) RETURNS SETOF public.__contracts BEGIN ATOMIC SELECT * FROM __contracts WHERE contract_id = n; END;")
  //db.Exec("CREATE OR REPLACE FUNCTION offset_exists(n text) RETURNS SETOF public.__contracts BEGIN ATOMIC SELECT * FROM __contracts WHERE offset = n; END;")
  //db.Exec("CREATE OR REPLACE FUNCTION partial_template(n text) RETURNS SETOF public.__contracts BEGIN ATOMIC SELECT * FROM active() WHERE template_fqn LIKE n; END;")
  return db
}
