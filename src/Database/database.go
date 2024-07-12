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

type ContractTable struct {
    ContractID string `gorm:"index:contracts_idx_contract_id,unique"`
    ContractKey []byte `gorm:"type:jsonb"`
    CreateArguments []byte `gorm:"type:jsonb"`
    TemplateFqn string `gorm:"index:contracts_idx_template_fqn"`
    Witnesses []byte
    Observers []byte
    Signatories []byte
    Offset string
}

func (ContractTable) TableName() string {
  return "__contracts"
}

type ArchivesTable struct {
    ContractID string `gorm:"index:archives_idx_contract_id,unique"`
    Offset string
}

func (ArchivesTable) TableName() string {
  return "__archives"
}

type CreatesTable struct {
   ContractID string `gorm:"index:creates_idx_contract_id,unique"`
}

func (CreatesTable) TableName() string {
  return "__creates"
}

func MigrateTables(db *gorm.DB) () {
  db.AutoMigrate(&ContractTable{})
  db.AutoMigrate(&ArchivesTable{})
  db.AutoMigrate(&CreatesTable{})

  hasOldOffset := db.Migrator().HasTable("last_offsets")
  if hasOldOffset {
    db.Migrator().RenameTable("last_offsets", "__offset")
  } else {
    db.AutoMigrate(&LastOffset{})
  }
}

func InitializeSQLiteDB() (db *gorm.DB) {
  db, err := gorm.Open(sqlite.Open("test.db"), &gorm.Config{})
  if err != nil {
    panic("Failed to open DB")
  }
  MigrateTables(db)
  return db
}

func InitializePostgresDB(host string, user string, password string, dbname string, port int, sslmode string) (db *gorm.DB) {
  connStr := fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%d sslmode=%s", host, user, password, dbname, port, sslmode)
  log.Printf("Connecting to PSQL DB %s", dbname)
  db, err := gorm.Open(postgres.Open(connStr), &gorm.Config{})
  if err != nil {
    panic(fmt.Sprintf("Failed to connect to db, error: %s", err))
  }
  MigrateTables(db)
  db.Exec("CREATE OR REPLACE FUNCTION active() RETURNS SETOF public.__contracts BEGIN ATOMIC SELECT * FROM __contracts WHERE contract_id NOT IN (SELECT contract_id FROM __archives); END;")
  db.Exec("CREATE OR REPLACE FUNCTION lookup_contract(n text) RETURNS SETOF public.__contracts BEGIN ATOMIC SELECT * FROM __contracts WHERE contract_id = n; END;")
  return db
}
